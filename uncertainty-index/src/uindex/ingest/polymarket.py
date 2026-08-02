"""Polymarket ingestion: Gamma (metadata) + CLOB (price history).

Runs in bounded portions: each invocation advances the crawl by a capped
amount, checkpoints to disk, and exits 3 if work remains (0 when the venue
is fully ingested). scripts/run_backfill.sh loops invocations, so memory
is released to the OS between portions.

Legacy metadata sweep (ids 1..LEGACY_MAX_ID): Gamma's `/markets/keyset`
endpoint only serves id >= LEGACY_MAX_ID + 1 -- verified live (2026-08-02),
`/markets/keyset` with no cursor returns id 559651 first. Below that, ids
are enumerated via the plain `/markets?id=X&id=Y&...` endpoint (repeated
`id` params). This form works and Gamma caps it hard at 100 ids/request
(confirmed live: 200 ids returns a "expected array length <= 100"
validation error); nonexistent ids are silently absent from the response,
not an error, so a dense sweep of every id in [1, LEGACY_MAX_ID] in batches
of LEGACY_BATCH is safe and simple -- no need for the offset-pagination
fallback the brief allows for.

One wrinkle the probe surfaced: unlike `/markets/keyset`, plain `/markets`
defaults to `closed=false` and silently drops everything else -- id 253591
("Will Donald Trump win the 2024 US Presidential Election?", resolved)
comes back `[]` with no `closed` param and with `closed=false`, but returns
the market with `closed=true`. Since the legacy range is almost entirely
resolved markets, every batch is fetched twice (once per closed state) and
the results unioned in fetch_legacy_batch; sending both closed values in one
request breaks it (0 results either way), so it can't be done in one call.
"""
import json
import os
import time
from pathlib import Path

import httpx
import pandas as pd

from .. import config
from .store import MetaStore, PriceStore, crawl, _stream_merge

GAMMA_KEYSET_URL = "https://gamma-api.polymarket.com/markets/keyset"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
HISTORY_URL = "https://clob.polymarket.com/prices-history"
OUT_DIR = config.DATA_DIR / "raw" / "polymarket"

LEGACY_MAX_ID = 559650  # last id below the keyset endpoint's floor
LEGACY_BATCH = 100      # Gamma's hard cap on repeated `id=` params/request

MARKETS_COLUMNS = [
    "market_id", "venue", "question", "venue_category",
    "yes_token_id", "total_volume_usd", "open_date", "close_date",
]


def markets_to_df(markets: list[dict]) -> pd.DataFrame:
    rows = []
    for m in markets:
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        if not token_ids:
            continue
        rows.append({
            "market_id": f"pm_{m['id']}",
            "venue": "polymarket",
            "question": m.get("question") or "",
            "venue_category": (m.get("category") or "").strip(),
            "yes_token_id": str(token_ids[0]),
            "total_volume_usd": float(m.get("volumeNum") or 0.0),
            "open_date": m.get("startDate"),
            "close_date": m.get("endDate"),
        })
    if not rows:
        return pd.DataFrame(columns=MARKETS_COLUMNS)
    df = pd.DataFrame(rows)
    for col in ("open_date", "close_date"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(None)
    return df


def keep(df: pd.DataFrame) -> pd.DataFrame:
    # The universe filter drops markets below the volume floor, so metadata
    # far under it can never enter any index and storing it is pure waste
    # (the raw catalog is 1M+ rows, mostly dead markets). Slack below the
    # floor is kept so robustness sweeps can lower it without a re-crawl.
    floor = config.POLYMARKET_MIN_TOTAL_VOLUME_USD / config.METADATA_VOLUME_SLACK
    return df[df["total_volume_usd"] >= floor]


def history_to_df(payload: dict, market_id: str) -> pd.DataFrame:
    pts = payload.get("history", [])
    if not pts:
        return pd.DataFrame(columns=["market_id", "date", "close_prob"])
    df = pd.DataFrame(pts)
    df["date"] = (
        pd.to_datetime(df["t"], unit="s", utc=True).dt.normalize().dt.tz_localize(None)
    )
    df = df.groupby("date", as_index=False)["p"].last()
    df = df.rename(columns={"p": "close_prob"})
    # An all-integral history (resolved market's JSON 0/1s) must not produce
    # an int64 shard that wedges the schema-locked stream merge.
    df["close_prob"] = df["close_prob"].astype(float)
    df.insert(0, "market_id", market_id)
    return df[["market_id", "date", "close_prob"]]


def crawl_markets(client: httpx.Client, store: MetaStore,
                  max_pages: int | None = None) -> bool:
    """Plain offset pagination 422s past a few thousand results; the keyset
    endpoint paginates arbitrarily deep but silently caps pages at 100.

    The pagination param is `after_cursor` (per the OpenAPI spec). Gamma
    silently ignores unknown params, so a wrong name replays page 1 forever
    with a fresh-looking next_cursor - which is exactly how the first two
    backfill attempts crawled 1M+ "markets" that were 100 unique ones.
    """
    last_first = None

    def fetch_page(cursor):
        nonlocal last_first
        params = {"limit": config.POLYMARKET_PAGE_SIZE,
                  "end_date_min": config.BACKFILL_START}
        if cursor:
            params["after_cursor"] = cursor
        r = client.get(GAMMA_KEYSET_URL, params=params)
        r.raise_for_status()
        j = r.json()
        batch = j.get("markets", [])
        if batch:
            if batch[0].get("id") == last_first:
                raise RuntimeError(
                    f"pagination stuck: page repeated (first id "
                    f"{last_first}); cursor not advancing")
            last_first = batch[0].get("id")
        return (keep(markets_to_df(batch)) if batch else pd.DataFrame(),
                len(batch), j.get("next_cursor"))

    return crawl(store, fetch_page, MARKETS_COLUMNS,
                 config.POLYMARKET_SLEEP_S, max_pages)


def fetch_legacy_batch(client: httpx.Client, ids: list[int]) -> list[dict]:
    """One id-range batch, both closed states unioned (see module docstring
    for why /markets needs closed=true fetched separately from closed=false)."""
    seen: set[str] = set()
    out: list[dict] = []
    for closed in ("true", "false"):
        r = client.get(GAMMA_MARKETS_URL, params={
            "id": ids, "closed": closed, "end_date_min": config.BACKFILL_START,
        })
        r.raise_for_status()
        for m in r.json():
            mid = m.get("id")
            if mid not in seen:
                seen.add(mid)
                out.append(m)
        time.sleep(config.POLYMARKET_SLEEP_S)
    return out


def crawl_legacy_markets(client: httpx.Client, store: MetaStore,
                         max_batches: int | None = None) -> bool:
    """Sweeps ids 1..LEGACY_MAX_ID in fixed batches of LEGACY_BATCH.

    Deliberately doesn't reuse the shared crawl() engine: crawl()'s
    stopping rule ("done" once a page comes back empty) is right for
    keyset, where an empty page means the catalog is exhausted, but wrong
    here -- most LEGACY_BATCH-sized windows in a sparse stretch of the
    legacy id space come back with zero markets and the sweep must keep
    going regardless. Termination is instead driven purely by the id
    watermark reaching LEGACY_MAX_ID, so an all-empty batch just advances
    the cursor like any other.
    """
    cursor, n = store.resume()
    start = cursor if cursor is not None else 1
    frames: list[pd.DataFrame] = []
    batches = 0

    def flush(frames):
        return (pd.concat(frames, ignore_index=True) if frames
                else pd.DataFrame(columns=MARKETS_COLUMNS))

    while max_batches is None or batches < max_batches:
        end = min(start + LEGACY_BATCH - 1, LEGACY_MAX_ID)
        ids = list(range(start, end + 1))
        raw = fetch_legacy_batch(client, ids)
        df = keep(markets_to_df(raw))
        if not df.empty:
            frames.append(df)
        n += len(raw)
        start = end + 1
        batches += 1
        done = start > LEGACY_MAX_ID
        cursor = None if done else start
        if done or batches % config.INGEST_FLUSH_PAGES == 0:
            store.commit(flush(frames), cursor, n)
            frames = []
            print(f"  legacy metadata: {n} markets seen, watermark {cursor}",
                 flush=True)
        if done:
            store.finalize(MARKETS_COLUMNS)
            return True
        time.sleep(config.POLYMARKET_SLEEP_S)
    store.commit(flush(frames), start, n)
    return False


def merge_legacy_markets(legacy_store: MetaStore, main_final: Path,
                         flag_path: Path) -> None:
    """One-time fold of the finished legacy sweep into markets.parquet.

    Both inputs are already fully materialized (each side's own
    MetaStore.finalize() already ran), so this is a single bounded merge
    rather than a shard-at-a-time stream -- reading two catalogs of well
    under a million rows each is nowhere near the 8GB budget. Idempotent:
    legacy's own final file is never deleted, so a crash between the
    _stream_merge and the flag write just re-merges next time (dedup on
    market_id, main catalog listed first as authoritative, makes that a
    no-op); the flag exists purely so a completed merge isn't redone (and
    markets.parquet rewritten) on every later price-phase portion.
    """
    sources = ([main_final] if main_final.exists() else []) + [legacy_store.final_path]
    _stream_merge(sources, main_final, ["market_id"])
    tmp = flag_path.with_name(flag_path.name + ".tmp")
    tmp.write_text("")
    os.replace(tmp, flag_path)


def history_todo(meta: pd.DataFrame, done: set[str]) -> pd.DataFrame:
    # Fetch down to the same slack floor the metadata keeps: prices for
    # anything a robustness sweep or a reweighting could ever admit, so no
    # methodology change downstream forces a re-crawl. Still lossless for
    # any floor >= floor/slack.
    floor = config.POLYMARKET_MIN_TOTAL_VOLUME_USD / config.METADATA_VOLUME_SLACK
    return meta[~meta["market_id"].isin(done)
                & (meta["total_volume_usd"] >= floor)]


def fetch_history(client: httpx.Client, token_id: str) -> dict:
    r = client.get(HISTORY_URL, params={
        "market": token_id, "interval": "max",
        "fidelity": config.POLYMARKET_HISTORY_FIDELITY,
    })
    r.raise_for_status()
    return r.json()


def main(pages: int | None = None, markets: int | None = None,
        legacy_batches: int | None = None) -> bool:
    """One portion of work. Returns True when the venue is fully ingested."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    meta_store = MetaStore(OUT_DIR)
    legacy_store = MetaStore(OUT_DIR / "legacy")
    (OUT_DIR / "legacy").mkdir(parents=True, exist_ok=True)
    legacy_flag = OUT_DIR / "markets_legacy_merged.flag"
    with httpx.Client(timeout=30) as client:
        if not meta_store.complete:
            crawl_markets(client, meta_store, pages)
            return False  # next phase starts in a fresh process either way

        if not legacy_store.complete:
            crawl_legacy_markets(client, legacy_store, legacy_batches)
            return False

        if not legacy_flag.exists():
            merge_legacy_markets(legacy_store, meta_store.final_path, legacy_flag)
            return False

        meta = pd.read_parquet(meta_store.final_path)
        store = PriceStore(OUT_DIR)
        todo = history_todo(meta, store.done_ids())
        print(f"{len(meta)} markets; {len(todo)} left after volume pre-filter")
        portion = todo if markets is None else todo.head(markets)
        for i, row in enumerate(portion.itertuples(index=False)):
            try:
                df = history_to_df(fetch_history(client, row.yes_token_id),
                                   row.market_id)
                if df.empty:
                    store.mark_no_data(row.market_id)
                else:
                    store.append(df)
            except httpx.HTTPStatusError as e:
                # Only a definitive 4xx is permanent. 429/5xx are transient:
                # crash the portion so the driver retries, instead of
                # silently ledgering the market as dataless forever.
                if e.response.status_code == 429 or e.response.status_code >= 500:
                    raise
                print(f"skip {row.market_id}: {e.response.status_code}")
                store.mark_no_data(row.market_id)
            time.sleep(config.POLYMARKET_SLEEP_S)
            if i % 200 == 199:
                store.checkpoint()
                print(f"checkpoint: {i + 1}/{len(portion)} this portion")
        store.checkpoint()
        if markets is not None and len(todo) > markets:
            return False
        store.finalize()
        print("polymarket ingestion complete")
        return True


if __name__ == "__main__":
    import argparse
    import sys
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=500,
                    help="metadata pages per portion (0 = unbounded)")
    ap.add_argument("--markets", type=int, default=2000,
                    help="price fetches per portion (0 = unbounded)")
    ap.add_argument("--legacy-batches", type=int, default=2000,
                    help="legacy id-sweep batches (100 ids each) per "
                         "portion (0 = unbounded)")
    a = ap.parse_args()
    sys.exit(0 if main(a.pages or None, a.markets or None,
                       a.legacy_batches or None) else 3)
