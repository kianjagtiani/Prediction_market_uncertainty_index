"""Polymarket ingestion: Gamma (metadata) + CLOB (price history).

Runs in bounded portions: each invocation advances the crawl by a capped
amount, checkpoints to disk, and exits 3 if work remains (0 when the venue
is fully ingested). scripts/run_backfill.sh loops invocations, so memory
is released to the OS between portions.
"""
import json
import time

import httpx
import pandas as pd

from .. import config
from .store import MetaStore, PriceStore, crawl

GAMMA_KEYSET_URL = "https://gamma-api.polymarket.com/markets/keyset"
HISTORY_URL = "https://clob.polymarket.com/prices-history"
OUT_DIR = config.DATA_DIR / "raw" / "polymarket"

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
    endpoint paginates arbitrarily deep but silently caps pages at 100."""
    def fetch_page(cursor):
        params = {"limit": config.POLYMARKET_PAGE_SIZE,
                  "end_date_min": config.BACKFILL_START}
        if cursor:
            params["cursor"] = cursor
        r = client.get(GAMMA_KEYSET_URL, params=params)
        r.raise_for_status()
        j = r.json()
        batch = j.get("markets", [])
        return (keep(markets_to_df(batch)) if batch else pd.DataFrame(),
                len(batch), j.get("next_cursor"))

    return crawl(store, fetch_page, MARKETS_COLUMNS,
                 config.POLYMARKET_SLEEP_S, max_pages)


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


def main(pages: int | None = None, markets: int | None = None) -> bool:
    """One portion of work. Returns True when the venue is fully ingested."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    meta_store = MetaStore(OUT_DIR)
    with httpx.Client(timeout=30) as client:
        if not meta_store.complete:
            crawl_markets(client, meta_store, pages)
            return False  # prices start in a fresh process either way

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
    a = ap.parse_args()
    sys.exit(0 if main(a.pages or None, a.markets or None) else 3)
