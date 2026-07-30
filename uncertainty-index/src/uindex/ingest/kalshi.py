"""Kalshi ingestion: public trade API v2 markets + daily candlesticks.

Runs in bounded portions: each invocation advances the crawl by a capped
amount, checkpoints to disk, and exits 3 if work remains (0 when the venue
is fully ingested). scripts/run_backfill.sh loops invocations, so memory
is released to the OS between portions.
"""
import time
from datetime import datetime, timezone

import httpx
import pandas as pd

from .. import config
from .store import MetaStore, PriceStore, crawl

BASE = "https://api.elections.kalshi.com/trade-api/v2"
OUT_DIR = config.DATA_DIR / "raw" / "kalshi"

MARKETS_COLUMNS = [
    "market_id", "venue", "question", "venue_category", "event_ticker",
    "ticker", "series_ticker", "total_volume_usd", "open_date", "close_date",
]


def markets_to_df(markets: list[dict]) -> pd.DataFrame:
    rows = []
    for m in markets:
        series = m.get("series_ticker") or m["event_ticker"].split("-")[0]
        rows.append({
            "market_id": f"ka_{m['ticker']}",
            "venue": "kalshi",
            "question": m.get("title") or "",
            # No category field on the market object (Task 2 checkpoint) -
            # categorization falls back to keyword rules only for Kalshi.
            "venue_category": "",
            "event_ticker": m["event_ticker"],
            "ticker": m["ticker"],
            "series_ticker": series,
            # volume_fp is a string count of contracts (Task 2 checkpoint,
            # not "volume"); ~$0.50 avg price is a fair notional proxy.
            "total_volume_usd": float(m.get("volume_fp") or 0) * 0.5,
            "open_date": m.get("open_time"),
            "close_date": m.get("close_time"),
        })
    if not rows:
        return pd.DataFrame(columns=MARKETS_COLUMNS)
    df = pd.DataFrame(rows)
    for col in ("open_date", "close_date"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(None)
    return df


def keep(df: pd.DataFrame) -> pd.DataFrame:
    # Same provable bound as history_todo (true notional <= 2x the $0.50
    # proxy), with slack below the rolling floor kept so robustness sweeps
    # can lower it without a re-crawl. This is what collapses the 10M+ row
    # raw catalog (every strike variant, overwhelmingly zero-volume) to a
    # size an 8 GB machine can hold.
    floor = config.KALSHI_MIN_ROLLING_NOTIONAL_USD / config.METADATA_VOLUME_SLACK
    return df[df["total_volume_usd"] * 2 >= floor]


def candles_to_df(payload: dict, market_id: str) -> pd.DataFrame:
    candles = payload.get("candlesticks", [])
    rows = []
    for c in candles:
        close_dollars = (c.get("price") or {}).get("close_dollars")
        if close_dollars is None:
            continue
        # close_dollars is already a 0-1 probability decimal string
        # (Task 2 checkpoint correction 3b) - do NOT divide by 100.
        close_prob = float(close_dollars)
        volume = float(c.get("volume_fp") or 0)
        rows.append({
            "market_id": market_id,
            "date": pd.to_datetime(c["end_period_ts"], unit="s", utc=True)
                      .normalize().tz_localize(None),
            "close_prob": close_prob,
            "daily_notional_usd": volume * close_prob,
        })
    df = pd.DataFrame(rows, columns=["market_id", "date", "close_prob",
                                     "daily_notional_usd"])
    return df.drop_duplicates(subset="date", keep="last")


def crawl_markets(client: httpx.Client, store: MetaStore,
                  max_pages: int | None = None) -> bool:
    min_close = int(datetime.fromisoformat(config.BACKFILL_START)
                    .replace(tzinfo=timezone.utc).timestamp())
    last_first = None

    def fetch_page(cursor):
        nonlocal last_first
        params = {"limit": config.KALSHI_PAGE_SIZE, "min_close_ts": min_close}
        if cursor:
            params["cursor"] = cursor
        r = client.get(f"{BASE}/markets", params=params)
        r.raise_for_status()
        j = r.json()
        batch = j.get("markets", [])
        if batch:
            # Same tripwire as Polymarket: a silently-ignored cursor param
            # replays page 1 forever; fail in seconds, not gigabytes.
            if batch[0].get("ticker") == last_first:
                raise RuntimeError(
                    f"pagination stuck: page repeated (first ticker "
                    f"{last_first}); cursor not advancing")
            last_first = batch[0].get("ticker")
        return (keep(markets_to_df(batch)) if batch else pd.DataFrame(),
                len(batch), j.get("cursor"))

    return crawl(store, fetch_page, MARKETS_COLUMNS,
                 config.KALSHI_SLEEP_S, max_pages)


def history_todo(meta: pd.DataFrame, done: set[str]) -> pd.DataFrame:
    # true lifetime notional <= volume_fp x $1.00 = total_volume_usd * 2, so
    # below the slack floor these markets can never pass any universe filter
    # a robustness sweep could configure - skipping their candle fetch is
    # lossless for any floor >= floor/slack.
    floor = config.KALSHI_MIN_ROLLING_NOTIONAL_USD / config.METADATA_VOLUME_SLACK
    return meta[~meta["market_id"].isin(done)
                & (meta["total_volume_usd"] * 2 >= floor)]


def fetch_candles(client: httpx.Client, series: str, ticker: str,
                  start_ts: int, end_ts: int) -> dict:
    """Fetch daily candlesticks for a market.

    `series_ticker` is usually absent from the market object and the
    derived prefix (event_ticker.split("-")[0]) is sometimes wrong for
    this endpoint: some 2024-era series need the bare prefix (HIGHNY),
    current series need a KX-prefixed one (KXHIGHNY, KXRHGOLD), and it's
    not predictable which ahead of time (Task 2 checkpoint correction 3).
    Try the derived prefix first, then retry once with a KX-prefixed
    variant if the first attempt 404s or returns zero candles. If both
    attempts come up empty (or the retry also fails to find the market),
    that's expected for the pre-2026 backfill window (zero historical
    Kalshi volume) - not an error condition, so an empty-candlesticks
    payload is returned rather than raising. Only genuinely unexpected
    errors (5xx, network failures) still propagate.
    """
    def _get(s: str) -> httpx.Response:
        return client.get(f"{BASE}/series/{s}/markets/{ticker}/candlesticks",
                          params={"start_ts": start_ts, "end_ts": end_ts,
                                  "period_interval": config.KALSHI_CANDLE_PERIOD_INTERVAL_MINUTES})

    r = _get(series)
    empty = r.status_code == 200 and not r.json().get("candlesticks")
    if r.status_code == 404 or empty:
        if series.startswith("KX"):
            # No further fallback prefix to try.
            if r.status_code == 404:
                return {"candlesticks": []}
        else:
            r2 = _get(f"KX{series}")
            if r2.status_code == 200:
                return r2.json()
            if r2.status_code == 404:
                # Both prefixes 404: no historical data available, not an error.
                return {"candlesticks": []}
            r2.raise_for_status()
    r.raise_for_status()
    return r.json()


def main(pages: int | None = None, markets: int | None = None) -> bool:
    """One portion of work. Returns True when the venue is fully ingested."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    meta_store = MetaStore(OUT_DIR)
    with httpx.Client(timeout=30) as client:
        if not meta_store.complete:
            crawl_markets(client, meta_store, pages)
            return False  # candles start in a fresh process either way

        meta = pd.read_parquet(meta_store.final_path)
        store = PriceStore(OUT_DIR)
        todo = history_todo(meta, store.done_ids())
        print(f"{len(meta)} markets; {len(todo)} left after notional pre-filter")
        portion = todo if markets is None else todo.head(markets)
        for i, row in enumerate(portion.itertuples(index=False)):
            if pd.isna(row.open_date) or pd.isna(row.close_date):
                store.mark_no_data(row.market_id)
                continue
            start = int(row.open_date.replace(tzinfo=timezone.utc).timestamp())
            end = int(row.close_date.replace(tzinfo=timezone.utc).timestamp())
            try:
                payload = fetch_candles(client, row.series_ticker, row.ticker,
                                        start, end)
                df = candles_to_df(payload, row.market_id)
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
            time.sleep(config.KALSHI_SLEEP_S)
            if i % 200 == 199:
                store.checkpoint()
                print(f"checkpoint: {i + 1}/{len(portion)} this portion")
        store.checkpoint()
        if markets is not None and len(todo) > markets:
            return False
        store.finalize()
        print("kalshi ingestion complete")
        return True


if __name__ == "__main__":
    import argparse
    import sys
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=500,
                    help="metadata pages per portion (0 = unbounded)")
    ap.add_argument("--markets", type=int, default=2000,
                    help="candle fetches per portion (0 = unbounded)")
    a = ap.parse_args()
    sys.exit(0 if main(a.pages or None, a.markets or None) else 3)
