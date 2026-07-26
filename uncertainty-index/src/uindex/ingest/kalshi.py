"""Kalshi ingestion: public trade API v2 markets + daily candlesticks."""
import time
from datetime import datetime, timezone

import httpx
import pandas as pd

from .. import config
from .store import PriceStore

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
    df = pd.DataFrame(rows)
    for col in ("open_date", "close_date"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(None)
    return df


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


def fetch_all_markets(client: httpx.Client) -> pd.DataFrame:
    # Each page is converted to a slim DataFrame immediately: accumulating
    # raw dicts for the full catalog (every strike variant since 2024) hit
    # ~9 GB RSS and never finished on an 8 GB machine.
    min_close = int(datetime.fromisoformat(config.BACKFILL_START)
                    .replace(tzinfo=timezone.utc).timestamp())
    frames, cursor, n = [], None, 0
    while True:
        params = {"limit": config.KALSHI_PAGE_SIZE, "min_close_ts": min_close}
        if cursor:
            params["cursor"] = cursor
        r = client.get(f"{BASE}/markets", params=params)
        r.raise_for_status()
        j = r.json()
        batch = j.get("markets", [])
        if batch:
            frames.append(markets_to_df(batch))
            n += len(batch)
            if len(frames) % 50 == 0:
                print(f"  metadata: {n} markets fetched", flush=True)
        cursor = j.get("cursor")
        if not cursor:
            break
        time.sleep(config.KALSHI_SLEEP_S)
    if not frames:
        return pd.DataFrame(columns=MARKETS_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def history_todo(meta: pd.DataFrame, done: set[str]) -> pd.DataFrame:
    # total_volume_usd is volume_fp x $0.50; true lifetime notional is at most
    # volume_fp x $1.00 = total_volume_usd * 2, so below the rolling floor
    # these markets can provably never pass the universe filter - skipping
    # their candle fetch is lossless. The bound holds relative to the metadata
    # snapshot: a still-open market would need ~ROLLING_WINDOW_DAYS x floor of
    # post-snapshot notional to sneak past it, which the deliberate slack in
    # the bound (vs the provable window-sum limit) absorbs.
    return meta[
        ~meta["market_id"].isin(done)
        & (meta["total_volume_usd"] * 2 >= config.KALSHI_MIN_ROLLING_NOTIONAL_USD)
    ]


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


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(timeout=30)

    markets_path = OUT_DIR / "markets.parquet"
    if markets_path.exists():
        meta = pd.read_parquet(markets_path)
    else:
        meta = fetch_all_markets(client)
        meta.to_parquet(markets_path, index=False)
    print(f"{len(meta)} kalshi markets")

    store = PriceStore(OUT_DIR)
    todo = history_todo(meta, store.done_ids())
    print(f"{len(todo)} markets to fetch after notional pre-filter")
    for i, row in enumerate(todo.itertuples(index=False)):
        start = int(row.open_date.replace(tzinfo=timezone.utc).timestamp())
        end = int(row.close_date.replace(tzinfo=timezone.utc).timestamp())
        try:
            payload = fetch_candles(client, row.series_ticker, row.ticker, start, end)
            store.append(candles_to_df(payload, row.market_id))
        except httpx.HTTPStatusError as e:
            print(f"skip {row.market_id}: {e.response.status_code}")
        time.sleep(config.KALSHI_SLEEP_S)
        if i % 200 == 199:
            store.checkpoint()
            print(f"checkpoint: {i + 1}/{len(todo)}")
    store.finalize()
    print("kalshi ingestion complete")


if __name__ == "__main__":
    main()
