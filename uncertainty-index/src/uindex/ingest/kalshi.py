"""Kalshi ingestion: public trade API v2 markets + daily candlesticks."""
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

from .. import config

BASE = "https://api.elections.kalshi.com/trade-api/v2"
OUT_DIR = config.DATA_DIR / "raw" / "kalshi"
SLEEP_S = 0.15  # adjust per Task 2 rate-limit findings


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


def fetch_all_markets(client: httpx.Client) -> list[dict]:
    min_close = int(datetime.fromisoformat(config.BACKFILL_START)
                    .replace(tzinfo=timezone.utc).timestamp())
    out, cursor = [], None
    while True:
        params = {"limit": 1000, "min_close_ts": min_close}
        if cursor:
            params["cursor"] = cursor
        r = client.get(f"{BASE}/markets", params=params)
        r.raise_for_status()
        j = r.json()
        out.extend(j.get("markets", []))
        cursor = j.get("cursor")
        if not cursor:
            return out
        time.sleep(SLEEP_S)


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
    attempts come up empty, that's expected for the pre-2026 backfill
    window (zero historical Kalshi volume) - not an error condition.
    """
    def _get(s: str) -> httpx.Response:
        return client.get(f"{BASE}/series/{s}/markets/{ticker}/candlesticks",
                          params={"start_ts": start_ts, "end_ts": end_ts,
                                  "period_interval": 1440})

    r = _get(series)
    empty = r.status_code == 200 and not r.json().get("candlesticks")
    if r.status_code == 404 or empty:
        if not series.startswith("KX"):
            r2 = _get(f"KX{series}")
            if r2.status_code == 200:
                r = r2
    r.raise_for_status()
    return r.json()


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(timeout=30)

    markets_path = OUT_DIR / "markets.parquet"
    if markets_path.exists():
        meta = pd.read_parquet(markets_path)
    else:
        meta = markets_to_df(fetch_all_markets(client))
        meta.to_parquet(markets_path, index=False)
    print(f"{len(meta)} kalshi markets")

    prices_path = OUT_DIR / "prices.parquet"
    done: set[str] = set()
    frames: list[pd.DataFrame] = []
    if prices_path.exists():
        existing = pd.read_parquet(prices_path)
        done = set(existing["market_id"].unique())
        frames = [existing]

    todo = meta[~meta["market_id"].isin(done)]
    for i, row in enumerate(todo.itertuples(index=False)):
        start = int(row.open_date.replace(tzinfo=timezone.utc).timestamp())
        end = int(row.close_date.replace(tzinfo=timezone.utc).timestamp())
        try:
            payload = fetch_candles(client, row.series_ticker, row.ticker, start, end)
            frames.append(candles_to_df(payload, row.market_id))
        except httpx.HTTPStatusError as e:
            print(f"skip {row.market_id}: {e.response.status_code}")
        time.sleep(SLEEP_S)
        if i % 200 == 199:
            pd.concat(frames, ignore_index=True).to_parquet(prices_path, index=False)
            print(f"checkpoint: {i + 1}/{len(todo)}")
    pd.concat(frames, ignore_index=True).to_parquet(prices_path, index=False)
    print("kalshi ingestion complete")


if __name__ == "__main__":
    main()
