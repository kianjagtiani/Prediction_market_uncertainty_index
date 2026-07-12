"""Probe Kalshi public trade API: market shape, candlestick history."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
FIXTURES.mkdir(parents=True, exist_ok=True)
BASE = "https://api.elections.kalshi.com/trade-api/v2"

client = httpx.Client(timeout=30)

# 1. One page of settled markets that closed in the Jan-Jun 2024 window.
# NOTE: min_close_ts alone returns markets closed *at or after* that ts,
# which today means mostly current/future markets. We also need
# max_close_ts to actually land on markets that ran and closed in 2024.
min_close = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
max_close = int(datetime(2024, 6, 30, tzinfo=timezone.utc).timestamp())
r = client.get(f"{BASE}/markets",
               params={"limit": 20, "status": "settled",
                       "min_close_ts": min_close, "max_close_ts": max_close})
print("markets status:", r.status_code)
r.raise_for_status()
j = r.json()
FIXTURES.joinpath("kalshi_markets.json").write_text(json.dumps(j, indent=2))
m = j["markets"][0]
print("== market keys ==", sorted(m.keys()))
for k in ("ticker", "event_ticker", "series_ticker", "title", "category", "volume",
          "open_time", "close_time", "result"):
    print(f"{k}: {m.get(k)}")
print("cursor present:", bool(j.get("cursor")))
print("has series_ticker field directly:", "series_ticker" in m)

# 1b. Pagination: follow the cursor for a second page
if j.get("cursor"):
    r_p2 = client.get(f"{BASE}/markets",
                       params={"limit": 20, "status": "settled",
                               "min_close_ts": min_close, "max_close_ts": max_close,
                               "cursor": j["cursor"]})
    j2 = r_p2.json()
    print("page2 status:", r_p2.status_code, "| page2 first ticker:",
          j2["markets"][0]["ticker"] if j2.get("markets") else None,
          "| same as page1 first?", j2["markets"][0]["ticker"] == m["ticker"] if j2.get("markets") else None)

# 2. Candlesticks for that market (series ticker = event ticker prefix;
#    confirm the exact series field/derivation here)
series = m.get("series_ticker") or m["event_ticker"].split("-")[0]
start = int(datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp())
end = int(datetime.fromisoformat(m["close_time"].replace("Z", "+00:00")).timestamp())
r = client.get(f"{BASE}/series/{series}/markets/{m['ticker']}/candlesticks",
               params={"start_ts": start, "end_ts": end,
                       "period_interval": 1440})
print("candlestick status:", r.status_code)
if r.status_code != 200:
    print("candlestick error body:", r.text[:500])
r.raise_for_status()
candles = r.json()
FIXTURES.joinpath("kalshi_candles.json").write_text(json.dumps(candles, indent=2))
cs = candles.get("candlesticks", [])
print(f"== {len(cs)} daily candles ==")
if cs:
    print("candle keys:", sorted(cs[0].keys()))
    print("sample:", json.dumps(cs[0], indent=2)[:800])

# 3. Rate-limit sniff
t0 = time.time()
statuses = []
for _ in range(10):
    resp = client.get(f"{BASE}/markets", params={"limit": 1})
    statuses.append(resp.status_code)
print(f"10 rapid calls in {time.time() - t0:.1f}s | statuses: {statuses}")
