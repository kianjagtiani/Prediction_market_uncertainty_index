"""Probe Polymarket Gamma + CLOB APIs: shape, history depth, rate limits."""
import json
from pathlib import Path

import httpx

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
FIXTURES.mkdir(parents=True, exist_ok=True)

client = httpx.Client(timeout=30)

# 1. Gamma markets: one page of closed markets that ended after Jan 2024
r = client.get("https://gamma-api.polymarket.com/markets",
               params={"limit": 5, "closed": "true",
                       "end_date_min": "2024-01-01"})
r.raise_for_status()
markets = r.json()
FIXTURES.joinpath("pm_markets.json").write_text(json.dumps(markets, indent=2))
m = markets[0]
print("== Gamma market keys ==", sorted(m.keys()))
print("question:", m.get("question"))
print("category:", m.get("category"), "| volumeNum:", m.get("volumeNum"))
print("startDate:", m.get("startDate"), "| endDate:", m.get("endDate"))
# list every volume-ish field
vol_fields = {k: v for k, v in m.items() if "volume" in k.lower()}
print("volume-ish fields on market object:", vol_fields)
token_ids = json.loads(m.get("clobTokenIds") or "[]")
print("clobTokenIds:", token_ids)

# 1b. Try Gamma pagination via offset
r_off = client.get("https://gamma-api.polymarket.com/markets",
                    params={"limit": 5, "closed": "true",
                            "end_date_min": "2024-01-01", "offset": 5})
print("offset pagination status:", r_off.status_code,
      "| distinct ids from page2:", [x.get("id") for x in r_off.json()])

# 2. Need a market that actually ran during 2024 (started before/near Jan 2024,
# not just ended after). Search explicitly for one with an early start_date.
r2 = client.get("https://gamma-api.polymarket.com/markets",
                params={"limit": 20, "closed": "true",
                        "start_date_min": "2023-12-01",
                        "start_date_max": "2024-01-15",
                        "end_date_min": "2024-06-01"})
r2.raise_for_status()
candidates = r2.json()
print(f"== {len(candidates)} candidate long-lived 2024 markets ==")
long_lived = None
for c in candidates:
    ids = json.loads(c.get("clobTokenIds") or "[]")
    if ids:
        long_lived = c
        break
if long_lived is None and markets:
    long_lived = m
    ids = token_ids

print("chosen market for history probe:", long_lived.get("question"))
print("chosen startDate:", long_lived.get("startDate"), "endDate:", long_lived.get("endDate"))
ids = json.loads(long_lived.get("clobTokenIds") or "[]")

# 2. CLOB price history for the YES token of a long-lived 2024 market
r = client.get("https://clob.polymarket.com/prices-history",
               params={"market": ids[0], "interval": "max",
                       "fidelity": 1440})
print("prices-history status:", r.status_code)
r.raise_for_status()
hist = r.json()
FIXTURES.joinpath("pm_prices.json").write_text(json.dumps(hist, indent=2))
pts = hist.get("history", [])
print(f"== price history: {len(pts)} points ==")
if pts:
    from datetime import datetime, timezone
    print("first:", datetime.fromtimestamp(pts[0]["t"], tz=timezone.utc))
    print("last: ", datetime.fromtimestamp(pts[-1]["t"], tz=timezone.utc))
    print("sample point keys:", sorted(pts[0].keys()))

# 3. Rate-limit sniff: 10 rapid history calls
import time
t0 = time.time()
statuses = []
for _ in range(10):
    resp = client.get("https://clob.polymarket.com/prices-history",
                       params={"market": ids[0], "interval": "max",
                               "fidelity": 1440})
    statuses.append(resp.status_code)
print(f"10 rapid calls in {time.time() - t0:.1f}s | statuses: {statuses}")
