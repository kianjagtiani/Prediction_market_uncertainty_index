# Uncertainty Index Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A validated family of prediction-market uncertainty indices (Turbulence + Unresolvedness, composite + 7 themed) backfilled Jan 2024 → present from Polymarket and Kalshi, with a validation report proving the numbers are meaningful.

**Architecture:** Five-stage batch pipeline — `ingest → normalize → universes → compute → validate` — where each stage reads/writes parquet under `data/` and is re-runnable in isolation. The index engine is universe-agnostic: an index = a universe definition + one shared computation.

**Tech Stack:** Python 3.11+, httpx, pandas, numpy, pyarrow, duckdb, matplotlib, pytest. Plain venv.

**Working directory:** everything in this plan is relative to `uncertainty-index/` inside the repo `/Users/kianjagtiani/Documents/pred_market_baskets`.

## Global Constraints

- Spec: `uncertainty-index/docs/specs/2026-07-12-uncertainty-index-design.md` — it wins on any conflict.
- Sports markets are **excluded entirely** (dropped at normalization; no sports index).
- Backfill window starts `2024-01-01`. Percentile scale seeds on the first 90 days (excluded from published series).
- All tunable parameters live in `src/uindex/config.py` — never hardcoded in logic modules.
- Tests never touch the network; API tests use JSON fixtures recorded by the Task 2 probes.
- `data/` is gitignored — parquet artifacts are never committed. Docs, code, fixtures are.
- Point-in-time discipline: a market may only influence day *t* using information available on day *t*. Known accepted deviations (static Polymarket volume weights, lifetime-volume strike representatives) are documented in the methodology doc, nowhere silently.
- When producing validation charts (Tasks 10–11), read the `dataviz` skill before writing chart code.
- Commit after every task (and at marked steps within tasks).

---

### Task 1: Project scaffold

**Files:**
- Create: `uncertainty-index/pyproject.toml`
- Create: `uncertainty-index/.gitignore`
- Create: `uncertainty-index/src/uindex/__init__.py`
- Create: `uncertainty-index/src/uindex/config.py`
- Test: `uncertainty-index/tests/test_config.py`

**Interfaces:**
- Produces: importable package `uindex`; `uindex.config` with all constants listed below (later tasks import these exact names).

- [ ] **Step 1: Write files**

`pyproject.toml`:

```toml
[project]
name = "uindex"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27",
    "pandas>=2.2",
    "numpy>=1.26",
    "pyarrow>=16",
    "duckdb>=1.0",
    "matplotlib>=3.9",
    "xlrd>=2.0",
    "openpyxl>=3.1",
]

[project.optional-dependencies]
dev = ["pytest>=8"]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`.gitignore`:

```
data/
.venv/
__pycache__/
*.egg-info/
.pytest_cache/
```

`src/uindex/__init__.py`: empty file.

`src/uindex/config.py`:

```python
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

BACKFILL_START = "2024-01-01"

# Index math
CLIP_LO, CLIP_HI = 0.01, 0.99
EWMA_HALFLIFE_DAYS = 10
EWMA_MIN_PERIODS = 5
SEED_DAYS = 90

# Point-in-time universe rules
RESOLUTION_EXCLUSION_DAYS = 3
POLYMARKET_MIN_TOTAL_VOLUME_USD = 50_000
KALSHI_MIN_ROLLING_NOTIONAL_USD = 5_000
ROLLING_WINDOW_DAYS = 7

INDEXES = ["GLOBAL", "WAR", "ELECTIONS", "POLITICS", "ECON_FED",
           "CRYPTO", "TECH_AI", "CLIMATE"]

# Categorization. Order matters: first match wins.
SPORTS_KEYWORDS = [
    "nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball",
    "baseball", "hockey", "tennis", "golf", "ufc", "boxing", "olympic",
    "world cup", "super bowl", "march madness", "f1 ", "grand prix",
    "premier league", "champions league", "playoff", "heisman",
    "world series", "stanley cup", "wimbledon", "masters tournament",
]
SPORTS_VENUE_CATEGORIES = {"sports", "sport"}

CATEGORY_RULES: dict[str, list[str]] = {
    "WAR": ["war", "ceasefire", "military", "strike on", "airstrike",
            "invasion", "invade", "missile", "nuclear", "sanction",
            "nato", "troops", "gaza", "ukraine", "russia", "iran",
            "taiwan", "hostage", "hezbollah", "houthis"],
    "ELECTIONS": ["election", "primary", "presidential race", "senate seat",
                  "senate race", "house seat", "governor", "midterm",
                  "nominee", "ballot", "electoral", "wins the presidency",
                  "popular vote"],
    "POLITICS": ["shutdown", "impeach", "supreme court", "scotus",
                 "congress", "bill", "executive order", "cabinet",
                 "resign", "speaker of the house", "confirm", "veto",
                 "pardon", "debt ceiling"],
    "ECON_FED": ["fed ", "fomc", "rate cut", "rate hike", "interest rate",
                 "cpi", "inflation", "recession", "gdp", "unemployment",
                 "jobs report", "payroll", "treasury", "tariff"],
    "CRYPTO": ["bitcoin", "btc", "ethereum", "eth ", "solana", "crypto",
               "stablecoin", "coinbase", "binance"],
    "TECH_AI": ["openai", "gpt", "claude", "anthropic", "gemini",
                "ai model", "artificial intelligence", "agi", "nvidia",
                "apple", "tesla", "spacex", "chatgpt", "deepmind"],
    "CLIMATE": ["temperature", "hottest", "hurricane", "wildfire",
                "climate", "emissions", "carbon", "heat record",
                "named storm", "el nino", "la nina"],
}

# Fallback: venue-provided category (lowercased) -> our taxonomy
VENUE_CATEGORY_MAP: dict[str, str] = {
    "politics": "POLITICS",
    "us-current-affairs": "POLITICS",
    "world": "WAR",
    "geopolitics": "WAR",
    "economics": "ECON_FED",
    "financials": "ECON_FED",
    "economy": "ECON_FED",
    "crypto": "CRYPTO",
    "science and technology": "TECH_AI",
    "tech": "TECH_AI",
    "climate and weather": "CLIMATE",
    "climate": "CLIMATE",
}

# Universe composition: which categories feed each index
INDEX_UNIVERSES: dict[str, list[str]] = {
    "GLOBAL": ["WAR", "ELECTIONS", "POLITICS", "ECON_FED",
               "CRYPTO", "TECH_AI", "CLIMATE"],
    "WAR": ["WAR"],
    "ELECTIONS": ["ELECTIONS"],
    "POLITICS": ["POLITICS"],
    "ECON_FED": ["ECON_FED"],
    "CRYPTO": ["CRYPTO"],
    "TECH_AI": ["TECH_AI"],
    "CLIMATE": ["CLIMATE"],
}
```

`tests/test_config.py`:

```python
from uindex import config


def test_every_index_has_a_universe():
    assert set(config.INDEXES) == set(config.INDEX_UNIVERSES)


def test_global_is_union_of_themed():
    themed = [i for i in config.INDEXES if i != "GLOBAL"]
    assert sorted(config.INDEX_UNIVERSES["GLOBAL"]) == sorted(themed)


def test_sports_never_in_taxonomy():
    assert "SPORTS" not in config.CATEGORY_RULES
    assert "SPORTS" not in config.INDEX_UNIVERSES
```

- [ ] **Step 2: Create venv, install, run tests**

```bash
cd uncertainty-index
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -q
```

Expected: `3 passed`.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml .gitignore src tests
git commit -m "feat: scaffold uindex package with config and taxonomy"
```

---

### Task 2: API feasibility probes (CHECKPOINT)

**Files:**
- Create: `uncertainty-index/scripts/probe_polymarket.py`
- Create: `uncertainty-index/scripts/probe_kalshi.py`
- Create: `uncertainty-index/docs/probe-findings.md` (written from probe output)
- Create: `uncertainty-index/tests/fixtures/` (recorded JSON responses)

**Interfaces:**
- Produces: fixtures `tests/fixtures/pm_markets.json`, `pm_prices.json`, `kalshi_markets.json`, `kalshi_candles.json` — consumed by Tasks 4–5 tests. Findings doc that may amend Tasks 4–5.

This task is exploratory, not TDD. Its purpose: confirm the API shapes Tasks 4–5 are written against, and measure history depth + rate limits **before** building full ingestion.

- [ ] **Step 1: Write and run the Polymarket probe**

`scripts/probe_polymarket.py`:

```python
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
token_ids = json.loads(m.get("clobTokenIds") or "[]")
print("clobTokenIds:", token_ids)

# 2. CLOB price history for the YES token of a long-lived 2024 market
r = client.get("https://clob.polymarket.com/prices-history",
               params={"market": token_ids[0], "interval": "max",
                       "fidelity": 1440})
r.raise_for_status()
hist = r.json()
FIXTURES.joinpath("pm_prices.json").write_text(json.dumps(hist, indent=2))
pts = hist.get("history", [])
print(f"== price history: {len(pts)} points ==")
if pts:
    from datetime import datetime, timezone
    print("first:", datetime.fromtimestamp(pts[0]["t"], tz=timezone.utc))
    print("last: ", datetime.fromtimestamp(pts[-1]["t"], tz=timezone.utc))

# 3. Rate-limit sniff: 10 rapid history calls
import time
t0 = time.time()
for _ in range(10):
    client.get("https://clob.polymarket.com/prices-history",
               params={"market": token_ids[0], "interval": "max",
                       "fidelity": 1440})
print(f"10 rapid calls in {time.time() - t0:.1f}s (watch for 429s)")
```

Run: `.venv/bin/python scripts/probe_polymarket.py`

- [ ] **Step 2: Write and run the Kalshi probe**

`scripts/probe_kalshi.py`:

```python
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

# 1. One page of settled markets that closed after Jan 2024
min_close = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())
r = client.get(f"{BASE}/markets",
               params={"limit": 20, "status": "settled",
                       "min_close_ts": min_close})
r.raise_for_status()
j = r.json()
FIXTURES.joinpath("kalshi_markets.json").write_text(json.dumps(j, indent=2))
m = j["markets"][0]
print("== market keys ==", sorted(m.keys()))
for k in ("ticker", "event_ticker", "title", "category", "volume",
          "open_time", "close_time", "result"):
    print(f"{k}: {m.get(k)}")
print("cursor present:", bool(j.get("cursor")))

# 2. Candlesticks for that market (series ticker = event ticker prefix;
#    confirm the exact series field/derivation here)
series = m.get("series_ticker") or m["event_ticker"].split("-")[0]
start = int(datetime.fromisoformat(m["open_time"].replace("Z", "+00:00")).timestamp())
end = int(datetime.fromisoformat(m["close_time"].replace("Z", "+00:00")).timestamp())
r = client.get(f"{BASE}/series/{series}/markets/{m['ticker']}/candlesticks",
               params={"start_ts": start, "end_ts": end,
                       "period_interval": 1440})
print("candlestick status:", r.status_code)
r.raise_for_status()
candles = r.json()
FIXTURES.joinpath("kalshi_candles.json").write_text(json.dumps(candles, indent=2))
cs = candles.get("candlesticks", [])
print(f"== {len(cs)} daily candles ==")
if cs:
    print("candle keys:", sorted(cs[0].keys()))
    print("sample:", json.dumps(cs[0], indent=2)[:500])

# 3. Rate-limit sniff
t0 = time.time()
for _ in range(10):
    client.get(f"{BASE}/markets", params={"limit": 1})
print(f"10 rapid calls in {time.time() - t0:.1f}s (watch for 429s)")
```

Run: `.venv/bin/python scripts/probe_kalshi.py`

- [ ] **Step 3: Write findings and adjust downstream tasks**

Write `docs/probe-findings.md` recording, for each venue: response field names actually seen, history depth (does a Jan-2024 market return full daily history?), pagination mechanics, rate-limit behavior, and **whether Polymarket exposes historical daily volume** (check the Gamma market object for any timeseries volume field; if only lifetime `volumeNum` exists, the static-weight fallback in Task 7 stands).

**CHECKPOINT:** If any field name, endpoint path, or pagination detail differs from the code in Tasks 4–5, edit those task code blocks in this plan NOW, before executing them. If history depth is insufficient for Jan 2024 backfill, stop and surface to Kian — the backfill window is a spec decision.

- [ ] **Step 4: Commit**

```bash
git add scripts tests/fixtures docs/probe-findings.md
git commit -m "feat: API feasibility probes + recorded fixtures and findings"
```

---

### Task 3: Core index math (`compute.py`)

**Files:**
- Create: `uncertainty-index/src/uindex/compute.py`
- Test: `uncertainty-index/tests/test_compute.py`

**Interfaces:**
- Consumes: `uindex.config` constants.
- Produces (exact signatures, used by Task 8):
  - `clip_prob(p) -> np.ndarray`
  - `logit(p) -> np.ndarray`
  - `binary_entropy(p) -> np.ndarray` (bits, max 1.0 at p=0.5)
  - `ewma_vol(innov: pd.Series, halflife: float) -> pd.Series`
  - `weighted_mean(values: pd.Series, weights: pd.Series) -> float`
  - `percentile_scale(raw: pd.Series, seed_days: int) -> pd.Series` (0–100, first `seed_days` NaN)

- [ ] **Step 1: Write the failing tests**

`tests/test_compute.py`:

```python
import numpy as np
import pandas as pd
import pytest

from uindex import compute
from uindex.config import CLIP_HI, CLIP_LO, SEED_DAYS


def test_clip_prob_bounds():
    out = compute.clip_prob(np.array([0.0, 0.5, 1.0]))
    assert out[0] == CLIP_LO and out[2] == CLIP_HI and out[1] == 0.5


def test_logit_center_and_symmetry():
    assert compute.logit(np.array([0.5]))[0] == pytest.approx(0.0)
    l = compute.logit(np.array([0.2, 0.8]))
    assert l[0] == pytest.approx(-l[1])


def test_logit_tail_moves_dominate():
    # 2% -> 7% must register as a bigger move than 50% -> 55%
    tail = compute.logit(np.array([0.07]))[0] - compute.logit(np.array([0.02]))[0]
    mid = compute.logit(np.array([0.55]))[0] - compute.logit(np.array([0.50]))[0]
    assert tail > 3 * mid


def test_entropy_max_at_half_and_symmetric():
    assert compute.binary_entropy(np.array([0.5]))[0] == pytest.approx(1.0)
    e = compute.binary_entropy(np.array([0.05, 0.95]))
    assert e[0] == pytest.approx(e[1])
    assert e[0] < 0.5


def test_ewma_vol_spikes_on_shock():
    rng = np.random.default_rng(0)
    innov = pd.Series(rng.normal(0, 0.02, 100))
    innov.iloc[80] = 2.0
    vol = compute.ewma_vol(innov, halflife=10)
    assert vol.iloc[80] > 5 * vol.iloc[79]
    assert vol.iloc[:4].isna().all()  # min_periods respected


def test_weighted_mean_ignores_nan_and_zero_weight():
    v = pd.Series([1.0, np.nan, 3.0, 100.0])
    w = pd.Series([1.0, 5.0, 1.0, 0.0])
    assert compute.weighted_mean(v, w) == pytest.approx(2.0)


def test_weighted_mean_empty_is_nan():
    assert np.isnan(compute.weighted_mean(pd.Series([np.nan]), pd.Series([1.0])))


def test_percentile_scale_seed_and_range():
    idx = pd.date_range("2024-01-01", periods=200, freq="D")
    raw = pd.Series(np.linspace(1, 2, 200), index=idx)
    scaled = compute.percentile_scale(raw, seed_days=SEED_DAYS)
    assert scaled.iloc[:SEED_DAYS].isna().all()
    # monotone series: every post-seed day is a new max -> 100
    assert (scaled.iloc[SEED_DAYS:] == 100.0).all()
    assert scaled.max() <= 100 and scaled.min() >= 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_compute.py -q`
Expected: FAIL — `ModuleNotFoundError` / `AttributeError` on `uindex.compute`.

- [ ] **Step 3: Write the implementation**

`src/uindex/compute.py`:

```python
"""Pure index math. No I/O, no venue knowledge."""
import numpy as np
import pandas as pd

from . import config


def clip_prob(p: np.ndarray) -> np.ndarray:
    return np.clip(np.asarray(p, dtype=float), config.CLIP_LO, config.CLIP_HI)


def logit(p: np.ndarray) -> np.ndarray:
    p = clip_prob(p)
    return np.log(p / (1.0 - p))


def binary_entropy(p: np.ndarray) -> np.ndarray:
    p = clip_prob(p)
    return -(p * np.log2(p) + (1.0 - p) * np.log2(1.0 - p))


def ewma_vol(innov: pd.Series, halflife: float = config.EWMA_HALFLIFE_DAYS) -> pd.Series:
    return (
        innov.pow(2)
        .ewm(halflife=halflife, min_periods=config.EWMA_MIN_PERIODS)
        .mean()
        .pow(0.5)
    )


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return float("nan")
    return float(np.average(values[mask], weights=weights[mask]))


def percentile_scale(raw: pd.Series, seed_days: int = config.SEED_DAYS) -> pd.Series:
    """Expanding percentile of each value vs strictly-prior history, 0-100.

    First seed_days values are NaN (seed period, not publishable).
    """
    scaled = raw.expanding(min_periods=2).apply(
        lambda w: float((w[:-1] <= w[-1]).mean() * 100.0), raw=True
    )
    scaled.iloc[:seed_days] = np.nan
    return scaled
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_compute.py -q`
Expected: `8 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/compute.py tests/test_compute.py
git commit -m "feat: core index math - logit, entropy, ewma vol, percentile scale"
```

---

### Task 4: Polymarket ingestion

**Files:**
- Create: `uncertainty-index/src/uindex/ingest/__init__.py` (empty)
- Create: `uncertainty-index/src/uindex/ingest/polymarket.py`
- Test: `uncertainty-index/tests/test_ingest_polymarket.py`

**Interfaces:**
- Consumes: fixtures from Task 2; `uindex.config`.
- Produces:
  - `markets_to_df(markets: list[dict]) -> pd.DataFrame` with columns `market_id, venue, question, venue_category, yes_token_id, total_volume_usd, open_date, close_date` (dates as `datetime64[ns]`, naive UTC).
  - `history_to_df(payload: dict, market_id: str) -> pd.DataFrame` with columns `market_id, date, close_prob`.
  - CLI `python -m uindex.ingest.polymarket` writing `data/raw/polymarket/markets.parquet` and `data/raw/polymarket/prices.parquet`, resumable.

> NOTE: field names below follow the documented Gamma/CLOB shapes; the Task 2 checkpoint has already corrected them against real responses if they differed.

- [ ] **Step 1: Write the failing tests (fixture-driven, no network)**

`tests/test_ingest_polymarket.py`:

```python
import json
from pathlib import Path

import pandas as pd

from uindex.ingest import polymarket as pm

FIXTURES = Path(__file__).parent / "fixtures"


def _markets():
    return json.loads((FIXTURES / "pm_markets.json").read_text())


def test_markets_to_df_schema():
    df = pm.markets_to_df(_markets())
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category",
        "yes_token_id", "total_volume_usd", "open_date", "close_date",
    ]
    assert (df["venue"] == "polymarket").all()
    assert df["market_id"].str.startswith("pm_").all()
    assert pd.api.types.is_datetime64_any_dtype(df["close_date"])
    assert df["close_date"].dt.tz is None


def test_markets_without_tokens_are_dropped():
    broken = _markets() + [{"id": "999", "question": "no tokens", "clobTokenIds": "[]"}]
    df = pm.markets_to_df(broken)
    assert "pm_999" not in set(df["market_id"])


def test_history_to_df_daily_close():
    payload = json.loads((FIXTURES / "pm_prices.json").read_text())
    df = pm.history_to_df(payload, market_id="pm_1")
    assert list(df.columns) == ["market_id", "date", "close_prob"]
    assert df["date"].is_monotonic_increasing
    assert not df["date"].duplicated().any()  # one close per day
    assert df["close_prob"].between(0, 1).all()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_ingest_polymarket.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

`src/uindex/ingest/polymarket.py`:

```python
"""Polymarket ingestion: Gamma (metadata) + CLOB (price history)."""
import json
import time
from pathlib import Path

import httpx
import pandas as pd

from .. import config

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
HISTORY_URL = "https://clob.polymarket.com/prices-history"
OUT_DIR = config.DATA_DIR / "raw" / "polymarket"
SLEEP_S = 0.3  # adjust per Task 2 rate-limit findings


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
    df = pd.DataFrame(rows)
    for col in ("open_date", "close_date"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(None)
    return df


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
    df.insert(0, "market_id", market_id)
    return df[["market_id", "date", "close_prob"]]


def fetch_all_markets(client: httpx.Client) -> list[dict]:
    out, offset = [], 0
    while True:
        r = client.get(GAMMA_URL, params={
            "limit": 500, "offset": offset,
            "end_date_min": config.BACKFILL_START,
        })
        r.raise_for_status()
        batch = r.json()
        if not batch:
            return out
        out.extend(batch)
        offset += 500
        time.sleep(SLEEP_S)


def fetch_history(client: httpx.Client, token_id: str) -> dict:
    r = client.get(HISTORY_URL, params={
        "market": token_id, "interval": "max", "fidelity": 1440,
    })
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
    print(f"{len(meta)} polymarket markets")

    prices_path = OUT_DIR / "prices.parquet"
    done: set[str] = set()
    frames: list[pd.DataFrame] = []
    if prices_path.exists():
        existing = pd.read_parquet(prices_path)
        done = set(existing["market_id"].unique())
        frames = [existing]

    todo = meta[~meta["market_id"].isin(done)]
    for i, row in enumerate(todo.itertuples(index=False)):
        try:
            frames.append(history_to_df(fetch_history(client, row.yes_token_id),
                                        row.market_id))
        except httpx.HTTPStatusError as e:
            print(f"skip {row.market_id}: {e.response.status_code}")
        time.sleep(SLEEP_S)
        if i % 200 == 199:  # checkpoint so the run is resumable
            pd.concat(frames, ignore_index=True).to_parquet(prices_path, index=False)
            print(f"checkpoint: {i + 1}/{len(todo)}")
    pd.concat(frames, ignore_index=True).to_parquet(prices_path, index=False)
    print("polymarket ingestion complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_ingest_polymarket.py -q`
Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/ingest tests/test_ingest_polymarket.py
git commit -m "feat: polymarket ingestion (gamma metadata + clob daily closes)"
```

---

### Task 5: Kalshi ingestion

**Files:**
- Create: `uncertainty-index/src/uindex/ingest/kalshi.py`
- Test: `uncertainty-index/tests/test_ingest_kalshi.py`

**Interfaces:**
- Consumes: fixtures from Task 2; `uindex.config`.
- Produces:
  - `markets_to_df(markets: list[dict]) -> pd.DataFrame` with columns `market_id, venue, question, venue_category, event_ticker, ticker, series_ticker, total_volume_usd, open_date, close_date`.
  - `candles_to_df(payload: dict, market_id: str) -> pd.DataFrame` with columns `market_id, date, close_prob, daily_notional_usd`.
  - CLI `python -m uindex.ingest.kalshi` writing `data/raw/kalshi/markets.parquet` and `data/raw/kalshi/prices.parquet`, resumable.

> NOTE: prices in Kalshi candles are cents (divide by 100); notional ≈ contracts × price in dollars. Confirmed/corrected at the Task 2 checkpoint.

- [ ] **Step 1: Write the failing tests**

`tests/test_ingest_kalshi.py`:

```python
import json
from pathlib import Path

import pandas as pd

from uindex.ingest import kalshi

FIXTURES = Path(__file__).parent / "fixtures"


def test_markets_to_df_schema():
    payload = json.loads((FIXTURES / "kalshi_markets.json").read_text())
    df = kalshi.markets_to_df(payload["markets"])
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category", "event_ticker",
        "ticker", "series_ticker", "total_volume_usd", "open_date", "close_date",
    ]
    assert (df["venue"] == "kalshi").all()
    assert df["market_id"].str.startswith("ka_").all()
    assert pd.api.types.is_datetime64_any_dtype(df["close_date"])


def test_candles_to_df_prob_and_notional():
    payload = json.loads((FIXTURES / "kalshi_candles.json").read_text())
    df = kalshi.candles_to_df(payload, market_id="ka_TEST")
    assert list(df.columns) == ["market_id", "date", "close_prob", "daily_notional_usd"]
    assert df["close_prob"].between(0, 1).all()  # cents converted
    assert (df["daily_notional_usd"] >= 0).all()
    assert not df["date"].duplicated().any()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_ingest_kalshi.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

`src/uindex/ingest/kalshi.py`:

```python
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
            "venue_category": (m.get("category") or "").strip(),
            "event_ticker": m["event_ticker"],
            "ticker": m["ticker"],
            "series_ticker": series,
            # volume is contracts; ~$0.50 avg price is a fair notional proxy
            "total_volume_usd": float(m.get("volume") or 0) * 0.5,
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
        close_cents = (c.get("price") or {}).get("close")
        if close_cents is None:
            continue
        close_prob = close_cents / 100.0
        volume = float(c.get("volume") or 0)
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
    r = client.get(f"{BASE}/series/{series}/markets/{ticker}/candlesticks",
                   params={"start_ts": start_ts, "end_ts": end_ts,
                           "period_interval": 1440})
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_ingest_kalshi.py -q`
Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/ingest/kalshi.py tests/test_ingest_kalshi.py
git commit -m "feat: kalshi ingestion (markets + daily candlesticks)"
```

---

### Task 6: Normalization + taxonomy

**Files:**
- Create: `uncertainty-index/src/uindex/normalize.py`
- Test: `uncertainty-index/tests/test_normalize.py`

**Interfaces:**
- Consumes: raw parquets from Tasks 4–5 (`markets.parquet`, `prices.parquet` per venue).
- Produces:
  - `categorize(question: str, venue_category: str) -> str` — returns a `CATEGORY_RULES` key, `"SPORTS"`, or `"UNMAPPED"`.
  - `build_panel(pm_meta, pm_prices, ka_meta, ka_prices) -> tuple[pd.DataFrame, pd.DataFrame]` returning `(meta, panel)`:
    - `meta`: `market_id, venue, question, category, event_ticker (NaN for pm), total_volume_usd, open_date, close_date` — SPORTS and UNMAPPED rows **dropped** (logged first).
    - `panel`: `market_id, date, close_prob, daily_notional_usd` (NaN for polymarket), inner-joined to surviving meta.
  - CLI `python -m uindex.normalize` writing `data/normalized/meta.parquet`, `data/normalized/panel.parquet`, `data/normalized/unmapped_triage.csv`.

- [ ] **Step 1: Write the failing tests**

`tests/test_normalize.py`:

```python
import pandas as pd

from uindex import normalize


def test_sports_by_keyword_and_by_venue_category():
    assert normalize.categorize("Will the Chiefs win the Super Bowl?", "") == "SPORTS"
    assert normalize.categorize("Who wins game 7?", "Sports") == "SPORTS"


def test_keyword_rules_first_match_wins():
    assert normalize.categorize("Will Russia and Ukraine sign a ceasefire?", "Politics") == "WAR"
    assert normalize.categorize("Will the Fed cut rates in September?", "") == "ECON_FED"
    assert normalize.categorize("Will Bitcoin close above $150k?", "") == "CRYPTO"


def test_venue_category_fallback_and_unmapped():
    assert normalize.categorize("Something opaque", "Economics") == "ECON_FED"
    assert normalize.categorize("Something opaque", "Mystery") == "UNMAPPED"


def _tiny_inputs():
    pm_meta = pd.DataFrame({
        "market_id": ["pm_1", "pm_2"],
        "venue": ["polymarket"] * 2,
        "question": ["Will the Fed cut rates?", "Will the Lakers win?"],
        "venue_category": ["", "Sports"],
        "yes_token_id": ["a", "b"],
        "total_volume_usd": [100000.0, 500000.0],
        "open_date": pd.to_datetime(["2024-01-01"] * 2),
        "close_date": pd.to_datetime(["2024-06-01"] * 2),
    })
    pm_prices = pd.DataFrame({
        "market_id": ["pm_1", "pm_1", "pm_2"],
        "date": pd.to_datetime(["2024-02-01", "2024-02-02", "2024-02-01"]),
        "close_prob": [0.4, 0.45, 0.7],
    })
    ka_meta = pd.DataFrame({
        "market_id": ["ka_X"], "venue": ["kalshi"],
        "question": ["Will CPI exceed 3%?"], "venue_category": ["Economics"],
        "event_ticker": ["CPI-24"], "ticker": ["CPI-24-T3"],
        "series_ticker": ["CPI"], "total_volume_usd": [20000.0],
        "open_date": pd.to_datetime(["2024-01-01"]),
        "close_date": pd.to_datetime(["2024-06-01"]),
    })
    ka_prices = pd.DataFrame({
        "market_id": ["ka_X"], "date": pd.to_datetime(["2024-02-01"]),
        "close_prob": [0.3], "daily_notional_usd": [1500.0],
    })
    return pm_meta, pm_prices, ka_meta, ka_prices


def test_build_panel_drops_sports_and_unifies():
    meta, panel = normalize.build_panel(*_tiny_inputs())
    assert set(meta["market_id"]) == {"pm_1", "ka_X"}  # Lakers dropped
    assert set(meta.columns) == {
        "market_id", "venue", "question", "category", "event_ticker",
        "total_volume_usd", "open_date", "close_date",
    }
    assert set(panel["market_id"]) == {"pm_1", "ka_X"}
    pm_rows = panel[panel["market_id"] == "pm_1"]
    assert pm_rows["daily_notional_usd"].isna().all()  # no pm daily volume
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_normalize.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

`src/uindex/normalize.py`:

```python
"""Unify venue data into one schema; apply taxonomy; drop sports/unmapped."""
import numpy as np
import pandas as pd

from . import config

META_COLS = ["market_id", "venue", "question", "category", "event_ticker",
             "total_volume_usd", "open_date", "close_date"]


def categorize(question: str, venue_category: str) -> str:
    text = (question or "").lower()
    vcat = (venue_category or "").strip().lower()
    if vcat in config.SPORTS_VENUE_CATEGORIES or any(
            k in text for k in config.SPORTS_KEYWORDS):
        return "SPORTS"
    for cat, keywords in config.CATEGORY_RULES.items():
        if any(k in text for k in keywords):
            return cat
    return config.VENUE_CATEGORY_MAP.get(vcat, "UNMAPPED")


def build_panel(pm_meta: pd.DataFrame, pm_prices: pd.DataFrame,
                ka_meta: pd.DataFrame, ka_prices: pd.DataFrame
                ) -> tuple[pd.DataFrame, pd.DataFrame]:
    pm = pm_meta.copy()
    pm["event_ticker"] = np.nan
    ka = ka_meta.copy()
    meta = pd.concat([pm, ka], ignore_index=True)
    meta["category"] = [
        categorize(q, c) for q, c in zip(meta["question"], meta["venue_category"])
    ]
    dropped = meta[meta["category"].isin(["SPORTS", "UNMAPPED"])]
    meta = meta[~meta["category"].isin(["SPORTS", "UNMAPPED"])][META_COLS]

    pmp = pm_prices.copy()
    pmp["daily_notional_usd"] = np.nan
    panel = pd.concat([pmp, ka_prices], ignore_index=True)
    panel = panel.merge(meta[["market_id"]], on="market_id", how="inner")
    panel = panel.sort_values(["market_id", "date"]).reset_index(drop=True)
    build_panel.last_dropped = dropped  # exposed for the CLI triage log
    return meta.reset_index(drop=True), panel


def main() -> None:
    raw = config.DATA_DIR / "raw"
    out = config.DATA_DIR / "normalized"
    out.mkdir(parents=True, exist_ok=True)

    meta, panel = build_panel(
        pd.read_parquet(raw / "polymarket" / "markets.parquet"),
        pd.read_parquet(raw / "polymarket" / "prices.parquet"),
        pd.read_parquet(raw / "kalshi" / "markets.parquet"),
        pd.read_parquet(raw / "kalshi" / "prices.parquet"),
    )
    meta.to_parquet(out / "meta.parquet", index=False)
    panel.to_parquet(out / "panel.parquet", index=False)

    dropped = build_panel.last_dropped
    triage = dropped[dropped["category"] == "UNMAPPED"]
    triage[["market_id", "venue", "question", "venue_category",
            "total_volume_usd"]].to_csv(out / "unmapped_triage.csv", index=False)
    n_sports = int((dropped["category"] == "SPORTS").sum())
    print(f"kept {len(meta)} markets | dropped {n_sports} sports, "
          f"{len(triage)} unmapped (see unmapped_triage.csv)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_normalize.py -q`
Expected: `5 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/normalize.py tests/test_normalize.py
git commit -m "feat: normalization to unified schema with taxonomy and sports drop"
```

---

### Task 7: Point-in-time universe construction

**Files:**
- Create: `uncertainty-index/src/uindex/universe.py`
- Create: `uncertainty-index/data-overrides/duplicates.csv` (committed — it's config, not data)
- Test: `uncertainty-index/tests/test_universe.py`

**Interfaces:**
- Consumes: `(meta, panel)` from Task 6.
- Produces:
  - `apply_pit_rules(meta, panel, params: dict | None = None) -> pd.DataFrame` — returns panel with added columns `eligible: bool`, `weight: float`, joined `category`. Params keys (defaults from config): `resolution_exclusion_days`, `pm_min_total_volume`, `ka_min_rolling_notional`, `rolling_window_days`.
  - Weight rule: Kalshi = 7-day rolling mean of `daily_notional_usd`; Polymarket = `ln(1 + total_volume_usd)` (static — documented limitation). Weights are only compared after daily normalization within an index, so mixed scales are fine.
  - Strike-group rule: within a Kalshi `event_ticker`, only the strike with max `total_volume_usd` (lifetime, deterministic) is ever eligible.
  - Dedup rule: rows in `data-overrides/duplicates.csv` (columns `drop_market_id,reason`) are never eligible; exact normalized-question duplicates across venues keep the higher `total_volume_usd`.

- [ ] **Step 1: Write the failing tests**

`tests/test_universe.py`:

```python
import numpy as np
import pandas as pd

from uindex import universe


def _meta_panel():
    meta = pd.DataFrame({
        "market_id": ["pm_a", "pm_small", "ka_s1", "ka_s2"],
        "venue": ["polymarket", "polymarket", "kalshi", "kalshi"],
        "question": ["Will the Fed cut rates?", "Will inflation hit 5%?",
                     "CPI above 3%?", "CPI above 4%?"],
        "category": ["ECON_FED"] * 4,
        "event_ticker": [np.nan, np.nan, "CPI-24", "CPI-24"],
        "total_volume_usd": [200000.0, 100.0, 50000.0, 9000.0],
        "open_date": pd.to_datetime(["2024-01-01"] * 4),
        "close_date": pd.to_datetime(["2024-03-01"] * 4),
    })
    dates = pd.date_range("2024-01-02", "2024-02-28", freq="D")
    frames = []
    for mid in meta["market_id"]:
        frames.append(pd.DataFrame({
            "market_id": mid, "date": dates, "close_prob": 0.5,
            "daily_notional_usd": 1000.0 if mid.startswith("ka_") else np.nan,
        }))
    return meta, pd.concat(frames, ignore_index=True)


def test_resolution_exclusion_window():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    a = out[out["market_id"] == "pm_a"]
    last3 = a[a["date"] > pd.Timestamp("2024-03-01") - pd.Timedelta(days=3)]
    assert not last3["eligible"].any()
    assert a[a["date"] == pd.Timestamp("2024-02-01")]["eligible"].all()


def test_liquidity_floor_drops_small_polymarket():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "pm_small"]["eligible"].any()


def test_strike_group_keeps_only_most_liquid():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "ka_s1"]["eligible"].any()   # 50k volume
    assert not out[out["market_id"] == "ka_s2"]["eligible"].any()  # 9k, same event


def test_kalshi_rolling_notional_floor():
    meta, panel = _meta_panel()
    # 1000/day * 7d rolling mean = 1000 < 5000 floor -> ineligible
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "ka_s1"]["eligible"].iloc[10:].any()
    # raise notional -> eligible after rolling window fills
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    out2 = universe.apply_pit_rules(meta, panel)
    assert out2[out2["market_id"] == "ka_s1"]["eligible"].iloc[10:-3].all()


def test_manual_override_dedup(tmp_path):
    meta, panel = _meta_panel()
    csv = tmp_path / "duplicates.csv"
    csv.write_text("drop_market_id,reason\npm_a,test dup\n")
    out = universe.apply_pit_rules(meta, panel, overrides_path=csv)
    assert not out[out["market_id"] == "pm_a"]["eligible"].any()


def test_weights_positive_for_eligible():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert (out.loc[out["eligible"], "weight"] > 0).all()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_universe.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

Also create `data-overrides/duplicates.csv` with just the header line: `drop_market_id,reason`

`src/uindex/universe.py`:

```python
"""Point-in-time eligibility and weights. Panel in, panel + flags out."""
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

DEFAULT_OVERRIDES = config.PROJECT_ROOT / "data-overrides" / "duplicates.csv"


def _default_params() -> dict:
    return {
        "resolution_exclusion_days": config.RESOLUTION_EXCLUSION_DAYS,
        "pm_min_total_volume": config.POLYMARKET_MIN_TOTAL_VOLUME_USD,
        "ka_min_rolling_notional": config.KALSHI_MIN_ROLLING_NOTIONAL_USD,
        "rolling_window_days": config.ROLLING_WINDOW_DAYS,
    }


def _normalized_question(q: str) -> str:
    return " ".join((q or "").lower().split())


def apply_pit_rules(meta: pd.DataFrame, panel: pd.DataFrame,
                    params: dict | None = None,
                    overrides_path: Path = DEFAULT_OVERRIDES) -> pd.DataFrame:
    p = {**_default_params(), **(params or {})}
    df = panel.merge(
        meta[["market_id", "venue", "category", "event_ticker",
              "total_volume_usd", "close_date", "question"]],
        on="market_id", how="left",
    ).sort_values(["market_id", "date"])

    eligible = df["close_prob"].notna()

    # 1. Resolution-collapse guard
    cutoff = df["close_date"] - pd.Timedelta(days=p["resolution_exclusion_days"])
    eligible &= df["date"] < cutoff

    # 2. Liquidity floors + weights
    is_pm = df["venue"] == "polymarket"
    eligible &= ~(is_pm & (df["total_volume_usd"] < p["pm_min_total_volume"]))
    rolling = (
        df.groupby("market_id")["daily_notional_usd"]
        .transform(lambda s: s.rolling(p["rolling_window_days"],
                                       min_periods=p["rolling_window_days"]).mean())
    )
    is_ka = df["venue"] == "kalshi"
    eligible &= ~(is_ka & ~(rolling >= p["ka_min_rolling_notional"]))
    df["weight"] = np.where(is_pm, np.log1p(df["total_volume_usd"]), rolling)

    # 3. Kalshi strike groups: lifetime-most-liquid strike represents the event
    ka_meta = meta[meta["venue"] == "kalshi"].dropna(subset=["event_ticker"])
    reps = ka_meta.loc[
        ka_meta.groupby("event_ticker")["total_volume_usd"].idxmax(), "market_id"
    ]
    non_reps = set(ka_meta["market_id"]) - set(reps)
    eligible &= ~df["market_id"].isin(non_reps)

    # 4. Cross-venue exact-question dedup: keep higher lifetime volume
    m = meta.copy()
    m["qnorm"] = m["question"].map(_normalized_question)
    keep = m.loc[m.groupby("qnorm")["total_volume_usd"].idxmax(), "market_id"]
    eligible &= df["market_id"].isin(set(keep))

    # 5. Manual overrides
    if Path(overrides_path).exists():
        drops = set(pd.read_csv(overrides_path)["drop_market_id"])
        eligible &= ~df["market_id"].isin(drops)

    df["eligible"] = eligible.fillna(False)
    return df.drop(columns=["question"]).reset_index(drop=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_universe.py -q`
Expected: `6 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/universe.py tests/test_universe.py data-overrides/duplicates.csv
git commit -m "feat: point-in-time universe rules - exclusion, floors, strikes, dedup"
```

---

### Task 8: Index computation pipeline

**Files:**
- Create: `uncertainty-index/src/uindex/pipeline.py`
- Test: `uncertainty-index/tests/test_pipeline.py`

**Interfaces:**
- Consumes: `apply_pit_rules` output (Task 7), `compute` functions (Task 3), `config.INDEX_UNIVERSES`.
- Produces:
  - `compute_indices(flagged_panel: pd.DataFrame, params: dict | None = None) -> pd.DataFrame` — tidy output with columns `date, index, gauge, raw, value` where `gauge ∈ {"turbulence", "unresolvedness"}`, `value` is the 0–100 scaled series (NaN during seed). Params keys: `ewma_halflife`, `seed_days`.
  - CLI `python -m uindex.pipeline` reading `data/normalized/`, writing `data/indices/indices.parquet` and `data/indices/constituents.parquet` (per date/index member counts for the churn audit).

- [ ] **Step 1: Write the failing tests (golden-day + reproducibility)**

`tests/test_pipeline.py`:

```python
import numpy as np
import pandas as pd

from uindex import pipeline


def _synthetic_flagged_panel(shock_day="2024-09-01"):
    """120 days x 8 ECON_FED markets, small logit noise, one big shock day."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-06-01", periods=120, freq="D")
    frames = []
    for i in range(8):
        logit_path = np.cumsum(rng.normal(0, 0.05, 120))
        shock_idx = list(dates).index(pd.Timestamp(shock_day))
        logit_path[shock_idx:] += 2.5  # violent repricing on shock day
        probs = 1 / (1 + np.exp(-logit_path))
        frames.append(pd.DataFrame({
            "market_id": f"m{i}", "date": dates,
            "close_prob": np.clip(probs, 0.02, 0.98),
            "category": "ECON_FED", "eligible": True, "weight": 1.0,
        }))
    return pd.concat(frames, ignore_index=True)


def test_golden_day_turbulence_spikes_on_shock():
    panel = _synthetic_flagged_panel()
    out = pipeline.compute_indices(panel, params={"seed_days": 30})
    turb = out[(out["index"] == "ECON_FED") & (out["gauge"] == "turbulence")]
    turb = turb.set_index("date")["value"].dropna()
    assert turb.idxmax() == pd.Timestamp("2024-09-01")
    assert turb.max() > 95


def test_global_includes_econ_markets():
    panel = _synthetic_flagged_panel()
    out = pipeline.compute_indices(panel, params={"seed_days": 30})
    glob = out[(out["index"] == "GLOBAL") & (out["gauge"] == "turbulence")]
    assert glob["raw"].notna().sum() > 50


def test_unresolvedness_present_and_bounded():
    panel = _synthetic_flagged_panel()
    out = pipeline.compute_indices(panel, params={"seed_days": 30})
    unres = out[out["gauge"] == "unresolvedness"]["value"].dropna()
    assert len(unres) > 0
    assert unres.between(0, 100).all()


def test_ineligible_markets_do_not_move_index():
    panel = _synthetic_flagged_panel()
    out1 = pipeline.compute_indices(panel, params={"seed_days": 30})
    poisoned = panel.copy()
    extra = panel[panel["market_id"] == "m0"].copy()
    extra["market_id"] = "poison"
    extra["close_prob"] = 0.5
    extra["eligible"] = False
    out2 = pipeline.compute_indices(pd.concat([poisoned, extra]),
                                    params={"seed_days": 30})
    merged = out1.merge(out2, on=["date", "index", "gauge"], suffixes=("_a", "_b"))
    assert np.allclose(merged["raw_a"].dropna(), merged["raw_b"].dropna())


def test_reproducibility_byte_identical():
    panel = _synthetic_flagged_panel()
    a = pipeline.compute_indices(panel, params={"seed_days": 30})
    b = pipeline.compute_indices(panel, params={"seed_days": 30})
    pd.testing.assert_frame_equal(a, b)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_pipeline.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

`src/uindex/pipeline.py`:

```python
"""Turn a flagged panel into daily index series (both gauges, all indices)."""
import numpy as np
import pandas as pd

from . import compute, config, normalize, universe


def _index_series(sub: pd.DataFrame, ewma_halflife: float) -> pd.DataFrame:
    """sub: eligible rows of one universe. Returns date-indexed raw gauges."""
    probs = sub.pivot_table(index="date", columns="market_id",
                            values="close_prob", aggfunc="last")
    weights = sub.pivot_table(index="date", columns="market_id",
                              values="weight", aggfunc="last")

    logits = pd.DataFrame(compute.logit(probs.values),
                          index=probs.index, columns=probs.columns)
    vols = logits.diff().apply(
        lambda col: compute.ewma_vol(col.dropna(), halflife=ewma_halflife)
    ).reindex(probs.index)
    entropy = pd.DataFrame(compute.binary_entropy(probs.values),
                           index=probs.index, columns=probs.columns)

    rows = []
    for date in probs.index:
        w = weights.loc[date]
        rows.append({
            "date": date,
            "turbulence": compute.weighted_mean(vols.loc[date], w),
            "unresolvedness": compute.weighted_mean(entropy.loc[date], w),
            "n_constituents": int(probs.loc[date].notna().sum()),
        })
    return pd.DataFrame(rows).set_index("date")


def compute_indices(flagged_panel: pd.DataFrame,
                    params: dict | None = None) -> pd.DataFrame:
    p = {"ewma_halflife": config.EWMA_HALFLIFE_DAYS,
         "seed_days": config.SEED_DAYS, **(params or {})}
    eligible = flagged_panel[flagged_panel["eligible"]]

    tidy, members = [], []
    for index_name, categories in config.INDEX_UNIVERSES.items():
        sub = eligible[eligible["category"].isin(categories)]
        if sub.empty:
            continue
        series = _index_series(sub, p["ewma_halflife"])
        for gauge in ("turbulence", "unresolvedness"):
            raw = series[gauge]
            scaled = compute.percentile_scale(raw, seed_days=p["seed_days"])
            tidy.append(pd.DataFrame({
                "date": series.index, "index": index_name, "gauge": gauge,
                "raw": raw.values, "value": scaled.values,
            }))
        members.append(pd.DataFrame({
            "date": series.index, "index": index_name,
            "n_constituents": series["n_constituents"].values,
        }))

    out = pd.concat(tidy, ignore_index=True).sort_values(
        ["index", "gauge", "date"]).reset_index(drop=True)
    compute_indices.constituents = pd.concat(members, ignore_index=True)
    return out


def main() -> None:
    norm = config.DATA_DIR / "normalized"
    out_dir = config.DATA_DIR / "indices"
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = pd.read_parquet(norm / "meta.parquet")
    panel = pd.read_parquet(norm / "panel.parquet")
    flagged = universe.apply_pit_rules(meta, panel)
    indices = compute_indices(flagged)

    indices.to_parquet(out_dir / "indices.parquet", index=False)
    compute_indices.constituents.to_parquet(out_dir / "constituents.parquet",
                                            index=False)
    for name in config.INDEXES:
        sub = indices[(indices["index"] == name) &
                      (indices["gauge"] == "turbulence")]["value"].dropna()
        print(f"{name:10s} turbulence: {len(sub)} days, "
              f"last={sub.iloc[-1]:.0f}" if len(sub) else f"{name:10s} EMPTY")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_pipeline.py -q`
Expected: `5 passed`. Then run the full suite: `.venv/bin/pytest -q` — everything passes.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/pipeline.py tests/test_pipeline.py
git commit -m "feat: index computation pipeline with golden-day and reproducibility tests"
```

---

### Task 9: Backfill execution (operational)

**Files:**
- Create: `uncertainty-index/docs/backfill-notes.md`

**Interfaces:**
- Consumes: all CLIs from Tasks 4–8.
- Produces: populated `data/` tree; `docs/backfill-notes.md` with counts and issues — consumed by Tasks 10–11.

No TDD here — this is a monitored production run. Ingestion may take hours; run each step in the background and check output.

- [ ] **Step 1: Run Polymarket ingestion**

```bash
cd uncertainty-index
.venv/bin/python -m uindex.ingest.polymarket
```

Resumable: re-run on crash/rate-limit; it skips completed markets.

- [ ] **Step 2: Run Kalshi ingestion**

```bash
.venv/bin/python -m uindex.ingest.kalshi
```

- [ ] **Step 3: Normalize and inspect the triage log**

```bash
.venv/bin/python -m uindex.normalize
```

Open `data/normalized/unmapped_triage.csv` sorted by `total_volume_usd`. If any high-volume market family (>$1M) is unmapped, add keywords/venue-category mappings to `config.py`, re-run, and note the additions. Target: unmapped share of total volume < 20%.

- [ ] **Step 4: Compute indices and sanity-check**

```bash
.venv/bin/python -m uindex.pipeline
.venv/bin/python - <<'EOF'
import duckdb
con = duckdb.connect()
print(con.sql("""
    SELECT index, gauge, count(*) AS days,
           min(date) AS first_day, max(date) AS last_day
    FROM 'data/indices/indices.parquet'
    WHERE value IS NOT NULL GROUP BY 1,2 ORDER BY 1,2
"""))
print(con.sql("""
    SELECT index, avg(n_constituents) AS avg_members
    FROM 'data/indices/constituents.parquet' GROUP BY 1 ORDER BY 1
"""))
EOF
```

Sanity gates: every index has published (non-NaN) values from ~Apr 2024 to within a week of today; GLOBAL averages ≥ 100 constituents; no themed index averages < 10 (if one does, note it — its 0–100 scale will be noisy and the validation report must flag it).

- [ ] **Step 5: Write notes and commit**

Write `docs/backfill-notes.md`: market counts per venue, date coverage, unmapped share, taxonomy additions made, ingestion issues (rate limits, gaps, skipped markets), constituent counts per index.

```bash
git add docs/backfill-notes.md src/uindex/config.py
git commit -m "chore: backfill run notes and taxonomy refinements"
```

---

### Task 10: Benchmark data + comparison analysis

**Files:**
- Create: `uncertainty-index/src/uindex/validate/__init__.py` (empty)
- Create: `uncertainty-index/src/uindex/validate/benchmarks.py`
- Test: `uncertainty-index/tests/test_benchmarks.py`

**Interfaces:**
- Consumes: `data/indices/indices.parquet`.
- Produces:
  - `align(index_series: pd.Series, bench: pd.Series) -> pd.DataFrame` — inner-joined on date, columns `["idx", "bench"]`.
  - `corr_and_leadlag(joined: pd.DataFrame, max_lag: int = 10) -> dict` — keys `level_corr`, `diff_corr`, `leadlag` (dict lag→corr of diffs; negative lag = our index leads).
  - CLI `python -m uindex.validate.benchmarks` downloading VIX (FRED CSV), EPU (policyuncertainty.com CSV), GPR (Iacoviello daily .xls) to `data/benchmarks/`, computing stats for GLOBAL turbulence vs each, writing `data/benchmarks/comparison.json` and charts to `docs/validation/`.

Read the `dataviz` skill before writing the chart code in this task.

- [ ] **Step 1: Write the failing tests (pure functions only — downloads aren't unit-tested)**

`tests/test_benchmarks.py`:

```python
import numpy as np
import pandas as pd

from uindex.validate import benchmarks


def _pair(lead_days=0):
    """Bench = our index shifted; if lead_days>0 our index LEADS the bench."""
    rng = np.random.default_rng(7)
    dates = pd.date_range("2024-01-01", periods=300, freq="D")
    ours = pd.Series(np.cumsum(rng.normal(0, 1, 300)), index=dates)
    bench = ours.shift(lead_days) + rng.normal(0, 0.1, 300)
    return ours, bench


def test_align_inner_joins_on_date():
    ours, bench = _pair()
    joined = benchmarks.align(ours, bench.dropna())
    assert list(joined.columns) == ["idx", "bench"]
    assert joined.notna().all().all()


def test_corr_detects_contemporaneous_relation():
    ours, bench = _pair(lead_days=0)
    stats = benchmarks.corr_and_leadlag(benchmarks.align(ours, bench))
    assert stats["level_corr"] > 0.9


def test_leadlag_detects_our_lead():
    ours, bench = _pair(lead_days=3)
    stats = benchmarks.corr_and_leadlag(benchmarks.align(ours, bench))
    best_lag = max(stats["leadlag"], key=stats["leadlag"].get)
    assert best_lag == -3  # our index leads by 3 days
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_benchmarks.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementation**

`src/uindex/validate/benchmarks.py`:

```python
"""Compare GLOBAL turbulence to VIX, EPU, GPR: correlation + lead-lag."""
import json
from pathlib import Path

import httpx
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .. import config

BENCH_DIR = config.DATA_DIR / "benchmarks"
CHART_DIR = config.PROJECT_ROOT / "docs" / "validation"

SOURCES = {
    "VIX": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS",
    "EPU": "https://www.policyuncertainty.com/media/All_Daily_Policy_Data.csv",
    "GPR": "https://www.matteoiacoviello.com/gpr_files/data_gpr_daily_recent.xls",
}


def align(index_series: pd.Series, bench: pd.Series) -> pd.DataFrame:
    joined = pd.concat({"idx": index_series, "bench": bench}, axis=1).dropna()
    return joined


def corr_and_leadlag(joined: pd.DataFrame, max_lag: int = 10) -> dict:
    diffs = joined.diff().dropna()
    leadlag = {
        lag: float(diffs["idx"].corr(diffs["bench"].shift(-lag)))
        for lag in range(-max_lag, max_lag + 1)
    }
    return {
        "level_corr": float(joined["idx"].corr(joined["bench"])),
        "diff_corr": float(diffs["idx"].corr(diffs["bench"])),
        "leadlag": leadlag,
    }


def _download() -> dict[str, pd.Series]:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(timeout=60, follow_redirects=True)
    out = {}

    raw = BENCH_DIR / "vix.csv"
    raw.write_bytes(client.get(SOURCES["VIX"]).content)
    vix = pd.read_csv(raw, na_values=".")
    vix.columns = ["date", "vix"]
    out["VIX"] = vix.assign(date=pd.to_datetime(vix["date"])).set_index("date")["vix"]

    raw = BENCH_DIR / "epu.csv"
    raw.write_bytes(client.get(SOURCES["EPU"]).content)
    epu = pd.read_csv(raw)
    epu["date"] = pd.to_datetime(epu[["year", "month", "day"]])
    out["EPU"] = epu.set_index("date")["daily_policy_index"]

    raw = BENCH_DIR / "gpr.xls"
    raw.write_bytes(client.get(SOURCES["GPR"]).content)
    gpr = pd.read_excel(raw)
    gpr.columns = [c.lower() for c in gpr.columns]
    out["GPR"] = gpr.assign(date=pd.to_datetime(gpr["date"])).set_index("date")["gprd"]
    return out


def main() -> None:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
    indices = pd.read_parquet(config.DATA_DIR / "indices" / "indices.parquet")
    ours = (indices[(indices["index"] == "GLOBAL") &
                    (indices["gauge"] == "turbulence")]
            .set_index("date")["value"].dropna())

    results = {}
    for name, bench in _download().items():
        joined = align(ours, bench)
        results[name] = corr_and_leadlag(joined)
        fig, ax1 = plt.subplots(figsize=(11, 4.5))
        ax1.plot(joined.index, joined["idx"], label="Global Uncertainty (0-100)")
        ax2 = ax1.twinx()
        ax2.plot(joined.index, joined["bench"], alpha=0.6, color="tab:orange",
                 label=name)
        ax1.set_title(f"Global Uncertainty Index vs {name} "
                      f"(level corr {results[name]['level_corr']:.2f})")
        fig.legend(loc="upper left")
        fig.tight_layout()
        fig.savefig(CHART_DIR / f"benchmark_{name.lower()}.png", dpi=150)
        plt.close(fig)

    (BENCH_DIR / "comparison.json").write_text(json.dumps(results, indent=2))
    for name, r in results.items():
        best = max(r["leadlag"], key=r["leadlag"].get)
        print(f"{name}: level={r['level_corr']:.2f} diff={r['diff_corr']:.2f} "
              f"best lag={best:+d} (negative = we lead)")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests, then run the CLI**

Run: `.venv/bin/pytest tests/test_benchmarks.py -q` — expected `3 passed`.
Run: `.venv/bin/python -m uindex.validate.benchmarks` — expect three charts in `docs/validation/` and printed correlations. If a benchmark URL has changed shape, fix the parser in `_download` (the raw file is saved for inspection).

- [ ] **Step 5: Commit**

```bash
git add src/uindex/validate tests/test_benchmarks.py docs/validation
git commit -m "feat: benchmark comparison vs VIX, EPU, GPR with lead-lag analysis"
```

---

### Task 11: Event study, robustness, churn audit, validation report

**Files:**
- Create: `uncertainty-index/src/uindex/validate/events.py`
- Create: `uncertainty-index/src/uindex/validate/robustness.py`
- Create: `uncertainty-index/src/uindex/validate/report.py`
- Test: `uncertainty-index/tests/test_events.py`

**Interfaces:**
- Consumes: `data/indices/indices.parquet`, `data/normalized/` (robustness recomputes), `data/indices/constituents.parquet`, `data/benchmarks/comparison.json`.
- Produces:
  - `events.EVENTS`: list of `{"name", "start", "end", "indexes"}` dicts.
  - `events.check_events(indices: pd.DataFrame) -> pd.DataFrame` — one row per (event, index): `event, index, window_max, passed` (passed = window max scaled turbulence ≥ 90).
  - `events.top_spike_days(indices: pd.DataFrame, n: int = 10) -> pd.DataFrame` — largest single-day GLOBAL turbulence values with dates.
  - `robustness.run(perturbations) -> pd.DataFrame` — GLOBAL turbulence recomputed per param variant; reports min pairwise correlation.
  - `report.main()` — assembles `docs/validation/report.md` embedding all results and charts.

- [ ] **Step 1: Write the failing tests**

`tests/test_events.py`:

```python
import pandas as pd

from uindex.validate import events


def _fake_indices():
    dates = pd.date_range("2024-06-01", periods=200, freq="D")
    vals = pd.Series(30.0, index=dates)
    vals["2024-11-04":"2024-11-08"] = 97.0
    return pd.DataFrame({
        "date": dates, "index": "GLOBAL", "gauge": "turbulence",
        "raw": 0.1, "value": vals.values,
    })


def test_event_passes_when_window_spikes():
    df = _fake_indices()
    res = events.check_events(df, event_list=[{
        "name": "US election week", "start": "2024-11-04",
        "end": "2024-11-08", "indexes": ["GLOBAL"],
    }])
    assert len(res) == 1
    assert bool(res.iloc[0]["passed"]) and res.iloc[0]["window_max"] == 97.0


def test_event_fails_when_quiet():
    df = _fake_indices()
    res = events.check_events(df, event_list=[{
        "name": "quiet window", "start": "2024-07-01",
        "end": "2024-07-05", "indexes": ["GLOBAL"],
    }])
    assert not bool(res.iloc[0]["passed"])


def test_top_spike_days_sorted():
    df = _fake_indices()
    top = events.top_spike_days(df, n=3)
    assert len(top) == 3
    assert top["value"].is_monotonic_decreasing
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_events.py -q`
Expected: FAIL — module not found.

- [ ] **Step 3: Write the implementations**

`src/uindex/validate/events.py`:

```python
"""Event study: known chaos windows must register as spikes."""
import pandas as pd

PASS_THRESHOLD = 90.0

EVENTS = [
    {"name": "US election week", "start": "2024-11-04", "end": "2024-11-08",
     "indexes": ["GLOBAL", "ELECTIONS"]},
    {"name": "Liberation Day tariffs", "start": "2025-04-02", "end": "2025-04-09",
     "indexes": ["GLOBAL", "ECON_FED"]},
    {"name": "Israel-Iran war / US strikes", "start": "2025-06-13",
     "end": "2025-06-23", "indexes": ["GLOBAL", "WAR"]},
]


def check_events(indices: pd.DataFrame,
                 event_list: list[dict] | None = None) -> pd.DataFrame:
    turb = indices[indices["gauge"] == "turbulence"]
    rows = []
    for ev in (event_list if event_list is not None else EVENTS):
        for idx in ev["indexes"]:
            window = turb[(turb["index"] == idx) &
                          (turb["date"] >= ev["start"]) &
                          (turb["date"] <= ev["end"])]["value"]
            wmax = float(window.max()) if len(window) else float("nan")
            rows.append({"event": ev["name"], "index": idx,
                         "window_max": wmax,
                         "passed": bool(wmax >= PASS_THRESHOLD)})
    return pd.DataFrame(rows)


def top_spike_days(indices: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    glob = indices[(indices["index"] == "GLOBAL") &
                   (indices["gauge"] == "turbulence")].dropna(subset=["value"])
    return (glob.nlargest(n, "value")[["date", "value"]]
            .reset_index(drop=True))
```

`src/uindex/validate/robustness.py`:

```python
"""Recompute GLOBAL turbulence under +/-20% param perturbations."""
import itertools

import pandas as pd

from .. import config, pipeline, universe

PERTURBATIONS = {
    "ewma_halflife": [8, 10, 12],
    "resolution_exclusion_days": [2, 3, 4],
    "pm_min_total_volume": [40_000, 50_000, 60_000],
    "ka_min_rolling_notional": [4_000, 5_000, 6_000],
}


def run() -> pd.DataFrame:
    meta = pd.read_parquet(config.DATA_DIR / "normalized" / "meta.parquet")
    panel = pd.read_parquet(config.DATA_DIR / "normalized" / "panel.parquet")

    variants = {}
    base_flagged = None
    for name, values in PERTURBATIONS.items():
        for v in values:
            uni_params = {name: v} if name != "ewma_halflife" else None
            pipe_params = {"ewma_halflife": v} if name == "ewma_halflife" else None
            flagged = universe.apply_pit_rules(meta, panel, params=uni_params)
            out = pipeline.compute_indices(flagged, params=pipe_params)
            series = out[(out["index"] == "GLOBAL") &
                         (out["gauge"] == "turbulence")].set_index("date")["value"]
            variants[f"{name}={v}"] = series
    # equal-weight variant (weighting-scheme sensitivity)
    flagged = universe.apply_pit_rules(meta, panel)
    flagged["weight"] = 1.0
    out = pipeline.compute_indices(flagged)
    variants["equal_weight"] = out[(out["index"] == "GLOBAL") &
                                   (out["gauge"] == "turbulence")
                                   ].set_index("date")["value"]

    wide = pd.DataFrame(variants).dropna()
    corr = wide.corr()
    pairs = [(a, b, corr.loc[a, b])
             for a, b in itertools.combinations(wide.columns, 2)]
    summary = pd.DataFrame(pairs, columns=["variant_a", "variant_b", "corr"])
    print(f"min pairwise correlation: {summary['corr'].min():.3f} "
          f"(target >= 0.90)")
    return summary


if __name__ == "__main__":
    run().to_csv(config.DATA_DIR / "indices" / "robustness.csv", index=False)
```

`src/uindex/validate/report.py`:

```python
"""Assemble the Phase 1 validation report."""
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from .. import config
from . import events

OUT = config.PROJECT_ROOT / "docs" / "validation"


def _index_chart(indices: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 5))
    for name in config.INDEXES:
        sub = indices[(indices["index"] == name) &
                      (indices["gauge"] == "turbulence")].dropna(subset=["value"])
        lw = 2.5 if name == "GLOBAL" else 0.9
        ax.plot(sub["date"], sub["value"], label=name, linewidth=lw)
    ax.set_title("Turbulence indices, 0-100 percentile scale")
    ax.legend(ncol=4, fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT / "all_indices.png", dpi=150)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    indices = pd.read_parquet(config.DATA_DIR / "indices" / "indices.parquet")
    constituents = pd.read_parquet(config.DATA_DIR / "indices" /
                                   "constituents.parquet")
    _index_chart(indices)

    ev = events.check_events(indices)
    spikes = events.top_spike_days(indices)
    bench = json.loads((config.DATA_DIR / "benchmarks" /
                        "comparison.json").read_text())
    robust_path = config.DATA_DIR / "indices" / "robustness.csv"
    robust = pd.read_csv(robust_path) if robust_path.exists() else None

    # churn audit: index moves must not track membership moves
    glob = indices[(indices["index"] == "GLOBAL") &
                   (indices["gauge"] == "turbulence")].set_index("date")["value"]
    n = constituents[constituents["index"] == "GLOBAL"
                     ].set_index("date")["n_constituents"]
    churn_corr = float(glob.diff().corr(n.diff()))

    lines = [
        "# Uncertainty Index — Phase 1 Validation Report", "",
        f"_Generated {pd.Timestamp.now():%Y-%m-%d}_", "",
        "![All indices](all_indices.png)", "",
        "## Event study (pass = window max >= 90)", "",
        ev.to_markdown(index=False), "",
        "## Top 10 spike days (GLOBAL turbulence)",
        "", "Annotate each date with the driving news story before publishing:",
        "", spikes.to_markdown(index=False), "",
        "## Benchmark comparison", "",
    ]
    for name, r in bench.items():
        best = max(r["leadlag"], key=lambda k: r["leadlag"][k])
        lines.append(f"- **{name}**: level corr {r['level_corr']:.2f}, "
                     f"diff corr {r['diff_corr']:.2f}, best lag {best} "
                     f"(negative = we lead). ![chart](benchmark_{name.lower()}.png)")
    lines += ["", "## Robustness", ""]
    if robust is not None:
        lines.append(f"Min pairwise correlation across param variants: "
                     f"**{robust['corr'].min():.3f}** (target >= 0.90).")
    lines += ["", "## Churn audit", "",
              f"Corr(Δindex, Δconstituent-count) = **{churn_corr:.3f}** "
              f"(want |value| small; large means membership churn, not news, "
              f"moves the index)."]

    (OUT / "report.md").write_text("\n".join(lines))
    print(f"report written to {OUT / 'report.md'}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests, then generate the report**

Run: `.venv/bin/pytest tests/test_events.py -q` — expected `3 passed`.
Run (in order; robustness recomputes the pipeline ~13 times, expect minutes):

```bash
.venv/bin/python -m uindex.validate.robustness
.venv/bin/python -m uindex.validate.report
```

Read `docs/validation/report.md` end to end. **Green-light gate (from spec):** all event-study rows pass, robustness min corr ≥ 0.90, churn corr near zero, benchmark corrs sensible. Any failure → return to methodology (config params, universe rules), do not massage the report.

- [ ] **Step 5: Commit**

```bash
git add src/uindex/validate tests/test_events.py docs/validation
git commit -m "feat: event study, robustness, churn audit, validation report"
```

---

### Task 12: Methodology document

**Files:**
- Create: `uncertainty-index/docs/methodology.md`

**Interfaces:**
- Consumes: final `config.py` values, validation report results, spec.
- Produces: the public-facing methodology doc — Phase 2's website methodology page and the project's IP.

- [ ] **Step 1: Write the document**

Sections (write from the *final* state of config and the validation results, not from the spec's initial guesses):

1. **Overview** — what the indices measure, the two gauges, the 0–100 scale interpretation.
2. **Data** — venues, backfill window, ingestion cadence.
3. **Universe rules** — taxonomy, PIT eligibility, floors, strike groups, dedup, sports exclusion. Exact parameter values.
4. **Computation** — logit turbulence and entropy formulas (LaTeX), EWMA half-life, percentile scaling, seed period.
5. **Known limitations** — static Polymarket volume weights (no historical daily volume), lifetime-volume strike representatives, keyword taxonomy imprecision, percentile-scale regime dependence. Every accepted PIT deviation goes here.
6. **Validation summary** — event-study table, benchmark correlations, robustness result, with links to the full report.

- [ ] **Step 2: Verify consistency**

Check every parameter value quoted in the doc against `src/uindex/config.py` (they drift during Task 9 taxonomy refinement). Check formulas against `compute.py`.

- [ ] **Step 3: Commit**

```bash
git add docs/methodology.md
git commit -m "docs: index methodology document"
```

---

## Self-Review (completed at write time)

- **Spec coverage:** decisions table → Tasks 1 (config/taxonomy, sports drop), 3 (A+B math), 4–5 (both venues), 6 (normalization/triage), 7 (PIT rules incl. settlement guard, floors, strikes, dedup), 8 (universe-agnostic engine, golden-day + reproducibility), 9 (backfill), 10 (benchmarks), 11 (event study, robustness, churn), 12 (methodology doc). Spec risks → Task 2 (feasibility probe, top risk) and Task 12 §5 (limitations).
- **Placeholder scan:** none — all code complete; Task 2/9/12 are operational/document tasks with concrete steps and gates.
- **Type consistency:** `apply_pit_rules` output columns (`eligible`, `weight`, `category`, `close_prob`, `date`, `market_id`) match `compute_indices` consumption; `compute.py` signatures match `pipeline.py` calls; tidy output columns (`date,index,gauge,raw,value`) match all Task 10–11 consumers.
- **Known planned deviation:** Tasks 4–5 API field names are corrected, if needed, at the Task 2 checkpoint before those tasks execute.
