# Task 2 — API Feasibility Probe Findings

## Polymarket (Gamma + CLOB) — OK, backfill-ready

- Gamma `/markets` field names match the plan's assumptions: `question`,
  `category`, `volumeNum`, `startDate`/`endDate`, `clobTokenIds` (JSON-encoded
  string) all present as coded in `ingest/polymarket.py`.
- Offset pagination (`offset=N`) works and returns distinct pages.
- CLOB `/prices-history` with `interval=max&fidelity=1440` returns real daily
  closes: sample market `pm_prices.json` has **396 daily points from
  2023-12-03 to 2025-01-01** — full pre-2024 lead-in through 2024, no gaps
  detected. `history` payload shape (`{"history": [{"t": ..., "p": ...}]}`)
  matches `history_to_df`.
- No historical *daily volume* field found on the Gamma market object — only
  lifetime `volumeNum` (and `volume1mo/1wk/1yr` rolling windows, not daily
  series). **Confirms the static-weight fallback in Task 7 stands** (Polymarket
  weight = `ln(1 + total_volume_usd)`, not a time-varying notional).
- Rate-limit sniff: 10 rapid `/prices-history` calls all returned 200, no
  429s observed in this small sample.
- **No code changes needed** in Task 4 — field names, pagination, and history
  shape all match the plan as written.

## Kalshi (trade-api v2) — BLOCKED on schema; backfill window is not viable as specced

### Schema mismatches (fixable)

The actual `/markets` market object does **not** match the fields
`kalshi.py` (Task 5) assumes:

| Plan assumes | Actual field | Note |
|---|---|---|
| `category` | *(absent)* | No category field on the market object at all. Category lives on the **event**, not the market — `markets_to_df` needs an event lookup or must accept `venue_category=""` and fall back entirely on keyword categorization. |
| `volume` | `volume_fp` | String, e.g. `"0.00"`. Also `volume_24h_fp`, `open_interest_fp`. |
| `series_ticker` on market | *(usually absent)* | Confirmed nullable/missing on the market object per the probe; `event_ticker.split("-")[0]` fallback is the only reliable path, but doesn't always equal the real series (e.g. `RHGOLD-24-Q1` → naive split gives `"RHGOLD"`, and the real series ticker is `KXRHGOLD` — the candlestick endpoint requires the `KX`-prefixed series or it silently returns an empty list; see below). |

These are mechanical fixes and don't block Task 5's code — they get applied
when Task 5 is implemented.

### The real blocker: no historical trade/price data for 2024 markets

- Sampled **561 Kalshi markets closing in calendar-year 2024** across 20
  events (WTI, forex, RHGOLD, ECB rate, NASDAQ100, INX, SpaceX Starship,
  Billboard charts) — **100% have `volume_fp = 0.00`**, empty
  `/candlesticks` (`kalshi_candles.json` → `candlesticks: []`), and the
  public `/markets/trades` endpoint also returns an empty trade list for the
  same tickers. This is not one illiquid market — it's every 2024 market
  sampled, across every asset class tried.
- Candlestick lookups require the `KX`-prefixed series ticker
  (`KXRHGOLD`, not `RHGOLD`) — using the correct prefix still returns zero
  candles for the 2024 date range, so this isn't a series-ticker bug.
- By contrast, **current/live series** (`KXHIGHNY`, `KXFED`) return dense,
  well-populated hourly/daily candlesticks with real volume (thousands of
  contracts) going back to roughly **May 2026** — i.e., Kalshi's public API
  appears to only carry meaningful trade history for markets from the last
  ~2 months, not the full lifetime archive.
- `min_close_ts`/`max_close_ts` filtering does surface the 2024 market
  *metadata* (ticker, strikes, rules, result) — the catalog entries exist —
  but no trading activity is exposed for them via any endpoint tried
  (`/markets`, `/candlesticks`, `/markets/trades`).

**This directly trips the plan's Task 2 checkpoint gate:** *"If history
depth is insufficient for Jan 2024 backfill, stop and surface to Kian — the
backfill window is a spec decision."* Kalshi cannot support a 2024-01-01
backfill through `api.elections.kalshi.com` as currently queried. Surfaced
to Kian for a decision before Tasks 4–5 proceed on the originally-specced
window for Kalshi.

## Rate limits

Both venues: no 429s in a 10-rapid-call sniff at the `SLEEP_S` intervals
already coded (0.3s Polymarket, 0.15s Kalshi). No adjustment indicated by
this sample size, but the real backfill (Task 9) will exercise this far
harder.
