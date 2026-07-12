# Uncertainty Index — Design Spec

**Date:** 2026-07-12
**Status:** Approved pending final review
**Scope:** Phase 1 — methodology engine + historical backfill + validation. No public product yet.

## Goal

Build a family of "VIX for the news" indices computed from prediction-market prices: a headline **Global Uncertainty Index** plus themed sub-indices, each measuring how violently the world's probabilities are being repriced (Turbulence) and how undecided the big open questions are (Unresolvedness). Phase 1 delivers a validated methodology and backfilled daily series from Jan 2024 to present; a live dashboard and custom index builder come only after the numbers are proven.

## Decisions (locked)

| Decision | Choice |
|---|---|
| Headline methodology | **A + B**: logit-space realized volatility (Turbulence) as the headline number; entropy (Unresolvedness) as companion gauge |
| Data sources | **Polymarket + Kalshi from day one** |
| Sports markets | **Excluded entirely** — not in the composite, no sports sub-index |
| V1 shape | Methodology + backfill + validation first; dashboard is Phase 2 |
| Backfill window | Jan 2024 → present |
| Engine design | Universe-agnostic: an index = (market universe spec) + (shared A/B computation). Composite, themed, and future custom indices are all universe configs |

## Index family

One computation engine; each index is a universe definition (venue tags + keyword rules → our taxonomy):

| Index | Universe |
|---|---|
| **GLOBAL (composite)** | Union of all themed universes below |
| WAR / Geopolitics | Conflicts, military action, ceasefires, sanctions |
| ELECTIONS | US + international electoral outcomes |
| POLITICS | Governance: shutdowns, legislation, SCOTUS, cabinet |
| ECON / FED | Rate decisions, CPI prints, recession, jobs |
| CRYPTO | BTC/ETH/crypto price and event markets |
| TECH / AI | Model releases, AI milestones, big-tech events |
| CLIMATE | Temperature records, hurricanes, emissions targets |

Sports markets are filtered out at universe construction and never enter any index.

## Methodology

### Turbulence (headline, per universe)

1. Per market per day: closing probability `p_t`, clipped to [0.01, 0.99].
2. Logit transform: `x_t = ln(p_t / (1 - p_t))`. Rationale: equal point-moves near 0/1 carry far more information than near 0.5; logit space scales them correctly.
3. Daily innovations: `r_t = x_t − x_{t−1}`.
4. Per-market realized volatility: EWMA of `r_t²` (window/half-life a tunable parameter, subject to robustness checks).
5. Universe aggregation: liquidity-weighted mean of per-market vols (weight = rolling volume or open interest).
6. Scaling: percentile rank of today's raw value against trailing history → 0–100. A seed period at the start of the backfill is consumed to initialize the percentile window and excluded from published series.

### Unresolvedness (companion, per universe)

Liquidity-weighted mean binary entropy `H(p) = −p·log₂(p) − (1−p)·log₂(1−p)`, same clipping and 0–100 percentile scaling.

### Point-in-time universe rules

A market qualifies on day *t* only using information available on day *t*:

- Active (open, not resolved).
- Liquidity above a floor (tunable; robustness-checked).
- More than N days from scheduled resolution (guards the settlement artifact: probabilities collapsing to 0/1 near resolution create fake turbulence — the single biggest artifact to prevent).
- Deduplicated: near-identical markets within and across venues count once (keyword/entity matching + manual override list).
- Kalshi multi-strike series (e.g., a dozen "CPI above X%" strikes on one event) are grouped as one event; a representative strike or aggregate is used so one event isn't counted twelve times.

Universe churn (markets entering/leaving) must not mechanically jump the index; aggregation is a weighted mean (not a sum), and churn effects are explicitly checked in validation.

## Architecture

Python project in `uncertainty-index/`. Components, each independently testable:

1. **Ingestion** — Polymarket (Gamma API: metadata; CLOB API: price history) and Kalshi (markets + candlesticks APIs). Raw responses stored as parquet (DuckDB for querying). Incremental and resumable; rate-limit aware.
2. **Normalization** — unified schema: `market_id, venue, category_tags, date, close_prob, volume, liquidity, open_date, close_date, resolution_date, resolved_outcome`. Venue categories + keyword rules map to our taxonomy; unmapped markets logged for triage.
3. **Universe construction** — applies PIT rules above; emits per-day constituent lists per index (auditable).
4. **Index computation** — the A/B math; input: constituents + prices; output: daily raw and 0–100 series per index.
5. **Validation harness** — produces the Phase 1 report (below).

Data flow: `ingest → normalize → universes → compute → validate`, each stage reading/writing parquet — any stage re-runnable in isolation.

## Validation plan (Phase 1 deliverable)

1. **Event study** — the backfilled composite and relevant sub-indices must spike on known chaos dates: Nov 2024 election week, 2025 tariff shocks, June 2025 Iran strikes, plus 2026 events identified during the build. Failure → methodology revision, not narrative adjustment.
2. **Benchmark comparison** — correlation and lead–lag vs VIX (FRED: VIXCLS), Baker-Bloom-Davis Economic Policy Uncertainty index, Caldara-Iacoviello Geopolitical Risk index. Target: meaningfully correlated (credibility) but not duplicative (reason to exist). Any lead over VIX is the flagship chart.
3. **Robustness** — ±20% perturbation of liquidity floor, EWMA half-life, clipping bounds, resolution-exclusion window: index shape and event-study conclusions must survive. Parameter-fragile = not a benchmark.
4. **Churn audit** — confirm index moves are driven by repricing, not universe entry/exit.

## Testing

- Unit tests: logit/entropy edge cases (clipping bounds, p→0/1), EWMA correctness, PIT rules (a market must never appear before its open date or after resolution-exclusion kicks in).
- Synthetic golden-day test: fabricated universe with an injected shock day → index must spike on that day and only that day.
- Reproducibility: identical inputs → byte-identical output series.

## Risks & unknowns

- **API history depth (top risk):** rate limits and history gaps for long-dead markets on both venues are unknown. First implementation step is a feasibility probe — pull one known 2024 market's full history from each venue — before building full ingestion.
- **Category mapping quality:** venue tags are messy; keyword rules will need iteration. Mitigated by the unmapped-market triage log.
- **Percentile scaling burn-in:** the 0–100 scale needs trailing history; early-2024 readings are seed data, not publishable.

## Phase 2 (explicitly out of scope for Phase 1)

Live daily computation, public dashboard with shareable charts, per-index pages, custom index builder (user-defined universes), data API. Begin only after the Phase 1 validation report passes.
