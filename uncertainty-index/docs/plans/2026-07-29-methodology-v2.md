# Methodology v2 — PIT Weights, Causal Pin Rule, Validation Overhaul

**Approved 2026-07-29** after the three-agent quant/code review. Fixes the
point-in-time violations and validation weaknesses; see the review findings
referenced in git history (`fb6b150`) and
`docs/research/polymarket-pit-volume-probe.md`.

Pre-staged (done): config constants (`POLYMARKET_MIN_ROLLING_NOTIONAL_USD`,
`PIN_CONSECUTIVE_DAYS`, `GOLDSKY_*`), clip bounds parameterized through
`compute.logit`/`binary_entropy` and `compute_indices` params
(`clip_lo`/`clip_hi`).

## Track A — Polymarket PIT daily volume (ingest layer)

- New `src/uindex/ingest/polymarket_volume.py`: portioned, checkpointed
  global sweep of Goldsky `enrichedOrderFilleds` ordered by (timestamp, id),
  cursor persisted like MetaStore's; buckets fills to (token_id, date,
  notional_usd) locally; parts + streaming merge via the existing store
  helpers; exit protocol 0/3 so `run_backfill.sh polymarket_volume` works.
- Verify USDC scaling empirically (reconcile a small market against Goldsky
  `orderbook.scaledCollateralVolume` and Gamma lifetime volume; the probe
  saw 96% agreement).
- Token→market mapping: mini-probe Goldsky for a token→condition entity; if
  absent, enrichment pass fetching `clobTokenIds` from Gamma for kept
  markets only (~tens of k, checkpointed). Output
  `data/raw/polymarket/volumes.parquet`: market_id, date,
  daily_notional_usd (both tokens summed).
- `normalize.build_panel` merges volumes into PM price rows'
  `daily_notional_usd` (missing days = 0.0 for active markets, since no
  fills genuinely means zero notional).

## Track B — Universe v2 (PIT rules)

- PM floor/weight: drop lifetime-volume rule; both venues use trailing
  `ROLLING_WINDOW_DAYS`-mean daily notional — floor per venue constant,
  weight `log1p(rolling)` uniformly. `pm_min_total_volume` param becomes
  `pm_min_rolling_notional`.
- Kalshi strike rep: per-day representative = strike with max trailing
  rolling notional that day (ties → market_id), replacing lifetime choice.
- Pin rule: replace `_terminal_pin` (anticausal) with causal rule — a
  market is excluded on day t iff its last `PIN_CONSECUTIVE_DAYS` observed
  closes (t inclusive, ignoring gap days) are all outside [pin_lo, pin_hi].
  Collapse day stays in (genuine repricing); settled flatline leaves after
  K days; a bounce re-admits. Update the pin tests to the causal contract.
- Cross-venue dedup keeper: earlier `open_date` wins (ties → market_id) —
  PIT-safe, replacing lifetime-volume choice.

## Track C — Validation overhaul (validate layer)

- `events.py`: placebo calibration — for each event window, p-value =
  share of equal-length non-event windows whose max ≥ the event window's
  max; report per-event p and pass at p ≤ 0.10 alongside the raw max.
- `benchmarks.py`: diff each series on its own calendar before aligning
  (kills the weekend-fold artifact); add an approximate 2/sqrt(n) noise
  band per lag and only claim a lead when best-lag corr exceeds lag-0 corr
  by that band; keep `best_lag`.
- New `validate/churn.py`: daily decomposition of Δ(GLOBAL turbulence raw)
  into repricing (t vs t-1 on the common membership) and membership terms,
  from the flagged panel + compute primitives; report membership share of
  total |Δ|. `report.py` uses it instead of the count correlation.
- `robustness.py`: add `clip` perturbation [(0.005,0.995), (0.02,0.98)]
  via the new `clip_lo`/`clip_hi` params; replace `pm_min_total_volume`
  perturbation with `pm_min_rolling_notional`; run the event-study check
  per variant and report which variants pass.

## Acceptance

Full suite green; each track adds tests for its new behavior; no track
edits `config.py`, `compute.py`, or another track's files; nothing
committed by implementers (integration commit at the end). Backfill keeps
running throughout — Track A's sweep is additive and joins at normalize
time.
