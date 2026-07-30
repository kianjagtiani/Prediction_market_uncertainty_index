# Progress — Uncertainty Index

_Last updated: 2026-07-30 (paused for exam; all state committed & pushed)._

## Where things stand

Phase 1 code is complete through **methodology v2** (commit `0362ca8`),
107 tests green. The historical backfill is **paused mid-crawl** with a
saved checkpoint. Two follow-up checks were killed mid-run and need a
re-run. Phase 2 (public site) is spec'd and approved but its
implementation plan is not yet written.

### Resume commands

```bash
cd uncertainty-index
# backfill (resumes from checkpoint: 1,825,500 PM markets seen):
caffeinate -i scripts/run_backfill.sh polymarket && \
caffeinate -i scripts/run_backfill.sh kalshi
# after PM metadata+prices: scripts/run_backfill.sh polymarket_volume
# then: normalize -> pipeline -> validate modules (Task 9 in docs/plans/)
```

Also to re-run: (a) the negRisk coverage probe, (b) the adversarial
review of commit `0362ca8` — both described under "Open items".

## Timeline of this build

1. **Repo published** to `github.com/kianjagtiani/Prediction_market_uncertainty_index`
   (SSH push; PAT lacks repo perms). README written; `main` fast-forwarded.
   Note: repo is currently **public** — decide before any sensitive content.
2. **Portioned, checkpointed ingestion** (`37e11da`): both prior backfill
   attempts died holding the full catalog (1M+ PM / 10M+ Kalshi rows) in
   RAM on the 8 GB machine. Now: metadata streams to parquet shards with a
   committed API cursor; every invocation does a bounded portion and exits
   (exit 3 = more work); `scripts/run_backfill.sh` loops fresh processes
   under a 2 GB RSS watchdog; provably-below-floor rows dropped at fetch
   (10x slack kept for robustness sweeps); merges stream shard-at-a-time.
3. **Three-agent code review** (quant-analyst, python-pro, data-engineer)
   → engineering fixes (`fb6b150`), methodology decisions approved by Kian
   → **methodology v2** implemented by three parallel agents (`0362ca8`).
4. **Research**: VIX adoption history (docs/research/vix-adoption-history.md
   — EPU playbook is our v1 path); Polymarket PIT volume feasibility
   (docs/research/polymarket-pit-volume-probe.md — Goldsky subgraph works).
5. **Phase 2 decided** (docs/specs/2026-07-29-phase2-site-design.md):
   static-first data product — daily artifact + thin site renderers,
   CDN-served JSON as the API, git-vintaged data repo. Name/domain TBD.

## Review findings → fixes

### Engineering (fixed in `fb6b150`)

| Finding (agent) | Fix |
|---|---|
| 429/5xx permanently tombstoned markets as "no data" — silent data loss (data-eng F1, python-pro #1) | Only definitive 4xx tombstones; 429/5xx crash the portion, driver retries |
| Part-file writes not atomic; kill mid-write wedges pipeline on corrupt shard (F2, #2) | tmp + os.replace everywhere (`_write_part`) |
| Watchdog kill (143) counted as failure → 3 kills abort backfill (F3) | 143 treated as progress (checkpoints make kills lossless) |
| No lockfile/trap: orphaned child, double-writer races (F4, #6) | Lockfile + trap kill in driver |
| Within-file duplicate rows survive merge (F6) | Per-file `drop_duplicates(key)` in `_stream_merge` |
| Int64 close_prob shard can wedge schema-locked merge (F7) | close_prob pinned float at source |
| Stale cursor after crashed finalize truncates re-crawl (F9) | State unlinked before parts in finalize |
| `compute_indices` crashes on empty universe (#4) | Clear ValueError |
| Function-attribute side channel for constituents (#5) | Returns `(indices, constituents)` |
| NaN question crashes categorize (#7) | isinstance guard |
| ~90 duplicated crawl-loop lines (#11) | Shared `crawl()` engine in store.py |
| Per-date Python aggregation loop (#14) | Vectorized `_weighted_rows` (suite 5.3s → 1.7s) |
| Robustness dead code + 4 redundant baseline runs (#12) | Single baseline, reused flagged panel |

### Methodology (approved by Kian; implemented in `0362ca8`)

| Finding (quant-analyst) | Fix |
|---|---|
| **#1 CRITICAL: PM floor/weight used crawl-time lifetime volume — look-ahead + survivorship in every published day** | True PIT daily volume from Goldsky orderbook subgraph (new `ingest/polymarket_volume.py`; scaling live-verified: notional = size/1e6, exact reconciliation vs `collateralVolume`); both venues now gate & weight on trailing rolling notional, `log1p(rolling)` |
| #2 Strike rep chosen by lifetime volume | Per-day representative by trailing rolling notional (ties → market_id) |
| #3 Terminal-pin guard anticausal: inflated unresolvedness, deleted genuine collapse days (election night!) | Causal rule: excluded only after PIN_CONSECUTIVE_DAYS=5 observed pinned closes; collapse day stays; bounce re-admits; truncation invariance proven by test |
| #4 Event study pass (max ≥ 90) near-vacuous | Placebo calibration: p = share of non-event windows with max ≥ observed; pass at p ≤ 0.10 |
| #5 Lead-lag argmax over 21 lags, no inference; weekend-fold in diffs | Own-calendar diffs; 2/√n noise band; "leads" only if best-lag beats lag-0 by the band |
| #6 Churn audit tested count correlation (wrong quantity) | `validate/churn.py`: Δraw decomposed into repricing (common membership) + membership terms; report membership share of Σ|Δ| (guide ≤ 0.20) |
| #7 Dedup keeper by lifetime volume | Earlier open_date wins (PIT-safe); fuzzy title matching still TODO (minor) |
| #8 Clip perturbation missing; robustness never re-ran event study | Clip variants added; every variant re-checked against the placebo event study |
| #9/#10/#11 (minor: EWMA per-observation halflife, row-based rolling window, n_constituents conflation) | **Not yet addressed** — documented candidates for the methodology doc's limitations section |

## Open items (in order)

1. **negRisk coverage probe (CRITICAL, blocks the volume sweep).** Track A
   found markets with large Gamma volume but ~zero subgraph volume
   (pm_559700: $0 vs $85k). Hypothesis: NegRiskCtfExchange fills (most
   election markets!) aren't indexed by `polymarket-orderbook-resync`.
   Probe was killed mid-run. Must confirm + find the sibling subgraph
   before running `polymarket_volume`, else election markets get zero
   weight. Also check whether Gamma `volumeClob` reconciles for
   non-negRisk markets (explains pm_544097's $5.8k vs $95k as AMM legacy).
2. **Adversarial review of `0362ca8`** — killed mid-run; re-dispatch. Focus
   seams: churn.py's mirror of the index math, volume-sweep cursor
   ordering (string id lexicographic?), normalize fillna(0) on a partial
   volumes.parquet (no "sweep complete" guard yet), strike-rep behavior
   when all strikes ineligible, token_map's 60s polling loop vs the
   driver's exit protocol.
3. **Backfill**: resume (commands above). PM metadata ~1.8M+ seen and
   still paging; then PM prices, PM volume sweep, Kalshi, normalize,
   pipeline, validation report (Task 9/10/11 of the Phase 1 plan).
4. **Phase 2 implementation plan** (spec approved; write after 1–2 settle
   the incremental-update design, incl. no-data-ledger invalidation and
   full-history-refetch rule from data-eng F5).
5. **Methodology doc** (Task 12) — must document v2 rules + limitations
   (#9/#10/#11 above) and the negRisk resolution.
6. Minor backlog: fuzzy dedup, test-isolation nit (universe tests read the
   real overrides CSV), `.venv` has no ruff.

## Key references

- Phase 1 plan: docs/plans/2026-07-12-uncertainty-index-phase1.md
- Methodology v2 plan: docs/plans/2026-07-29-methodology-v2.md
- Phase 2 spec: docs/specs/2026-07-29-phase2-site-design.md
- Probes: docs/research/polymarket-pit-volume-probe.md, docs/probe-findings.md
- VIX/EPU adoption playbook: docs/research/vix-adoption-history.md
