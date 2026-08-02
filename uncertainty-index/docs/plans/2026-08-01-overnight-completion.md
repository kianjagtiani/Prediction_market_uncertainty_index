# Overnight completion plan — Phase 1 close-out + Phase 2 plan

Goal: finish every open item in PROGRESS.md autonomously. Repo:
`uncertainty-index/`, branch `phase1-uncertainty-index`, base commit `1ccdb74`.

## Global Constraints

- Python 3.11, run tests with `PYTHONPATH=src .venv/bin/python -m pytest tests/ -q`.
  All 109 tests must stay green after every code task.
- 8 GB RAM machine: never load full catalogs into memory; all ingestion goes
  through the portioned/checkpointed protocol (exit 3 = more work, exit 0 =
  done) driven by `scripts/run_backfill.sh` under its 2 GB RSS watchdog.
- All data writes atomic (tmp + `os.replace`); crawl state must be cursor-
  resumable; a kill at any point must be lossless.
- Point-in-time discipline is non-negotiable: no crawl-time/lifetime
  quantities may influence any published day; eligibility and weights use
  trailing rolling notional only (methodology v2, commit `0362ca8`).
- Commit granularity: one logical change per commit, message style matches
  `git log` (prefix `feat:`/`fix:`/`docs:`/`test:`).
- Network calls to Gamma/Goldsky/Kalshi: respect existing throttles in
  `src/uindex/ingest/`; Goldsky batch size ≤ 500 rows (larger batches time out).

### Task 1: negRisk coverage probe (investigation)

Confirm or refute: fills routed through NegRiskCtfExchange are missing from
the `polymarket-orderbook-resync` Goldsky subgraph that
`src/uindex/ingest/polymarket_volume.py` sweeps, which would zero-weight most
election markets.

Evidence so far (docs/research/polymarket-pit-volume-probe.md + Track A):

- pm_559700 (negRisk): Gamma lifetime volume ≈ $85k, subgraph volume $0.
- pm_544097 (non-negRisk, old): $5.8k subgraph vs $95k Gamma — hypothesized
  AMM-era legacy volume predating the CLOB.
- Belgium PM market reconciled within 4% (validates method for indexed markets).
- Known scaling: notional = size / 1e6; reconciles exactly vs `collateralVolume`.

Required outputs (write to `docs/research/negrisk-coverage-probe.md`):

1. Verdict on the hypothesis with query evidence (market ids, token ids,
   fill counts, dollar totals) for ≥ 3 negRisk and ≥ 3 non-negRisk markets.
2. The fix path: identify the sibling/companion Goldsky subgraph (or entity
   within the same subgraph) that indexes NegRiskCtfExchange fills; verify
   its scaling reconciles against Gamma volumes for ≥ 2 negRisk markets.
   If no public subgraph covers negRisk fills, say so explicitly and rank
   the fallback options (Gamma daily volume timeseries? Dune? on-chain logs
   via public RPC?) by PIT-correctness and effort.
3. For non-negRisk markets: does Gamma `volumeClob` (vs `volume`) reconcile
   with the subgraph? Resolves whether pm_544097's gap is AMM legacy.
4. A concrete spec for Task 4: endpoint URL(s), GraphQL query shape, entity
   names/fields, scaling constant, pagination params, and how to merge with
   the existing sweep (same buckets? union of two sweeps?).

Constraints: read-only investigation — no changes to `src/`. Use `curl`
against public endpoints; batch ≤ 500 rows; keep each query < 30 s.

### Task 2: Adversarial review of methodology v2 (commit 0362ca8)

Whole-diff adversarial review of `0362ca8` (plus its interaction with HEAD
fixes `1ccdb74`) — the original review was killed mid-run. Known focus seams:

- `src/uindex/validate/churn.py` re-implements the index math — does its
  mirror diverge from `compute.py`/`pipeline.py` under any input?
- Volume-sweep cursor ordering in `polymarket_volume.py` — string market ids
  compared lexicographically? Does resume skip or repeat buckets?
- `normalize.py` `fillna(0)` on a partial `volumes.parquet` — a half-finished
  sweep silently zero-weights everything not yet swept (no "sweep complete"
  guard). Real defect or guarded upstream?
- `universe.py` strike-representative selection when all strikes of a family
  are ineligible on a day.
- `token_map`'s 60 s polling loop vs the portion driver's exit protocol —
  can a portion spin forever without making progress?
- Anything else: PIT violations, causality leaks, weight/eligibility edge
  cases, resume-safety, dtype/schema drift between modules.

Output: findings report with Critical/Important/Minor severities, each with
file:line, a concrete failure scenario, and a suggested fix. No code changes.

### Task 3: Fix adversarial-review findings

Fix every Critical and Important finding from Task 2 (Minors go to the
ledger). Tests required for each behavioral fix; suite stays green. If a
finding requires data unavailable until the backfill completes, implement
the guard + unit test now (synthetic data).

### Task 4: Polymarket volume source switch + freshness discipline

Task 1's probe (docs/research/negrisk-coverage-probe.md, §5 is the spec —
implement it exactly) found the swept subgraph frozen since 2026-01-05 and
the best replacement (`orderbook-subgraph/0.0.1`, same Goldsky project)
frozen since 2026-04-28 (Polymarket v1→v2 contract migration). Implement in
`src/uindex/ingest/polymarket_volume.py` + config:

1. Point the sweep at `orderbook-subgraph/0.0.1`; entity is
   `orderFilledEvents` (NOT `enrichedOrderFilleds`): derive token/notional
   from the non-"0" of makerAssetId/takerAssetId with the matching
   AmountFilled / 1e6 (spec §5 has the exact rule and query shape). Fresh
   sweep from BACKFILL_START — the old resync cursor must NOT be resumed
   against the new deployment.
2. Before trusting the scaling at scale: do the exact-sum spot-check vs
   `Orderbook.scaledCollateralVolume` for one token (same method as the
   module docstring's 243-fill check); record the result in the docstring.
3. Freshness/coverage manifest: the sweep records the max fill timestamp
   reached; a sweep whose horizon is > STALENESS_DAYS behind now is
   recorded as complete-but-stale, never silently complete. Downstream
   (normalize/universe): days beyond the volume horizon are NaN, never 0;
   rolling notional carries forward causally from the last covered day and
   such days are marked in the flagged panel. (Coordinate with Task 3's C1
   fix — build on whatever guard it landed, don't duplicate.)
4. Unit tests (mocked GraphQL): asset-id scaling rule both directions,
   resume mid-sweep, stale-horizon manifest flagging, carry-forward
   behavior. Suite green.

Out of scope (document as future work in Task 8): on-chain RPC ingestion
for post-2026-04-28 fills and the residual 0%-coverage negRisk markets
(pm_559700-class); Dune. Contract addresses in the probe are unverified.

### Task 5: Backfill execution (operational — controller-run)

Run to completion, sequentially: (a) `scripts/run_backfill.sh polymarket`
(metadata ~268k markets ≈ 30 min, then prices), (b)
`scripts/run_backfill.sh kalshi` (tripwire adjudicates July's suspicious
10.3M count), (c) after Task 4 lands: `scripts/run_backfill.sh
polymarket_volume`. All under `caffeinate -i`. Monitor logs for the
stuck-pagination tripwire, watchdog kills, and 3-strike aborts. Record
final counts (markets, price rows, volume buckets, dates spanned) in the
ledger.

### Task 6: Normalize → pipeline → validation report

With real data on disk: run `normalize` (with volumes), `pipeline`
(compute indices + constituents), then the validation suite
(`validate/report.py`: benchmarks with noise bands, placebo-calibrated
event study, churn decomposition with membership share guide ≤ 0.20,
robustness variants re-checked against the placebo event study).
Output: the validation report artifact under `data/` or `docs/` as the
report module writes it, plus a short summary of pass/fail per check.
This is plan Tasks 9–11 of docs/plans/2026-07-12-uncertainty-index-phase1.md.
If a validation check fails, do NOT tune parameters to pass — record the
failure honestly for the methodology doc and ledger.

### Task 7: Phase 2 implementation plan

Write `docs/plans/2026-08-01-phase2-site.md` from the approved spec
`docs/specs/2026-07-29-phase2-site-design.md`: static-first data product,
daily artifact + thin site renderers, CDN-served JSON as API, git-vintaged
data repo. Must settle the incremental-update design: no-data-ledger
invalidation and the full-history-refetch rule (data-eng finding F5), now
informed by Tasks 1–4 outcomes. Plan format: tasks with acceptance
criteria, same style as existing plans in docs/plans/. No implementation.

### Task 8: Methodology document

Phase 1 Task 12 (docs/plans/2026-07-12-uncertainty-index-phase1.md:2058):
write the public methodology doc covering v2 rules (PIT rolling weights,
causal pin rule with PIN_CONSECUTIVE_DAYS=5, per-day strike reps, placebo-
calibrated event study, churn decomposition), the negRisk volume resolution
(Task 1/4), and a limitations section that includes the deferred findings:
EWMA per-observation halflife, row-based rolling window, n_constituents
conflation, fuzzy dedup TODO. Cite validation results from Task 6.

### Task 9: Minor backlog

(a) test-isolation nit: universe tests read the real overrides CSV — point
them at a fixture; (b) add ruff to `.venv` and fix any findings in changed
files only; (c) fuzzy title dedup remains TODO — document, don't implement.

### Task 11: Legacy Polymarket metadata backfill (pre-keyset range)

Discovered during Task 5: Gamma's `/markets/keyset` endpoint only serves
markets with id ≥ 559651 (created ≥ 2025-07-03) — verified live: its first
page with no cursor starts at id 559651. The completed metadata crawl is
therefore missing the entire 2024-01-01 → 2025-07-03 span of the catalog
(~148k markets by prior estimates), including the 2024 election markets the
event-study validation depends on. The offset `/markets` endpoint 422s past
a few thousand results (documented in polymarket.py), so neither endpoint
alone covers the legacy range.

Implement a legacy sweep in `src/uindex/ingest/polymarket.py` that fills
ids 1..559650:

- Enumerate candidate ids in batches via Gamma `/markets?id=X&id=Y&...`
  (repeated `id` params, ≤ 100 per request; nonexistent ids are simply
  absent from the response). Verify the repeated-id form works with a live
  probe before building on it; if it fails or is capped lower, fall back to
  offset pagination inside end-date windows sized to stay under the 422
  limit, and record which strategy was chosen in the module docstring.
- Same row schema, floor prefilter (BACKFILL_START end-date filter,
  volume/10-slack drop), portioned protocol (exit 3/0), cursor state (a
  simple next-id watermark file distinct from the keyset cursor), atomic
  shard writes, and merge into markets.parquet alongside keyset rows
  (dedup on market_id).
- The driver invocation must remain `scripts/run_backfill.sh polymarket`
  compatible: the module runs the legacy sweep after the keyset sweep
  reports complete (or as its own phase before prices) — keep the exit-code
  protocol intact so the existing driver loops it without changes.
- Unit tests with mocked Gamma responses: batch enumeration, absent ids,
  watermark resume, dedup-merge with keyset rows. Suite green.
- Then re-run the price phase so the newly added markets get price
  histories (the price crawl must pick up markets.parquet additions —
  verify its own checkpoint tolerates the merge; if it assumes an
  immutable market list, fix that here).

### Task 10: Final adversarial review + fix wave (user-mandated)

After Tasks 1–9 are complete: run a fresh adversarial whole-branch review
(most capable model) over everything produced tonight — all code since the
branch's merge-base with main, plus the new docs (methodology, Phase 2
plan, probe findings) checked for internal consistency against the code.
Then fix ALL issues it raises — Critical, Important, and Minor alike (the
user explicitly asked for all issues fixed, overriding the usual
Minor-deferral rule; only findings that are factually wrong may be parked,
with a written ruling). One fix dispatch, one scoped re-review; suite
green; commit and push.
