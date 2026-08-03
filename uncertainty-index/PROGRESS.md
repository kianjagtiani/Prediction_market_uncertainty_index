# Progress — Uncertainty Index

_Last updated: 2026-08-03 (paused mid-backfill to free the machine; all code
committed & pushed, all crawl state checkpointed on disk)._

## TL;DR for the next session

All **code** for Phase 1 close-out is written, reviewed, and pushed
(180 tests green). What remains is **data**: three crawls are paused
mid-flight with intact cursors, and the three tasks that consume their
output (validation run, methodology doc, final review) cannot start until
the data lands.

**Branch:** `finish-overnight` (pushed to origin), based on
`phase1-uncertainty-index`. Worktree:
`.claude/worktrees/finish-overnight/`. HEAD = `a2f8445`.

⚠️ **No PR exists.** `gh pr create` fails — the PAT lacks PR permissions
(same limitation noted 2026-07-30; pushes work over SSH). Open the PR by
hand in the GitHub UI: `finish-overnight` → `phase1-uncertainty-index`.

⚠️ **Machine was thrashing when we stopped** — swap at 16.3 GB of 17.4 GB
on 8 GB RAM, disk 88% full. Reboot before resuming, and **run one crawl at
a time**, not three. Running all three concurrently is what caused it.

## Resume commands

Run these **one at a time**, from `uncertainty-index/`. Each is safely
resumable and each logs to `data/logs/<venue>.log`. Every one of them
aborts after 3 consecutive portion failures, so wrap in a retry loop if
running unattended (see "Retry wrappers" below).

```bash
# 1. Polymarket legacy metadata — resumes at id watermark 280,001 of 559,650.
#    Then automatically runs the price phase for newly merged markets.
caffeinate -i scripts/run_backfill.sh polymarket

# 2. Polymarket volume sweep — resumes at 3,569,700 fills.
#    MUST run after (1): shares the same lock file, and (1)'s merge
#    invalidates the token map by design.
caffeinate -i scripts/run_backfill.sh polymarket_volume

# 3. Kalshi metadata — resumes at 76,516,264 markets seen.
#    Independent lock; can run alongside (1) or (2) if RAM allows — but
#    on this machine it did not. Prefer sequential.
caffeinate -i scripts/run_backfill.sh kalshi
```

### Retry wrappers

`run_backfill.sh` stops after 3 consecutive portion failures, which
transient network errors trigger regularly on multi-hour crawls. The
overnight run used a trivial outer loop; recreate it as needed:

```bash
for i in $(seq 1 30); do
  caffeinate -i scripts/run_backfill.sh <venue> && break
  sleep 180
done
```

## Crawl state (all checkpointed, all lossless)

| Crawl | Position | Est. remaining | Blocking |
|---|---|---|---|
| PM legacy metadata | watermark **280,001** / 559,650; 1,621 markets kept | ~1 h at observed 4.5k ids/min | Task 6 |
| PM price phase | not started (runs after legacy merge) | unknown; scales with markets recovered | Task 6 |
| PM volume sweep | **3,569,700** fills, through 2024-08-06 | **~30 h minimum** (see below) | Task 6 |
| Kalshi metadata | **76,516,264** markets seen | unknown — see "Kalshi is enormous" | Task 6 |

Data on disk: `markets.parquet` 6,210 rows (keyset range only),
`prices.parquet` 776,450 rows, 1,531 Kalshi shards (1.8 GB), 4 legacy
shards, 256 volume flush files.

### The volume sweep is a multi-day job, not an overnight one

The Goldsky fix (below) made the sweep *possible*; it did not make it
fast. Measured lower bound: **~85M fills still to sweep at ~800 fills/s ≈
30 h**, and that is a lower bound because the probe saturated its sampling
cap at every date past the wall. Plan around this — it is the critical
path for Task 6. If a faster path matters more than fidelity, that is a
methodology decision to make deliberately, not by tuning.

### Kalshi is enormous

76.5M markets seen and still climbing, against a July estimate of 10.3M.
The stuck-pagination tripwire has **not** fired and the cursor advances
normally, so this is real catalog size, not a pagination bug — the tail is
sports multi-game parlay markets (`KXMVESPORTSMULTIGAME...`). Nearly all
will be dropped by the liquidity floor. **Worth deciding before resuming:**
a series-level prefilter that skips the parlay flood would likely cut this
crawl by an order of magnitude. Left as-is because changing the universe
mid-crawl is a methodology change, not an optimization.

## What landed this session

All four commits are on `finish-overnight`, each reviewed by an
independent subagent with at least one fix round.

### Task 4 — Polymarket volume source switch (`e19f2ea`, `b843b36`)

The swept subgraph (`polymarket-orderbook-resync`) was frozen since
2026-01-05. Repointed to `orderbook-subgraph/0.0.1`, entity
`orderFilledEvents` (not `enrichedOrderFilleds`), deriving token and
notional from the non-`"0"` leg of `makerAssetId`/`takerAssetId` with the
matching `AmountFilled / 1e6`.

- Live spot-check reconciled **exactly**: 5,327 fills summing to
  6319.049035999963 vs the subgraph's own `scaledCollateralVolume`
  6319.049036. Caveat recorded: that aggregate is computed by the same
  mapping over the same events, so it pins field selection and scale but
  is **not independent ground truth**.
- Staleness discipline: `STALENESS_DAYS = 3`; a sweep whose horizon is
  further behind is recorded complete-but-stale, never silently complete.
  Days past the horizon stay NaN (never 0), rolling notional carries
  forward causally from the last covered day, and those days are flagged
  in the panel.
- **Review fix:** a state file lacking an `endpoint` key — the shape
  *every* pre-switch file has — was defaulting to "matches current
  endpoint," so the old frozen cursor would have been silently resumed
  against the new deployment. Now treated as a legacy deployment and
  discarded.

### Task 11 — Legacy Polymarket metadata backfill (`9f8f293`, `cda53db`)

Gamma's keyset endpoint only serves ids ≥ 559651 (created ≥ 2025-07-03),
so the catalog was missing all of 2024-01-01 → 2025-07-03 — including the
2024 election markets the event study depends on. Adds a repeated-`id`
batch sweep over ids 1..559650 with its own watermark, atomic shards, and
a dedup merge into `markets.parquet`.

- Live probing found `/markets` defaults to `closed=false` and **silently
  drops resolved markets** — a naive sweep would have missed precisely the
  settled election markets this task exists to recover. The sweep unions a
  `closed=true` and a `closed=false` pass.
- **Review fixes, all three of which caught real defects:**
  1. No `limit` was sent, and re-probing proved the first implementation
     was **already silently truncating** — returning 20 markets where 40
     existed. Now sends `limit=len(ids)+1` and raises if the response hits
     that limit, which makes truncation structurally distinguishable from
     a genuinely full batch.
  2. Nothing could detect an under-returned batch and the watermark
     advanced regardless. Added `verify_legacy_completeness()`: a row-count
     floor plus a live spot-check of known-good ids (253591, 559640) that
     fails the phase if either is absent from the recovered catalog.
  3. The merge grew `markets.parquet` after the volume sweep may already
     have considered itself done, which would leave every recovered market
     with zero volume. The merge now invalidates `token_map.parquet`,
     `token_map_cursor.json`, `volumes.parquet`, and
     `volumes_coverage.csv` — but only when it actually adds a market_id,
     and before the merge, so a crash mid-merge still converges.

Sanity anchor: id **253591** is "Will Donald Trump win the 2024 US
Presidential Election?", $1.53B volume, resolving 2024-11-05. If the
legacy sweep finishes and that market is absent from `markets.parquet`,
something is wrong regardless of what the logs say.

### Goldsky pagination blocker — root-caused and fixed (`a2f8445`)

The volume sweep died deterministically at cursor ts 1722938232 with a
Postgres **statement timeout**, after ~3.57M fills.

**Root cause:** the keyset filter `{or: [{timestamp_gt}, {timestamp,
id_gt}]}` is not sargable. Postgres cannot seek to the cursor, so it walks
the index from the table start discarding every prior row — page cost is
linear in scan *depth* and independent of `first:`. It crossed the
statement timeout at 2024-08-06, and **every cursor past ~2024-10 was
unservable at any page size or any time window.** A bounded time-window
workaround was measured and does *not* help (2.12 s, same as unbounded).

The fix pages with sargable queries instead. Verified against the *real*
stuck cursor: the first page returns the 5 remaining fills of the stall
timestamp — which a naive `timestamp_gt`-only fix would have **silently
skipped** — then 11,084 fills with 0 duplicates. Cursor representation is
unchanged, so the on-disk cursor resumes with no migration and no restart.

Full analysis:
`.superpowers/sdd/2026-08-01-overnight-completion/goldsky-timeout-report.md`

## Remaining tasks (in dependency order)

From `docs/plans/2026-08-01-overnight-completion.md`. Tasks 1, 2, 3, 4, 7,
and 11 are **done**.

1. **Task 5 (operational)** — finish the three crawls above. This is the
   long pole; everything else waits on it.
2. **Task 9 (minor backlog)** — independent of data, can be done anytime:
   (a) universe tests read the real overrides CSV, point them at a
   fixture; (b) add ruff to `.venv` and fix findings in changed files
   only; (c) fuzzy title dedup stays a documented TODO.
3. **Task 6** — `normalize` → `pipeline` → `validate/report.py` on real
   data. **If a validation check fails, record it honestly — do not tune
   parameters to pass.**
4. **Task 8** — public methodology doc. Must cover v2 rules, the negRisk
   volume resolution, and a limitations section. Deferred findings that
   belong in it are listed below.
5. **Task 10** — final adversarial whole-branch review, then fix
   everything it raises (the user asked for all severities fixed, not just
   Critical/Important). One fix dispatch, one scoped re-review.

## Deferred findings — inputs to Task 8's limitations section

Carried from review rounds; each was ruled non-blocking at the time.

- **Carry-forward never expires.** Past a stale volume horizon a
  Polymarket market's weight stays pinned at its last observed value
  indefinitely while remaining index-eligible. It is flagged
  (`volume_stale`) but not bounded. The brief mandated carry-forward
  without decay, so this is compliant — and it is exactly the kind of
  thing a methodology doc must disclose.
- **Coverage thresholds not recalibrated** for the new subgraph:
  `PM_MIN_SUBGRAPH_COVERAGE` and `PM_TOKEN_LEGS_PER_FILL` were tuned
  against the old frozen source.
- **`LEGACY_MIN_KEPT_MARKETS = 500`** is anchored to a single empirical
  point (keyset's 6,210 kept markets); the spot-check is the sharper
  detector.
- **`archived` / NULL-`closed` handling** rests on a single ~100-market
  live sample, documented rather than proven.
- `_token_and_notional`'s `else` branch assumes `takerAssetId == "0"`
  without checking; a token↔token fill would book quantity as notional.
- From methodology v2, still unaddressed: EWMA per-observation halflife,
  row-based rolling window, `n_constituents` conflation, fuzzy dedup TODO.
- On-chain RPC ingestion for post-2026-04-28 fills and the residual
  0%-coverage negRisk markets (pm_559700-class) remain out of scope.

## Key references

- Overnight plan: `docs/plans/2026-08-01-overnight-completion.md`
- Phase 1 plan: `docs/plans/2026-07-12-uncertainty-index-phase1.md`
- Phase 2 plan: `docs/plans/2026-08-01-phase2-site.md` (written, reviewed)
- Phase 2 spec: `docs/specs/2026-07-29-phase2-site-design.md`
- negRisk probe (§5 is the volume-source spec):
  `docs/research/negrisk-coverage-probe.md`
- Goldsky timeout analysis + per-task reports and the SDD ledger:
  `.superpowers/sdd/2026-08-01-overnight-completion/`
- VIX/EPU adoption playbook: `docs/research/vix-adoption-history.md`
