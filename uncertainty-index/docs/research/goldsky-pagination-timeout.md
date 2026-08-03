# Goldsky `orderFilledEvents` sweep: statement-timeout root cause and fix

**Date:** 2026-08-03
**Module:** `src/uindex/ingest/polymarket_volume.py`
**Endpoint:** `orderbook-subgraph/0.0.1` (Goldsky, project `cl6mb8i9h0003e201j6li0diw`)
**Status:** FIXED — root cause identified, fix verified live against the real stuck cursor.

---

## 1. Reproduction

The stuck cursor on disk (`data/raw/polymarket/volumes_cursor.json`, untouched):

```
ts = 1722938232   (2024-08-06)
id = 0x07e69b35…d336_0x58147d22…299c
n  = 3569700, seq = 16
```

The failing query, replayed verbatim via `httpx` against the same endpoint,
entity and cursor:

| probe | `first:` | result |
|---|---|---|
| `{or: [{timestamp_gt}, {timestamp, id_gt}]}` | 500 | **OK, 1.96 s** |
| same | 100 | OK, 1.98 s |
| same | 10 | OK, 1.92 s |
| same | 1 | OK, 1.93 s |
| `{timestamp_gt: ts}` | 500 | OK, **0.19 s** |

The exact query did *not* fail on replay — it succeeded in ~1.9 s. Two facts
were immediately diagnostic:

1. The `or` form cost **~10× the plain `timestamp_gt` form** at the same cursor.
2. That ~1.9 s was **flat from `first: 1` to `first: 500`** — the cost does not
   come from fetching rows, so the plan is not stopping at the `LIMIT`.

The original log line already showed `limit 100` in the server-side SQL: the
module's adaptive halving had *already* bottomed out at `MIN_PAGE_SIZE` and
still timed out. Page size was never the lever.

## 2. Hypotheses tested

### H1 — dense-timestamp hotspot (many fills sharing `timestamp = 1722938232`)
**FALSIFIED.** `{timestamp: 1722938232}` returns **7 rows** in 0.40 s. There is
no hotspot at the stall.

### H2 — cost grows with scan depth / it's specific to this cursor
**CONFIRMED as a symptom, and far worse than reported.** Sweeping the `or` form
across cursor timestamps, 500-row pages, two reps each:

| cursor | `or` keyset | plain `timestamp_gt` |
|---|---|---|
| 2024-01-01 | 0.34 s / 0.52 s | 0.20 s / 0.15 s |
| 2024-03-01 | 0.42 s / 0.44 s | 0.18 s / 0.16 s |
| 2024-06-01 | 0.65 s / 0.66 s | 0.16 s / 0.16 s |
| **2024-08-06 (stall)** | 1.96 s / 2.07 s | 0.25 s / 0.17 s |
| 2024-10-01 | **TIMEOUT** | 0.18 s / 0.19 s |
| 2025-01-01 | **TIMEOUT** | 0.18 s / 0.15 s |
| 2025-06-01 | **TIMEOUT** | 0.20 s / 0.16 s |
| 2026-01-01 | **TIMEOUT** | 0.15 s / 0.19 s |
| 2026-04-01 | **TIMEOUT** | 0.22 s / 0.19 s |

This reframes the incident. The sweep did not hit one unlucky cursor — it hit a
**wall**. With the `or` predicate, *every* cursor from ~2024-10 to the end of
the subgraph is unreachable. The remaining ~21 months of history could not have
been crawled at any page size. Meanwhile the plain `timestamp_gt` form is a flat
0.15–0.25 s at **every** depth, including 2026-04.

### H3 — bounded time window (`timestamp_gte`/`timestamp_lt`) rescues it
**FALSIFIED**, and this is worth recording because it was the brief's suggested
fix. A 1-hour window wrapped around the same `or` still cost **2.12 s** —
statistically identical to the unbounded form. Narrowing the window does not
change the discarded prefix, so window paging *with an in-window `or` keyset*
would not have fixed this.

### H4 — the `or` is not sargable, so the store cannot seek to the cursor
**CONFIRMED** by a discriminating experiment. Take the identical `or`, and AND a
**redundant** `timestamp_gte` onto it — same result set, but now with a seekable
lower bound:

| cursor | bare `or` | `{and: [{timestamp_gte}, or]}` |
|---|---|---|
| 2024-10-01 | **TIMEOUT** (2.13 s) | **OK, 0.14 s** |
| 2025-06-01 | **TIMEOUT** (2.11 s) | **OK, 0.13 s** |

A redundant predicate that changes nothing semantically turns a hard timeout
into a 0.14 s query. That isolates the cause to the *plan*, not the data.

## 3. Root cause

The server-side SQL in the error is:

```sql
where c.block_range @> $1
  and (((c."timestamp" > $2) or (c."id" > $3 and c."timestamp" = $4)))
order by c."timestamp", "id"
limit 100
```

A bare `OR` across two columns is **not sargable**: Postgres cannot convert it
into a range on the `(timestamp, id)` index, so it cannot *seek* to the cursor.
It can still use the index for **ordering**, but it must walk it from the
beginning of the table and evaluate the `OR` as a filter, discarding every row
*before* the cursor.

That single fact explains every observation:

- cost is **linear in scan depth** (rows *before* the cursor), which is why it
  grew monotonically 0.34 s → 0.65 s → 1.96 s → timeout as the cursor advanced;
- cost is **independent of `first:`**, because the discarded prefix dominates
  and the `LIMIT` only caps the tail;
- a **narrower window doesn't help**, because it doesn't shrink the prefix;
- **any** sargable lower bound fixes it, because it restores the index seek.

The sweep therefore degraded smoothly for seven months of data and then crossed
the store's statement timeout. It was never going to recover on retry: past
2024-10 the predicate is simply unservable.

## 4. The fix

`_fetch_fills` no longer emits `or` at all. The keyset is split into the two
sargable queries whose union is *exactly* the `or`'s result set:

1. `{timestamp: ts, id_gt: id}` — drain the rows sharing the cursor's own
   timestamp that it has not yet consumed;
2. only once (1) returns empty, `{timestamp_gt: ts}` — everything strictly after.

### Why it is correct

- **Equivalence.** `(timestamp > T) OR (timestamp = T AND id > I)` is exactly the
  disjoint union of `(timestamp = T AND id > I)` and `(timestamp > T)`. The two
  branches are mutually exclusive, so the union neither drops nor duplicates.
- **Ordering.** Every row of branch (1) has `timestamp = T`; every row of branch
  (2) has `timestamp > T`. So all of (1) sorts strictly before all of (2), and
  returning (1) alone is a correctly ordered page.
- **No gaps at a dense timestamp.** Branch (2) is only reached when (1) is
  *empty*, i.e. the timestamp is provably fully consumed. A timestamp with more
  fills than one page is drained over successive pages via `id_gt`, and only
  then stepped past.
- **Exhaustion is still unambiguous.** An empty return means *both* branches were
  empty — genuine exhaustion. A short page still just means "keep going", so the
  existing short-page-≠-done guard is untouched.
- **Termination.** Within the tie-drain, ids strictly increase each page, and a
  timestamp holds finitely many fills.
- **Cursor shape is unchanged** (`{"ts", "id"}`), so the on-disk cursor resumes
  with no migration — see §6.

Verified live before writing any code:

- the split reproduces the `or`'s 500 rows in **identical (timestamp, id) order,
  no duplicates**;
- draining an 82-fill timestamp **two rows at a time** via `id_gt` returns that
  timestamp's full set, in order, exactly once;
- both branches are flat in depth: 0.08–0.14 s from 2024-01 through 2026-04.

### Secondary change: `_PageSizer` recovers

The old logic halved on timeout and then **kept the reduced size forever**. That
was right when the timeout was a permanent property of the query shape (the old
1000-row nested-join page). Now that no query is depth-dependent, a timeout is
transient load, and pinning the rest of an 85M-row sweep at the 100-row floor
would multiply the request count sevenfold for one blip. The sizer now steps
back up after `RECOVER_PAGES` (50) clean pages, capped at its starting ceiling.
The floor behaviour is unchanged: a timeout at `MIN_PAGE_SIZE` still raises,
because that is a real outage.

## 5. Tests

`tests/test_ingest_polymarket_volume.py`, +6 tests. Full suite: **180 passed**
(was 174). All mocked; `tmp_path` only; no network, no real data directory.

- `_FakeGoldsky` now serves the three sargable shapes and **asserts no `or` ever
  reaches the store** — the root cause is baked in as a fixture-level invariant,
  so any future reintroduction fails every sweep test at once.
- `test_sweep_survives_a_store_where_the_or_keyset_always_times_out` — the live
  failure in mocked form: a store on which `or` *always* times out while the
  sargable forms serve normally. The sweep completes and sums correctly.
- `test_dense_timestamp_straddling_page_boundaries_is_swept_exactly_once` —
  7 fills on one timestamp with pages of 3 (straddles two boundaries), flanked
  by neighbours either side; asserts each counted exactly once.
- `test_dense_timestamp_is_drained_across_a_portion_restart` — same boundary
  with the process dying mid-timestamp; asserts the committed cursor lands
  *inside* the dense timestamp and the restart loses nothing.
- `test_on_disk_cursor_from_the_or_era_resumes_unchanged` — a state file of the
  old shape with `n=3569700, seq=16`; asserts the first query is the tie-drain,
  that **no query reaches back before the cursor**, that only post-cursor fills
  are summed, and that the prior fill count carries into the manifest.
- `test_page_size_recovers_after_a_transient_timeout` — halves to a working size,
  then climbs back to the ceiling, losing no rows.
- `test_page_sizer_never_exceeds_its_ceiling` — unit guard on the sizer.

Existing coverage retained: cursor resume without double-count, orphan-shard
pruning, flush-boundary summing, short-page-≠-exhausted, floor-raise, staleness.

## 6. Does the existing on-disk cursor resume safely?

**Yes — verified two ways, and nothing on disk was modified.**

The cursor representation is byte-identical (`{"ts", "id"}`, plus `endpoint`),
so `VolumeStore.resume()` is unchanged and the endpoint guard still matches.
There is no migration and no restart from zero.

Empirically, driving the **real `_fetch_fills`** from the **real stuck cursor**
(read-only, 40 pages):

```
page   0  0.18s     5 rows   ts=1722938232   <- drained the stall timestamp
page   1  0.25s   500 rows   ts=1722938684
page  32  0.28s   500 rows   ts=1722948076

11084 fills in 13.6s, 11084 distinct ids (dupes: 0)
strictly increasing in (timestamp, id): True
advanced past the stall: True
no row at/before the resumed cursor: True
```

The first page returns the **5 remaining fills of the stall timestamp** — the
exact rows a naive `timestamp_gt`-only "fix" would have silently skipped. The
sweep then advances normally at ~0.2 s/page.

A second soak was run **deep past the wall**, starting from a 2025-06 cursor
where the `or` form timed out on 100% of attempts — 60 consecutive pages of the
real `_fetch_fills`:

```
16222 fills, 16222 distinct (dupes 0)
strictly increasing: True
latency: min 0.08s  median 0.25s  max 0.92s
no timeout in 60 pages; page size still 500 (never had to shrink)
```

So the region that was wholly unreachable before is now served at a steady
0.25 s/page with no backoff required.

## 7. Residual risk

1. **Volume of remaining work.** Sampling fills/10 min at six dates from
   2024-08 to 2026-04 saturated the 1000-row probe cap at every date after
   2024-08, giving a **lower bound of ~85M fills** still to sweep (~136k/day
   × ~630 days). Measured end-to-end throughput is ~800 fills/s, so this is a
   multi-day crawl, not an overnight one. This is a scheduling fact, not a
   correctness problem, and it was previously *infinite* — the sweep could not
   pass 2024-10 at all. The portioned exit-3 protocol is intact, so it resumes
   across as many portions as needed.
2. **Request amplification.** Each page now costs ~3 requests per 500 rows
   (tie-drain + advance, plus a tie-drain page at each boundary) instead of 1.
   The obvious optimisation — skip the tie-drain when the previous page came
   back short — was **deliberately rejected**: this module documents that the
   endpoint returns *spuriously* short pages under load (`_ShortPageOnce`), so
   inferring "timestamp complete" from page length could silently skip fills.
   A safe variant exists (drop the trailing partial-timestamp group and
   re-fetch it) but it changes the accumulation path, which is where the
   exactly-once guarantee lives; not worth the risk for a 3× constant.
3. **Planner dependence is now eliminated, not merely mitigated.** Every query
   the sweep issues is a single-column equality or range with no `or`, so there
   is no plan for the optimiser to get wrong at depth. The `{and: [gte, or]}`
   form measured 0.13 s and would have been a one-line change, but it relies on
   the planner continuing to choose the seek; the split form does not.
4. **Unchanged upstream caveats.** The subgraph is still frozen at 2026-04-28,
   still does not index negRisk/AMM fills, and `volumes_coverage.csv` remains
   the audit trail. Point-in-time discipline is unaffected: no crawl-time
   quantity was introduced.
5. **Transient transport errors** (`RemoteProtocolError`, `ReadTimeout`) still
   propagate to the driver's retry loop, as before. The log shows those
   recovering on their own; they were not part of this failure and were left
   alone deliberately.

## 8. Files changed

- `src/uindex/ingest/polymarket_volume.py` — `_fetch_fills` split into two
  sargable queries; `_where` removed; `_PageSizer`/`_page` replace the inline
  halving; module docstring records the live measurements above.
- `tests/test_ingest_polymarket_volume.py` — fake updated to the new shapes with
  an anti-`or` assertion; 6 tests added.
