# Probe: negRisk fill coverage in the Goldsky Polymarket subgraph

_2026-08-01. Task 1 of the overnight completion plan. Read-only investigation;
no changes under `src/`. All queries via `curl` against public endpoints,
batch ≤ 500 rows, each request `--max-time 30`._

## Verdict

**The simple hypothesis — "NegRiskCtfExchange fills are categorically missing
from the subgraph" — is REFUTED as stated, but a real and severe negRisk-linked
coverage gap DOES exist for a subset of negRisk markets, and it is dwarfed by a
bigger, previously-unknown problem: the subgraph our sweep currently points at
is frozen and has not indexed any new fill since 2026-01-05.**

Three separate, independent findings, each confirmed with query evidence below:

1. **NegRisk coverage is real but highly market-dependent, not categorical.**
   Some negRisk markets reconcile near 100% (even >100%) against Gamma; others
   are at 0%. Coverage does not correlate cleanly with the `negRisk` flag
   alone — a comparably-sized non-negRisk market also showed a near-total gap,
   traced to a different cause (see #3).
2. **The subgraph currently swept (`polymarket-orderbook-resync/prod`) stopped
   indexing on 2026-01-05 — about 7 months before today (2026-08-01).** A
   sibling subgraph that Polymarket's own docs point to
   (`orderbook-subgraph/0.0.1`) is more current but itself froze on
   2026-04-28, matching Polymarket's documented CTF-Exchange v1→v2 contract
   migration. **No publicly reachable subgraph on this Goldsky project has any
   fill data from the last ~3 months.** This affects every market, negRisk or
   not, and is a bigger threat to PIT correctness than negRisk alone.
3. **The brief's `pm_544097` "non-negRisk, old / AMM legacy" framing is
   itself wrong under current Gamma data**: Gamma now reports
   `negRisk: true` for `pm_544097` (it has a `negRiskMarketID` and
   `groupItemTitle: "30+ years"` — it's a bucket of a multi-outcome sentencing
   market), it was created 2025-05-12 (not "old"/pre-CLOB), and its on-chain
   `Condition` has **zero** `FixedProductMarketMaker` pools in the subgraph —
   there is no AMM leg to account for the gap. `Gamma volume == volumeClob ==
   volumeNum` exactly on every market sampled (10/10), negRisk and
   non-negRisk alike, so there is no distinguishable AMM-only volume field to
   reconcile against in current data.

---

## 1. Evidence: ≥3 negRisk and ≥3 non-negRisk markets

Method: for each market, pulled `conditionId` / `clobTokenIds` from
`gamma-api.polymarket.com/markets/{id}`, then queried the `Orderbook` entity
(`{ orderbook(id: $tokenId) { tradesQuantity scaledCollateralVolume } }`) for
both outcome tokens on `polymarket-orderbook-resync/prod` and summed. This
entity is Goldsky's own running aggregate over `EnrichedOrderFilled`/
`OrderFilledEvent` — using it instead of paging through raw fills avoids the
1000-row timeout risk and gives an exact total in one request per token.
`notional_usd = scaledCollateralVolume` (already `/1e6`-scaled by the
subgraph; verified in the module docstring and re-confirmed live below).

### negRisk markets (Gamma `negRisk: true`)

| id | question | Gamma volume | subgraph total (resync) | coverage | fills |
|---|---|---:|---:|---:|---:|
| pm_559700 | Will Adam Schiff win the 2028 Dem nomination? | $87,364 | **$0** | **0%** | 0 |
| pm_544097 | Weinstein sentenced 30+ years? (bucket) | $95,322 | $5,845.79 | 6.1% | 423 |
| pm_253591 | Will Trump win the 2024 election? | $1,531,479,285 | $1,636,188,222 | 106.8% | 5,109,619 |
| pm_253597 | Will Harris win the 2024 election? | $1,037,039,118 | $886,879,597 | 85.5% | 2,812,944 |
| pm_511754 | Will Trump be inaugurated? | $400,409,527 | $477,855,564 | 119.3% | 2,501,200 |
| pm_507892 | Sacramento Kings win 2025 NBA Finals? | $378,011,507 | $55,152,246 | 14.6% | 182,635 |

Coverage across these six ranges from 0% to 119%. The three 2024 US-election
markets (Trump/Harris/inauguration) — the flagship, highest-liquidity, mostly
binary negRisk markets — reconcile near or above 100%. The three
smaller/many-way-group negRisk markets (Schiff — 1 of ~20 candidates;
Weinstein — 1 of ~7 sentence buckets; Kings — 1 of 30 NBA teams) reconcile at
0–15%. This pattern (severe gap concentrated in markets that are one bucket of
a large multi-outcome group) is consistent with volume in those markets
coming disproportionately from cross-outcome complement-minting/arbitrage
flows through the neg-risk adapter rather than simple two-party order
matching — the former plausibly doesn't emit the same `OrderFilledEvent` the
subgraph indexes. This is the most defensible mechanism given the evidence,
but it was **not confirmed at the contract/event level** (no `exchange`
address field exists anywhere in the subgraph schema to test directly — see
§3). Treat it as a hypothesis, not a proven root cause.

### non-negRisk markets (Gamma `negRisk: false`, confirmed via API, not inferred)

Restricted to markets whose full lifetime predates the 2026-01-05 freeze
(§2), so staleness doesn't confound the reading:

| id | question | Gamma volume | subgraph total (resync) | coverage | fills |
|---|---|---:|---:|---:|---:|
| pm_546814 | Zelenskyy wears a suit before July? | $242,231,180 | $177,058,978 | 73.1% | 112,776 |
| pm_680392 | US government shutdown Saturday? | $157,296,576 | $787,409 | **0.5%** | 12,142 |
| pm_507276 | TikTok banned in the US before May 2025? | $119,653,358 | $99,937,855 | 83.5% | 100,719 |
| pm_797327 | Trump releases Epstein files by Dec 19? | $90,915,984 | $75,195,695 | 82.7% | 61,278 |
| pm_812010 | Eleven dies in Stranger Things S5? | $80,824,835 | $62,952,407 | 77.9% | 43,176 |

Two things this table establishes:

- **A general Gamma-vs-subgraph gap (roughly 15–30% under) is pervasive
  across non-negRisk markets too** — this is not negRisk-specific baseline
  noise, it's the ordinary reconciliation slop already flagged in the prior
  probe (Belgium market reconciled within 4%; these are worse, so 4% is not a
  reliable general bound).
- **pm_680392's 0.5% coverage is not a negRisk artifact.** Its `endDate` is
  2026-01-31 but the resync subgraph's last indexed fill is 2026-01-05
  (§2) — a government-shutdown market's volume is heavily concentrated near
  resolution, so most of its trading life falls after the freeze. This is
  the staleness bug (§2) manifesting on an ordinary market, not a negRisk
  bug. It's included here specifically as a caution: **a raw coverage-%
  number is not by itself evidence of a negRisk-specific gap** unless the
  market's full lifetime is checked against the subgraph's actual coverage
  window.

---

## 2. The bigger problem: the swept subgraph is frozen (~7 months stale)

```
polymarket-orderbook-resync/prod  _meta.block.number = 81,265,743
                                   latest EnrichedOrderFilled.timestamp = 1767650745
                                   = 2026-01-05T22:05:45Z

orderbook-subgraph/0.0.1          _meta.block.number = 87,814,766
(= orderbook-subgraph/prod,       latest OrderFilledEvent.timestamp = 1777374040
 identical deployment)            = 2026-04-28T11:00:40Z
```

Both are static/frozen — verified by querying a market created **2026-07-18**
(`pm_2970448`, non-negRisk, currently trading, $1.9M in `volume24hr` per
Gamma today): its `Orderbook` entity is `null` in **both** subgraphs, because
neither has indexed anything from after its own freeze point.

The 2026-04-28 freeze date on `orderbook-subgraph` is not a coincidence: web
search corroborates that Polymarket migrated to a new set of CTF Exchange
contracts on 2026-04-28 (`Polymarket/ctf-exchange-v2` on GitHub) and that its
own market-data pipeline now merges fill streams from "v1 CTF, v1 NegRisk, v2
CTF, and v2 NegRisk" exchanges — i.e. Polymarket's *own* current
infrastructure treats v1 and v2 as genuinely separate sources, and both
Goldsky subgraphs found here only ever indexed the v1 contracts. **No v2
subgraph was found publicly reachable on this Goldsky project** (§3).

**Why this matters more than negRisk:** `polymarket-orderbook-resync` is the
subgraph `src/uindex/ingest/polymarket_volume.py` sweeps today. Because it is
frozen, a full backfill run will exhaust its data on 2026-01-05 and — per
Task 2's review finding C1 (`sweep()` treats `len(rows) < size` as "done" with
no freshness check) — **will silently mark the sweep complete with no
warning**, then `normalize.py`'s `fillna(0.0)` will treat every day since
2026-01-05 as literally zero Polymarket volume for every market, negRisk or
not. This is the same failure mode C1 already describes, but it is now known
to be *guaranteed to trigger on this data*, not merely a risk under
degraded-service conditions. Whatever fix path Task 4 implements for negRisk
must also carry a freshness/coverage-window check, or it inherits this bug at
its own new freeze point (2026-04-28 if `orderbook-subgraph` is adopted).

---

## 3. Fix-path search: is there a subgraph that covers NegRiskCtfExchange fills?

**Schema introspection of `polymarket-orderbook-resync`:** no `exchange`
address field exists on `EnrichedOrderFilled`, `OrderFilledEvent`, or any
other type; no entity named anything like `NegRisk*` exists. Full object-type
list: `Account, Collateral, Condition, EnrichedOrderFilled,
FixedProductMarketMaker, FpmmFundingAddition, FpmmFundingRemoval,
FpmmPoolMembership, Global, MarketData, MarketPosition, MarketProfit, Merge,
OrderFilledEvent, Orderbook, OrdersMatchedEvent, OrdersMatchedGlobal,
Redemption, Split, Transaction`. There is no way, from this subgraph alone,
to distinguish CTFExchange-routed from NegRiskCtfExchange-routed fills, and
no separate entity capturing what the first one misses.

**Sibling-subgraph slug search** (36 candidates probed under
`project_cl6mb8i9h0003e201j6li0diw`, HTTP 200 vs 404):

- 19 guesses at `polymarket-{negrisk-ctf-exchange, neg-risk-ctf-exchange,
  negriskctfexchange, negrisk, neg-risk, negrisk-adapter, negriskadapter,
  activity(-subgraph), positions(-subgraph), pnl(-subgraph),
  names(-subgraph), open-interest(-subgraph), orderbook,
  orderbook-subgraph}/prod/gn` — **all 404**.
- 6 guesses at `{orderbook,activity,positions,pnl,open-interest,names}
  -subgraph/0.0.1/gn` (the versioning scheme documented at
  `docs.polymarket.com/market-data/subgraph`) — **only `orderbook-subgraph`
  exists (200)**; the other four categories mentioned in Polymarket's docs
  (Positions, Activity, PNL, Open Interest) are not reachable at this
  version tag under this project id.
- 11 guesses at v2/negrisk-specific variants (`orderbook-subgraph/{0.0.2,
  0.0.3, 1.0.0, prod}`, `orderbook-subgraph-v2/*`, `orderbook-v2-subgraph/*`,
  `polymarket-orderbook-v2/prod`, `{negrisk,neg-risk}-orderbook-subgraph/0.0.1`,
  `ctf-exchange-v2/0.0.1`) — **only `orderbook-subgraph/prod` exists (200),
  and it is byte-identical in `_meta`/latest-timestamp to
  `orderbook-subgraph/0.0.1`** (same deployment, two tags).

**Does the one live sibling (`orderbook-subgraph/0.0.1`) fix negRisk
coverage?** Partially, and inconsistently:

| market | resync total | orderbook-subgraph/0.0.1 total | Gamma | resync cov. | new cov. |
|---|---:|---:|---:|---:|---:|
| pm_559700 Schiff2028 | $0 | $0 | $87,364 | 0% | 0% (unchanged) |
| pm_544097 Weinstein | $5,845.79 (423 fills) | $37,112.03 (7,595 fills) | $95,322 | 6.1% | 38.9% |
| pm_507892 KingsNBA | $55,152,245.84 (182,635 fills) | $55,152,245.84 (182,635 fills) | $378,011,507 | 14.6% | 14.6% (identical, byte-for-byte) |

This is itself a finding worth flagging honestly: for the same market over
the same historical period (both markets fully resolved well before either
subgraph's freeze date), the two subgraphs disagree for Weinstein (423 vs
7,595 fills — `orderbook-subgraph` has strictly more data, not just newer
data) but agree exactly for KingsNBA. That means `polymarket-orderbook-resync`
has at least one real indexing gap independent of staleness — not just
"frozen", but incomplete even within its covered window, for some markets.
Root cause not identified (would require Polygon RPC log-level comparison,
out of scope for this probe). Net: switching to `orderbook-subgraph` is a
free, drop-in improvement, but it does **not** fully close the negRisk gap
(Schiff2028 stays at 0% in both) and does **not** extend coverage past
2026-04-28.

**Conclusion: no publicly reachable Goldsky subgraph gives complete
NegRiskCtfExchange coverage.** Ranked fallback options for the residual gap
(both the sub-100%-coverage negRisk markets and the >2026-04-28 recency gap):

| # | Option | PIT-correctness | Effort | Notes |
|---|---|---|---|---|
| 1 | Switch sweep source to `orderbook-subgraph/0.0.1` | Same class as current (subgraph-derived, block-height PIT) | Very low — same GraphQL shape, see §4 | Free win: fixes the Weinstein-class gap and extends coverage to 2026-04-28. Does not fix Schiff-class (0%) markets or the >04-28 window. Recommended as the immediate Task 4 change regardless of what else is done. |
| 2 | On-chain logs via public Polygon RPC (`eth_getLogs` on NegRiskCtfExchange `0xC5d563A36AE78145C45a50134d48A1215220f80` and/or v2 `0xe2222d279d744050d28e00520010520000310F59`, `OrderFilledEvent`/similar topic) | Highest — ground truth, no subgraph indexing gap possible | High — need a log fetcher + ABI decode + own PIT bucketing; public RPC rate limits (Polygon has ~2s blocks, ~7M blocks since 2024 launch) make a full historical backfill a multi-day job even chunked | Best long-run fix if negRisk/recency gaps turn out to matter for the index's headline markets. Addresses on GitHub / PolygonScan search need re-verification before use — the exact checksummed addresses were pulled from a web summary, not confirmed byte-for-byte here. |
| 3 | Dune Analytics (community/own SQL over decoded Polymarket tables) | Medium-high if decoded tables are complete and PIT-correct (untested here) | Medium — needs a Dune API key/credits, and community query completeness/currency was not verified in this probe | Plausible but unverified; would need its own probe before relying on it. |
| 4 | Gamma daily volume timeseries | N/A — does not exist | N/A | Already ruled out in the prior probe (`docs/probe-findings.md`): only rolling `volume24hr/1wk/1mo/1yr` snapshots, no daily history endpoint found after exhausting guesses. Not re-probed here; citing the prior finding. |
| 5 | Accept and disclose the gap | Lower — under-weights affected negRisk markets and the post-04-28 window | Zero | Given time constraints, this is a legitimate Task 4/8 choice **if** paired with the manifest/freshness guard from Task 2 finding C1, so the gap is visible (`NaN`, not silently `0.0`) rather than silently zeroing out affected markets. |

---

## 4. Non-negRisk `volumeClob` reconciliation (brief item 3)

Checked across all 11 sampled markets (6 negRisk + 5 non-negRisk):
`volume == volumeClob == volumeNum` **exactly**, to the fractional cent, in
every single case (e.g. pm_559700: all three fields `87364.25783299998`;
pm_544097: all three `95321.71520800001`). There is no case in this sample
where `volumeClob` differs from `volume`. This means:

- Brief item 3's premise — that `volumeClob` might reconcile better than
  `volume` because `volume` includes a legacy AMM component — **does not
  apply to current Gamma data**: there's nothing to compare, the fields are
  identical.
- The AMM-legacy explanation for `pm_544097`'s gap is independently refuted
  by the `Condition` entity check: `condition(id:
  "0x2499928ffbe6022444543dcd940075259cecb5e41e346284b578cb64e1404d32")
  .fixedProductMarketMakers` is `[]` (empty) — zero AMM pools ever existed
  for this condition on-chain, per the subgraph. There is no AMM leg to
  recover.
- Combined with `pm_544097` actually being `negRisk: true` under current
  Gamma data (see Verdict, point 3), its ~6–39% coverage is better explained
  by the same negRisk-adjacent gap as Weinstein's sibling buckets, not by an
  AMM-era/legacy-market story.

---

## 5. Concrete spec for Task 4

**Recommended primary change:** point the sweep at
`orderbook-subgraph/0.0.1` (equivalently `.../prod` — same deployment)
instead of `polymarket-orderbook-resync/prod`. This is a strict upgrade on
every axis measured (more current, more complete on at least one sampled
market, identical on the rest, same scaling constant) and requires a schema
change because **the entity is different — `EnrichedOrderFilled` does not
exist on this subgraph.**

- **Endpoint:**
  `POST https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn`
- **Entity:** `orderFilledEvents` (not `enrichedOrderFilleds`). Fields
  confirmed present: `id, transactionHash, timestamp, orderHash, maker,
  taker, makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled,
  fee`. No `market { id }` join — the CLOB token id must be derived from
  whichever of `makerAssetId`/`takerAssetId` is **not** `"0"` (`"0"` is the
  USDC-collateral leg; confirmed live on 5 sample rows, §below).
- **Query shape** (mirrors the existing `FILLS_QUERY` pattern):
  ```graphql
  query($first: Int!, $where: OrderFilledEvent_filter) {
    orderFilledEvents(first: $first, orderBy: timestamp,
                       orderDirection: asc, where: $where) {
      id timestamp makerAssetId takerAssetId
      makerAmountFilled takerAmountFilled
    }
  }
  ```
- **Scaling / notional derivation** (replaces `size / 1e6` on `market{id}`):
  for each row, if `makerAssetId == "0"`: `token_id = takerAssetId`,
  `notional_usd = int(makerAmountFilled) / 1e6`; else (`takerAssetId ==
  "0"`): `token_id = makerAssetId`, `notional_usd =
  int(takerAmountFilled) / 1e6`. Verified live on 5 rows against sane
  implied prices (0.02–0.07, consistent with a long-shot outcome token) —
  not a full exact-sum reconciliation against `Orderbook.scaledCollateralVolume`
  the way the original `size/1e6` rule was verified in the module docstring;
  Task 4 should do that exact-sum spot-check (same method as the docstring's
  243-fill check) before trusting this at scale.
- **Pagination/cursor:** same filter shape works —
  `{"or": [{"timestamp_gt": ts}, {"timestamp": ts, "id_gt": id}]}` for resume
  (confirmed the filter type exposes `id_gt`, `timestamp_gt/gte/lt/lte`,
  `and`, `or`). `first: 500` returned 500 rows in 0.38s in a live test
  starting exactly from the old resync cursor
  (`timestamp_gte: "1767650745"`) — well inside the 30s/500-row constraint,
  no adaptive-halving evidence needed in this sample but keep the existing
  halving-on-timeout logic defensively.
- **Merge with the existing sweep:** do **not** run two parallel sweeps
  against two different subgraphs long-term — `orderbook-subgraph` is a
  superset-or-equal of `polymarket-orderbook-resync` everywhere sampled, so
  the clean design is a **single sweep against `orderbook-subgraph`**,
  starting `BACKFILL_START` fresh (its data goes back further than the
  resync cursor in this codebase's state, since it's a different deployment
  ID — do not attempt to resume the old resync cursor against the new
  subgraph; start over). No union/reconciliation logic needed since we're
  not aware of complementary (non-overlapping) coverage between the two —
  the evidence found is subset/improvement, not disjoint coverage.
- **Required new guard (do this in Task 4, not deferred to Task 3's C1
  fix alone):** record the max fill timestamp actually reached and refuse to
  treat the sweep as "complete" — or at minimum, flag in the manifest —  if
  that timestamp is more than a few days behind `time.time()`. Given
  `orderbook-subgraph` is *itself* frozen at 2026-04-28, a real production
  run today (2026-08-01) **will** hit this condition; the manifest/NaN
  discipline from Task 2 finding C1 is not optional here, it is guaranteed
  to trigger on first real use. Document the resulting ~3-month (and
  counting) gap explicitly in Task 8's methodology/limitations doc rather
  than silently zero-filling it.

---

## Appendix: raw query evidence log

All requests were `curl -s --max-time 30 -X POST <url> -H
"Content-Type: application/json" -d '{"query": ..., "variables": ...}'`
against://
- `gamma-api.polymarket.com/markets/{id}` (path form; the `?id=` query-param
  form intermittently returned empty results for closed markets in this
  session — path form was reliable and used throughout)
- `https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/polymarket-orderbook-resync/prod/gn`
- `https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn`

Key raw responses (condensed):

```
# resync _meta / latest fill
{"data":{"_meta":{"block":{"number":81265743,"timestamp":null}}}}
{"data":{"enrichedOrderFilleds":[{"timestamp":"1767650745"}]}}  # 2026-01-05T22:05:45Z

# orderbook-subgraph/0.0.1 _meta / latest fill
{"data":{"_meta":{"block":{"number":87814766,"timestamp":null}}}}
{"data":{"orderFilledEvents":[{"timestamp":"1777374040"}]}}     # 2026-04-28T11:00:40Z

# pm_2970448 (created 2026-07-18, non-negRisk, $1.9M volume24hr) — proves both frozen
resync:            {"data":{"orderbook":null}}
orderbook-subgraph:{"data":{"orderbook":null}}

# pm_559700 Schiff2028 — both tokens, both subgraphs
resync:             {"data":{"orderbook":null}}                 (yes token)
orderbook-subgraph: {"data":{"orderbook":null}}                 (yes token)

# pm_544097 Weinstein — condition entity, resync
{"data":{"condition":{"id":"0x2499928ffbe6022444543dcd940075259cecb5e41e346284b578cb64e1404d32",
  "outcomeSlotCount":2,"fixedProductMarketMakers":[]}}}

# orderFilledEvents scaling sample (Weinstein yes token, orderbook-subgraph)
{"makerAssetId":"0","takerAssetId":"8703...778714","makerAmountFilled":"210","takerAmountFilled":"10000"}
{"makerAssetId":"0","takerAssetId":"8703...778714","makerAmountFilled":"26600","takerAmountFilled":"1330000"}
{"makerAssetId":"8703...778714","takerAssetId":"0","makerAmountFilled":"8300000","takerAmountFilled":"505100"}
```

Sibling-subgraph slugs probed (36 total, 2 distinct live deployments found —
`orderbook-subgraph/0.0.1` and its `prod` alias):

```
404: polymarket-negrisk-ctf-exchange, polymarket-neg-risk-ctf-exchange,
     polymarket-negriskctfexchange, polymarket-negrisk, polymarket-neg-risk,
     polymarket-negrisk-adapter, polymarket-negriskadapter,
     polymarket-activity, polymarket-activity-subgraph, polymarket-positions,
     polymarket-positions-subgraph, polymarket-pnl, polymarket-pnl-subgraph,
     polymarket-names, polymarket-names-subgraph, polymarket-open-interest,
     polymarket-open-interest-subgraph, polymarket-orderbook,
     polymarket-orderbook-subgraph
     (all at /prod/gn)
404: activity-subgraph/0.0.1, positions-subgraph/0.0.1, pnl-subgraph/0.0.1,
     open-interest-subgraph/0.0.1, names-subgraph/0.0.1
200: orderbook-subgraph/0.0.1   <-- live, documented at
                                    docs.polymarket.com/market-data/subgraph
404: orderbook-subgraph/{0.0.2,0.0.3,1.0.0}, orderbook-subgraph-v2/0.0.1,
     orderbook-v2-subgraph/0.0.1, polymarket-orderbook-v2/prod,
     negrisk-orderbook-subgraph/0.0.1, neg-risk-orderbook-subgraph/0.0.1,
     ctf-exchange-v2/0.0.1, orderbook-subgraph-v2/prod
200: orderbook-subgraph/prod    <-- identical _meta/timestamp to 0.0.1, same deployment
```

Sources consulted for the v1→v2 migration / contract-address context (web
search, not independently re-verified byte-for-byte — flag for
re-verification before any code depends on the exact addresses):
[Polymarket/ctf-exchange-v2 (GitHub)](https://github.com/Polymarket/ctf-exchange-v2),
[docs.polymarket.com/market-data/subgraph](https://docs.polymarket.com/market-data/subgraph),
[NegRiskCtfExchange on PolygonScan](https://polygonscan.com/address/0xc5d563a36ae78145c45a50134d48a1215220f80a),
[Neg Risk CTF Exchange V2 on PolygonScan](https://polygonscan.com/address/0xe2222d279d744050d28e00520010520000310F59).
