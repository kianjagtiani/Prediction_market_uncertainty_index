# Probe: Point-in-Time Daily Volume for Polymarket

_2026-07-29. Motivated by the quant audit's critical finding: PM floors and
weights currently use crawl-time lifetime volume (look-ahead)._

## Verdict

**PIT daily volume IS publicly obtainable — via the Goldsky Polymarket
orderbook subgraph.** Not via data-api or CLOB.

## What was ruled out

- **data-api.polymarket.com `/trades`** — no auth, but offset hard-capped at
  10,000 and all timestamp filters silently ignored; only the most recent
  ~20k trades per market are reachable (Trump-2024 has ~3.45M fills; the
  deepest page reaches 2024-11-06). Fine as a live feed, useless for history.
- **clob.polymarket.com** — `/trades` is 401 (L2 key, user-scoped);
  `/prices-history` gives full daily PIT *prices* back to inception but no
  volume.
- **gamma-api.polymarket.com** — only crawl-time rolling snapshots
  (`volume24hr/1wk/1mo/1yr`); every guessed history endpoint 404s.

## The viable source

- `POST https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/polymarket-orderbook-resync/prod/gn`
  (GraphQL, public, no key).
- Entity `enrichedOrderFilleds` `{timestamp, market (clobTokenId), side,
  size, price, ...}` supports `where: {market, timestamp_gte, timestamp_lt}`
  + `orderBy: timestamp`, 1000 rows/page, cursor via `timestamp_gt`/`id_gt`;
  `skip: 6000` worked (no 5000-skip cap observed).
- History is complete (on-chain indexed): earliest Trump-2024 fill
  2024-01-05, matching inception. Platform totals via `ordersMatchedGlobals`:
  ~108M matches, $37.8B collateral volume, 268k conditions.
- Reconciliation spot-check: subgraph token volumes summed to 96% of Gamma's
  lifetime volume on a small closed market (definitional gap — maker/taker
  attribution or AMM component — to pin down during implementation).
- No throttling observed at 15 rapid requests; day-window queries ~1-2 s.

## Cost

- Per-market paging: ~1 request per 1000 fills (small markets 1-5 requests;
  Trump-2024 alone ~3.5-7k).
- Cheaper: one global timestamp-ordered sweep of all fills, bucketed to
  market x day locally — ~100-250k requests for 2024-present, one-time
  (~10-30 h at 2-5 req/s), then daily increments are trivial
  (`timestamp_gte: yesterday`). Fits the portioned/checkpointed ingestion
  pattern directly (cursor = last timestamp+id).
- Join key: `enrichedOrderFilled.market` = clobTokenId = Gamma
  `clobTokenIds`.

## Implication for the index

Polymarket can get true PIT rolling-notional weights and floors — same
construction as Kalshi's — eliminating the look-ahead entirely instead of
disclosing it as a limitation. CLOB `/prices-history` full-depth daily
prices were also confirmed as a bonus.
