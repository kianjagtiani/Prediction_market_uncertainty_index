# Phase 2 — Public Site & Data Product Design

**Date:** 2026-07-29
**Status:** Approved in discussion; queued behind the Phase 1 validation gate
**Depends on:** Completed backfill + validation report with a passing event study. No public launch on a number that failed its own validation.

## Product decision

Option A of the Phase 2 discussion: the index as a **data product** — one canonical
daily artifact, with every surface (site, future bot, widgets, any venue overlay)
a thin renderer of it. No backend, no database, no accounts. Explicitly rejected
for MVP: full web app with custom index builder (upgrade path if traction),
browser extension overlay (later distribution experiment; needs this data feed
anyway).

**Name/domain:** deliberately deferred — a naming pass happens before launch.
Nothing below depends on the name. ("Site" and "data repo" are placeholders.)

**Audiences and their entry points:** curious public + journalists (home page
headline), traders (per-index constituent drill-down, deep links to venues),
researchers (/data downloads, vintages, methodology).

## Architecture

```
daily cron (GitHub Actions)
  incremental ingest (top-up since last snapshot)   [new, small]
  -> normalize -> universes -> compute              [exists, unchanged]
  -> uindex.publish                                 [new]
  -> commit artifacts to public data repo + deploy static site to CDN
```

- **Incremental ingest:** a top-up mode for both venue modules — crawl markets
  with activity since the stored snapshot, fetch missing price days. The
  portioned/checkpointed ingestion (cursor state, no-data ledger, part-file
  merges) is already shaped for this. Refresh policy: daily, re-crawl metadata
  for markets closing within the trailing 30 days plus all current
  constituents (their volumes drift, and weights depend on them); monthly,
  a full metadata re-crawl to catch anything the window missed.
- **`uindex.publish`:** the only substantial new Python. Reads
  `indices.parquet` + `constituents.parquet`, emits:
  - `latest.json` — today's value and delta for all 8 indices x 2 gauges,
    plus top-5 constituent contributions per index (question, venue, weight,
    contribution, venue URL).
  - `series/<INDEX>.csv` — full daily history per index, both gauges.
  - `constituents/<INDEX>.json` — current membership table for drill-downs.
  Writes to a temp dir; deploy is atomic (same discipline as the ingest stores).
- **Data repo:** artifacts committed daily to a public repo. Git history =
  point-in-time vintages for free (EPU/GPR distribution model). CDN-served
  static JSON/CSV URLs are the public API; no server.
- **Site:** static HTML/JS reading those artifacts. GitHub Pages or Cloudflare
  Pages. Compute code, data repo, and site version independently.

## Pages

1. **Home** — GLOBAL turbulence as headline numeral + 90-day sparkline; grid of
   eight sub-index sparklines; "What moved today": top contributions in plain
   English, deep-linked to the live Kalshi/Polymarket markets. Unresolvedness
   visually secondary.
2. **/index/<NAME>** — full-history chart (gauge toggle); constituents table
   (question, venue, weight, today's contribution), every row linking out to
   the venue. Trader layer + the auditability story.
3. **/data** — CSV downloads, JSON URL docs, vintage explanation, citation block.
4. **/methodology** — Task 12 methodology doc rendered to HTML: logit-space RV,
   entropy, PIT rules, parameters. Auditability is the moat vs black-box scores.

## Failure behavior

Daily job failure: site keeps serving the last good day with a visible
"as of <date>" stamp; GitHub Actions notifies on failure. No partial publishes.

## Testing

- Unit tests for `publish`: JSON schema stability, contribution math,
  venue deep-link construction.
- Golden-file test pinning the JSON contract the site depends on.
- CI smoke test building the site against fixture JSON.

## Fast-follows (not MVP)

X/Twitter bot posting the daily number + top mover from `latest.json`
(~50 lines); embeddable sparkline widget; alerts. Custom index builder and
venue overlay remain future options gated on traction.
