# A VIX for the News: Uncertainty Indices from Prediction Markets

Independent research project, Summer 2026 \
**Author:** Kian Jagtiani (USC)

---

Can prediction-market prices be turned into a live gauge of how uncertain the world is,
the way the VIX gauges equity fear? Every open question with a market on it (Will the Fed
cut? Ceasefire by March? CPI above 3%?) carries a probability that gets repriced daily.
This project aggregates those repricings into a family of daily indices: a headline
**Global Uncertainty Index** plus themed sub-indices (war, elections, econ/Fed, crypto,
tech/AI, climate), each with two gauges. **Turbulence** measures how violently
probabilities are moving; **Unresolvedness** measures how far from decided the open
questions sit. Phase 1, this repo, is the methodology engine, the Polymarket and Kalshi
ingestion, and a validation harness, with a Jan 2024 → present backfill in progress.

## What we built, stage by stage

**Feasibility probe.** Polymarket is backfill-ready: the CLOB price-history endpoint
returns clean daily closes (a sample market gave 396 points, Dec 2023 through Jan 2025,
no gaps). Kalshi is not, for old markets: of 561 markets that closed during 2024, sampled
across 20 event series, every single one reports zero volume and an empty candlestick
history. Kalshi's usable history effectively starts with markets alive in 2025, and even
there the candles are sparse. The index weights Kalshi accordingly rather than pretending
the venue has 2024 data.

**Ingestion.** Polymarket metadata comes from the Gamma API and prices from the CLOB API;
Kalshi from its trade API v2. Polymarket's offset pagination 422s past roughly 2,400
markets, so ingestion uses the keyset-cursor endpoint instead (which silently caps pages
at 100 rows regardless of the requested limit). Raw responses land as parquet, queried
with DuckDB, and the crawl is resumable and memory-bounded after an early version fought
the laptop for RAM.

**Methodology engine.** Turbulence is logit-space realized volatility: clip the closing
probability to [0.01, 0.99], take daily logit innovations, EWMA the squared innovations
per market, then take a liquidity-weighted mean across the universe. Logit space is the
point; a two-point move at p = 0.97 is a much bigger statement than the same move at 0.50.
Unresolvedness is weighted mean binary entropy. Both are percentile-scaled to 0–100
against trailing history. The golden-day test injects a synthetic shock and the raw
turbulence jumps 16x on the day; a subtler bug surfaced here too, where NaNs inside the
expanding percentile window counted as failed comparisons and silently dragged the scaled
index down around data gaps. That fix has its own regression tests.

**Point-in-time universe rules.** The single biggest artifact to prevent is settlement:
probabilities collapsing to 0 or 1 near resolution look like turbulence but are just a
market closing its book. Markets leave the universe a fixed window before scheduled close,
and a terminal-pin guard catches early settlers (a market that jumps 0.40 → 0.995 months
ahead of its close date has its whole pinned tail dropped, settlement jump included).
Weights use log1p notional on both venues; raw Kalshi notional would outweigh a logged
Polymarket weight by three orders of magnitude and turn every mixed universe into a pure
Kalshi index. Kalshi multi-strike events (a dozen "CPI above X%" markets on one print)
collapse to their most liquid strike, and cross-venue duplicate questions count once, with
every dedup decision written to an audit CSV.

**Validation harness.** Built and unit-tested, waiting on the backfill for real numbers.
The event study requires the composite to spike (window max ≥ 90) on known chaos dates:
the Nov 2024 election week, the 2025 tariff shocks, the June 2025 Iran strikes. Benchmark
comparison runs lead-lag correlations against VIX, EPU, and GPR. A churn audit checks that
index moves don't just track membership moves, and a robustness sweep varies the EWMA
half-life, liquidity floors, and clipping bounds.

## Conclusion and next steps

The pipeline is complete and green (70 tests) but the headline question is still open;
methodology without a backfill is scaffolding. The immediate work is operational: the full
Jan 2024 → present crawl now runs on a machine with enough memory to hold it.

- Finish the two-venue backfill and freeze the raw parquet snapshot.
- Generate the Phase 1 validation report; the event study is the gate, and a miss means
  revising methodology, not the narrative.
- Annotate the top spike days with the news story that drove each one.
- Write the methodology document against the as-built parameters in `config.py`.
- Only then Phase 2: a live dashboard and custom index builder.

## Repository contents

- `uncertainty-index/src/uindex/` — the package: `ingest/` (Polymarket, Kalshi, parquet
  store), `normalize.py` (unified schema + taxonomy mapping), `universe.py` (point-in-time
  rules), `compute.py` and `pipeline.py` (the index math), `validate/` (event study,
  benchmarks, robustness, churn, report).
- `uncertainty-index/tests/` — 70 tests, including golden-day shock and byte-identical
  reproducibility checks.
- `uncertainty-index/docs/` — design spec, Phase 1 implementation plan, API probe findings.
- `uncertainty-index/scripts/` — the raw API probes behind the feasibility numbers above.
- Data provenance: `data/` is untracked and fully regenerated by the ingestion CLIs; all
  tunable parameters live in `src/uindex/config.py`; manual dedup overrides in
  `data-overrides/duplicates.csv`.

## References

The efficient-aggregation case for prediction markets is Wolfers and Zitzewitz (2004,
*Journal of Economic Perspectives*) and Arrow et al. (2008, *Science*). The two benchmark
uncertainty series come from Baker, Bloom and Davis (2016, *QJE*), the Economic Policy
Uncertainty index, and Caldara and Iacoviello (2022, *AER*), the Geopolitical Risk index.
Manski (2006, *Economics Letters*) is the standard caution on reading market prices as
probabilities.
