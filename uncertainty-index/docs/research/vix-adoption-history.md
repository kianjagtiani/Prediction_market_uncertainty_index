# From Idea to Benchmark: The Full History of the VIX — and the Playbook It Reveals

_Research notes, 2026-07-29. All dates and citations verified via web search; unverified items flagged at the end._

## Short answer to the framing question

Neither pure academic diffusion nor pure industry push. The pattern was **academics proposed, an exchange commissioned an academic to build it, the exchange gave it away free daily for a decade, media made it famous, and only then was it re-engineered (with a bank's quant desk) to be tradable**. The methodology paper and the launch were tightly coupled but the *launch came first* — the paper was the credibility layer published months later.

## Timeline

| Year | Event |
|---|---|
| 1977 | Gastineau proposes an index of option premiums (Financial Analysts Journal) |
| 1986–89 | Brenner & Galai propose the "Sigma Index"; published FAJ Jul/Aug 1989 |
| Late 1992 | CBOE commissions Whaley (on sabbatical from Duke) to build the index |
| **Jan 19, 1993** | CBOE press conference launches real-time VIX dissemination |
| Fall 1993 | Whaley's methodology paper appears in the *inaugural issue* of Journal of Derivatives |
| 1993–2003 | VIX quoted daily, untradable; becomes the media "fear gauge" (Whaley formalizes the term in a 2000 JPM paper) |
| Mar 1999 | Goldman Sachs research note on variance swaps (Demeterfi–Derman–Kamal–Zou) |
| 2000 | Britten-Jones & Neuberger publish model-free implied volatility in Journal of Finance |
| **Sep 2003** | CBOE + Goldman redesign: model-free, SPX, all strikes; old index preserved as VXO |
| **Mar 26, 2004** | VIX futures — first product on the new CBOE Futures Exchange |
| **Feb 24, 2006** | VIX options launch |
| **Jan 29, 2009** | Barclays launches VXX ETN; ETP ecosystem follows |
| **Feb 5, 2018** | "Volmageddon": VIX +115.6% in a day; Credit Suisse's XIV loses ~96%, terminated |

## 1. Academic prehistory (1977–1989)

- **Gastineau (1977), "An Index of Listed Option Premiums," Financial Analysts Journal 33(3), 70–75.** First proposal of a continuously updated index of average option premium levels.
- **Brenner & Galai (1989), "New Financial Instruments for Hedging Changes in Volatility," Financial Analysts Journal, July/August 1989.** Proposed a frequently updated volatility index — the "Sigma Index" — explicitly *as an underlying for futures and options*: the stated problem was that volatility risk was unhedgeable. The idea dates to a 1986 working paper.

Key point: the academics' motivation was *tradability from day one* — but it took 15+ years and two methodology generations to get there.

## 2. The 1993 launch: commission first, paper second

CBOE hired **Robert Whaley** (Duke, late 1992) with the exchange's full index-option price history to construct the index. The original VIX: Black–Scholes-style implied volatility from at-the-money S&P 100 (OEX) options, 30-day horizon. Launch: press conference, January 19, 1993, with real-time dissemination.

The paper — **Whaley (1993), "Derivatives on Market Volatility: Hedging Tools Long Overdue," Journal of Derivatives 1(1), 71–84** — appeared in Fall 1993, *after* the launch, in the journal's first-ever issue. So the coupling was: exchange commissions academic → index launches → methodology published openly months later. The title itself argues the Brenner–Galai case: the point of the index was to enable derivatives, which then took 11 more years.

## 3. The untradable decade (1993–2003)

No product existed, but the number was free, daily, real-time, and had a clean narrative: it spikes when markets fall. Media adoption did the work — "the fear gauge" became the standard shorthand, and Whaley canonized it academically in **"The Investor Fear Gauge," Journal of Portfolio Management 26(3), Spring 2000, 12–17**. By 2003 the VIX was already the benchmark *before* it was investable — a crucial sequencing fact.

## 4. The 2003 redesign: replicability made it tradable

The original VIX was model-dependent and couldn't be statically replicated, so a futures contract on it would have no clean hedge. The fix came from the variance-swap literature:

- **Demeterfi, Derman, Kamal & Zou, "More Than You Ever Wanted to Know About Volatility Swaps," Goldman Sachs Quantitative Strategies Research Notes, March 1999** (cited in Cboe's own methodology document).
- **Britten-Jones & Neuberger (2000), "Option Prices, Implied Price Processes, and Stochastic Volatility," Journal of Finance 55, 839–866** — model-free implied volatility.
- **Carr & Madan, "Towards a Theory of Volatility Trading"** (verified via the Cambridge reprint; original 1998 Risk Books details unverified).

In September 2003 CBOE, working with Goldman Sachs, switched VIX to a model-free weighted strip of SPX options across all strikes — effectively the square root of a 30-day variance swap rate. Because dealers could now replicate the exposure with a static options portfolio, market makers could hedge, so derivatives became viable. The old index survives as VXO; the redesign deliberately preserved the VIX ticker and its accumulated brand equity and history.

## 5. Productization and the ecosystem flywheel

- VIX futures, March 26, 2004 — the first product on the newly created CBOE Futures Exchange (built partly *for* this product).
- VIX options, February 24, 2006.
- VXX ETN, January 29, 2009 (Barclays iPath), opening retail access; dozens of ETPs followed. CBOE's licensing of the patented methodology (USPTO 8,249,972) became a real revenue line, and Cboe exported the method (VXN, VXD, OVX, international clones).
- Flywheel: media quotes → benchmark status → products → hedging demand → more quoting → more products.
- Cautionary tail — Feb 5, 2018 "Volmageddon": VIX rose 115.6% (17.31 → 37.32), the levered/inverse ETP rebalancing loop amplified the move, Credit Suisse's XIV (recently $1.9B AUM) lost ~96% and was terminated. The ecosystem can grow large enough to feed back into the index itself.

## 6. The extracted playbook

**Sequence:** academic proposal → exchange commissions academic → simultaneous launch + free real-time dissemination + published methodology paper → media adoption / memorable framing → redesign for replicability once tradability is the goal → derivatives → retail ETPs + licensing.

**Parallel example — EPU (pure academic route):** Baker, Bloom & Davis, "Measuring Economic Policy Uncertainty," QJE 131(4), 2016, 1593–1636. Circulated as a working paper from ~2011–12 with a free public website (policyuncertainty.com), transparent newspaper-count methodology, human-audit validation, and country extensions. Bloomberg, FRED, Haver, and Reuters now carry the EPU indices. EPU proves you can reach benchmark status without an exchange — but it never became tradable; VIX shows what the exchange/replicability route adds.

## 7. Practical implications for a prediction-market uncertainty index

1. **Free, daily, real-time publication beats everything else.** VIX was a public good for 11 years before it made money. Distribution first, monetization later.
2. **Publish the methodology paper — but don't wait for it to launch.** Whaley's paper followed the launch; the index and paper co-brand each other. Journal of Derivatives is the exact precedent.
3. **A memorable frame matters as much as the math.** "Fear gauge" did more for adoption than the formula. This index needs a two-word media handle.
4. **An anchor institution provides credibility and plumbing.** CBOE for VIX; Stanford/Chicago/NBER + policyuncertainty.com for EPU. A university affiliation plus a data-vendor listing (FRED/Bloomberg carriage, as EPU did) is the academic-route equivalent.
5. **Design for auditability and replicability from day one** — rule-based, computable by third parties from public prediction-market prices. This is what let VIX v2 support derivatives and what let EPU survive scrutiny. Backcast a long history: CBOE backcast VIX so it "existed" during the 1987 crash — the historical spikes *are* the marketing.
6. **Version the methodology, keep the ticker.** The 2003 rebuild kept the VIX name and history; brand continuity through methodology changes is deliberate.
7. **Tradability is a later, optional stage** — and it imports reflexivity risk (Volmageddon). For a research-stage index, the EPU path (paper + free data + vendor carriage) is the realistic v1 target; the VIX path is the v2 option.

## Unverified / flagged

- Exact original publication details of Carr & Madan 1998 (Risk Books volume) — title confirmed via the Cambridge reprint only.
- The precise date EPU first appeared on Bloomberg terminals (vendor carriage confirmed by the authors; no launch date verifiable).
