from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

BACKFILL_START = "2024-01-01"

# Index math
CLIP_LO, CLIP_HI = 0.01, 0.99
EWMA_HALFLIFE_DAYS = 10
EWMA_MIN_PERIODS = 5
SEED_DAYS = 90

# Point-in-time universe rules
RESOLUTION_EXCLUSION_DAYS = 3
POLYMARKET_MIN_ROLLING_NOTIONAL_USD = 5_000  # symmetric with Kalshi
KALSHI_MIN_ROLLING_NOTIONAL_USD = 5_000
ROLLING_WINDOW_DAYS = 7
PIN_CONSECUTIVE_DAYS = 5  # observed pinned days before exclusion (causal)
# Pin band. Deliberately NOT CLIP_LO/CLIP_HI even though the defaults match:
# the clip band is a numerical-stability bound on the index math, the pin band
# is an economic judgement about "this market has settled". Perturbing one
# must not move the other.
PIN_LO, PIN_HI = 0.01, 0.99

# Ingestion fetch bound only (not a universe rule): a 7-day rolling mean
# of $5k implies >= $35k lifetime, so lifetime/slack is a lossless
# prefilter for any configurable rolling floor.
POLYMARKET_MIN_TOTAL_VOLUME_USD = 50_000

# Ingestion portioning (see scripts/run_backfill.sh)
METADATA_VOLUME_SLACK = 10  # keep metadata down to floor/10 for robustness sweeps
INGEST_FLUSH_PAGES = 50     # metadata pages held in RAM between shard writes

# Polymarket ingestion
POLYMARKET_SLEEP_S = 0.3  # adjust per Task 2 rate-limit findings
POLYMARKET_PAGE_SIZE = 500
POLYMARKET_HISTORY_FIDELITY = 1440

# Kalshi ingestion
KALSHI_SLEEP_S = 0.15  # adjust per Task 2 rate-limit findings
KALSHI_PAGE_SIZE = 1000
KALSHI_CANDLE_PERIOD_INTERVAL_MINUTES = 1440

# Polymarket PIT daily volume (public Goldsky orderbook subgraph; see
# docs/research/polymarket-pit-volume-probe.md)
GOLDSKY_URL = ("https://api.goldsky.com/api/public/"
               "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
               "polymarket-orderbook-resync/prod/gn")
GOLDSKY_PAGE_SIZE = 1000
GOLDSKY_SLEEP_S = 0.2
# Sweep-completion guards. The sweep is only "exhausted" when a page comes
# back empty AND the cursor has caught up to roughly now; a degraded store,
# a rate-limit truncation or a reindexing lag otherwise finalizes a partial
# sweep that is indistinguishable from a complete one on disk. The lag bound
# is generous (Polymarket fills are near-continuous, so a genuinely complete
# sweep lands within seconds) but a truncated one is days or months behind.
GOLDSKY_MAX_CURSOR_LAG_S = 6 * 3600
GOLDSKY_MIN_FILLS = 100_000
# The orderbook subgraph does not index negRisk (most election markets) or
# legacy AMM fills: pm_559700 reconciles $0 subgraph against $85k Gamma. A
# market whose swept notional is below this fraction of its Gamma lifetime
# volume is not "quiet", it is not covered — its days stay NaN rather than
# being read as $0 traded. See data/raw/polymarket/volumes_coverage.csv.
PM_MIN_SUBGRAPH_COVERAGE = 0.5

INDEXES = ["GLOBAL", "WAR", "ELECTIONS", "POLITICS", "ECON_FED",
           "CRYPTO", "TECH_AI", "CLIMATE"]

# Categorization. Order matters: first match wins.
SPORTS_KEYWORDS = [
    "nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball",
    "baseball", "hockey", "tennis", "golf", "ufc", "boxing", "olympic",
    "world cup", "super bowl", "march madness", "f1 ", "grand prix",
    "premier league", "champions league", "playoff", "heisman",
    "world series", "stanley cup", "wimbledon", "masters tournament",
]
SPORTS_VENUE_CATEGORIES = {"sports", "sport"}

CATEGORY_RULES: dict[str, list[str]] = {
    "WAR": ["war", "ceasefire", "military", "strike on", "airstrike",
            "invasion", "invade", "missile", "nuclear", "sanction",
            "nato", "troops", "gaza", "ukraine", "russia", "iran",
            "taiwan", "hostage", "hezbollah", "houthis"],
    "ELECTIONS": ["election", "primary", "presidential race", "senate seat",
                  "senate race", "house seat", "governor", "midterm",
                  "nominee", "ballot", "electoral", "wins the presidency",
                  "popular vote"],
    "POLITICS": ["shutdown", "impeach", "supreme court", "scotus",
                 "congress", "bill", "executive order", "cabinet",
                 "resign", "speaker of the house", "confirm", "veto",
                 "pardon", "debt ceiling"],
    "ECON_FED": ["fed ", "fomc", "rate cut", "rate hike", "interest rate",
                 "cpi", "inflation", "recession", "gdp", "unemployment",
                 "jobs report", "payroll", "treasury", "tariff"],
    "CRYPTO": ["bitcoin", "btc", "ethereum", "eth ", "solana", "crypto",
               "stablecoin", "coinbase", "binance"],
    "TECH_AI": ["openai", "gpt", "claude", "anthropic", "gemini",
                "ai model", "artificial intelligence", "agi", "nvidia",
                "apple", "tesla", "spacex", "chatgpt", "deepmind"],
    "CLIMATE": ["temperature", "hottest", "hurricane", "wildfire",
                "climate", "emissions", "carbon", "heat record",
                "named storm", "el nino", "la nina"],
}

# Fallback: venue-provided category (lowercased) -> our taxonomy
VENUE_CATEGORY_MAP: dict[str, str] = {
    "politics": "POLITICS",
    "us-current-affairs": "POLITICS",
    "world": "WAR",
    "geopolitics": "WAR",
    "economics": "ECON_FED",
    "financials": "ECON_FED",
    "economy": "ECON_FED",
    "crypto": "CRYPTO",
    "science and technology": "TECH_AI",
    "tech": "TECH_AI",
    "climate and weather": "CLIMATE",
    "climate": "CLIMATE",
}

# Universe composition: which categories feed each index
INDEX_UNIVERSES: dict[str, list[str]] = {
    "GLOBAL": ["WAR", "ELECTIONS", "POLITICS", "ECON_FED",
               "CRYPTO", "TECH_AI", "CLIMATE"],
    "WAR": ["WAR"],
    "ELECTIONS": ["ELECTIONS"],
    "POLITICS": ["POLITICS"],
    "ECON_FED": ["ECON_FED"],
    "CRYPTO": ["CRYPTO"],
    "TECH_AI": ["TECH_AI"],
    "CLIMATE": ["CLIMATE"],
}
