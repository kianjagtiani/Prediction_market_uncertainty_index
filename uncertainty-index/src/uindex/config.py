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
POLYMARKET_MIN_TOTAL_VOLUME_USD = 50_000
KALSHI_MIN_ROLLING_NOTIONAL_USD = 5_000
ROLLING_WINDOW_DAYS = 7

# Polymarket ingestion
POLYMARKET_SLEEP_S = 0.3  # adjust per Task 2 rate-limit findings
POLYMARKET_PAGE_SIZE = 500
POLYMARKET_HISTORY_FIDELITY = 1440

# Kalshi ingestion
KALSHI_SLEEP_S = 0.15  # adjust per Task 2 rate-limit findings
KALSHI_PAGE_SIZE = 1000
KALSHI_CANDLE_PERIOD_INTERVAL_MINUTES = 1440

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
