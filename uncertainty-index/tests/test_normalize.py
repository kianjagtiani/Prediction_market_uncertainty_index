import pandas as pd

from uindex import normalize


def test_sports_by_keyword_and_by_venue_category():
    assert normalize.categorize("Will the Chiefs win the Super Bowl?", "") == "SPORTS"
    assert normalize.categorize("Who wins game 7?", "Sports") == "SPORTS"


def test_keyword_rules_first_match_wins():
    assert normalize.categorize("Will Russia and Ukraine sign a ceasefire?", "Politics") == "WAR"
    assert normalize.categorize("Will the Fed cut rates in September?", "") == "ECON_FED"
    assert normalize.categorize("Will Bitcoin close above $150k?", "") == "CRYPTO"


def test_venue_category_fallback_and_unmapped():
    assert normalize.categorize("Something opaque", "Economics") == "ECON_FED"
    assert normalize.categorize("Something opaque", "Mystery") == "UNMAPPED"


def _tiny_inputs():
    pm_meta = pd.DataFrame({
        "market_id": ["pm_1", "pm_2"],
        "venue": ["polymarket"] * 2,
        "question": ["Will the Fed cut rates?", "Will the Lakers win?"],
        "venue_category": ["", "Sports"],
        "yes_token_id": ["a", "b"],
        "total_volume_usd": [100000.0, 500000.0],
        "open_date": pd.to_datetime(["2024-01-01"] * 2),
        "close_date": pd.to_datetime(["2024-06-01"] * 2),
    })
    pm_prices = pd.DataFrame({
        "market_id": ["pm_1", "pm_1", "pm_2"],
        "date": pd.to_datetime(["2024-02-01", "2024-02-02", "2024-02-01"]),
        "close_prob": [0.4, 0.45, 0.7],
    })
    ka_meta = pd.DataFrame({
        "market_id": ["ka_X"], "venue": ["kalshi"],
        "question": ["Will CPI exceed 3%?"], "venue_category": ["Economics"],
        "event_ticker": ["CPI-24"], "ticker": ["CPI-24-T3"],
        "series_ticker": ["CPI"], "total_volume_usd": [20000.0],
        "open_date": pd.to_datetime(["2024-01-01"]),
        "close_date": pd.to_datetime(["2024-06-01"]),
    })
    ka_prices = pd.DataFrame({
        "market_id": ["ka_X"], "date": pd.to_datetime(["2024-02-01"]),
        "close_prob": [0.3], "daily_notional_usd": [1500.0],
    })
    return pm_meta, pm_prices, ka_meta, ka_prices


def test_build_panel_drops_sports_and_unifies():
    meta, panel, dropped = normalize.build_panel(*_tiny_inputs())
    assert set(meta["market_id"]) == {"pm_1", "ka_X"}  # Lakers dropped
    assert set(meta.columns) == {
        "market_id", "venue", "question", "category", "event_ticker",
        "total_volume_usd", "open_date", "close_date",
    }
    assert set(panel["market_id"]) == {"pm_1", "ka_X"}
    pm_rows = panel[panel["market_id"] == "pm_1"]
    assert pm_rows["daily_notional_usd"].isna().all()  # no pm daily volume
