import json
from pathlib import Path

import pandas as pd

from uindex.ingest import polymarket as pm

FIXTURES = Path(__file__).parent / "fixtures"


def _markets():
    return json.loads((FIXTURES / "pm_markets.json").read_text())


def test_markets_to_df_schema():
    df = pm.markets_to_df(_markets())
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category",
        "yes_token_id", "total_volume_usd", "open_date", "close_date",
    ]
    assert (df["venue"] == "polymarket").all()
    assert df["market_id"].str.startswith("pm_").all()
    assert pd.api.types.is_datetime64_any_dtype(df["close_date"])
    assert df["close_date"].dt.tz is None


def test_markets_without_tokens_are_dropped():
    broken = _markets() + [{"id": "999", "question": "no tokens", "clobTokenIds": "[]"}]
    df = pm.markets_to_df(broken)
    assert "pm_999" not in set(df["market_id"])


def test_history_to_df_daily_close():
    payload = json.loads((FIXTURES / "pm_prices.json").read_text())
    df = pm.history_to_df(payload, market_id="pm_1")
    assert list(df.columns) == ["market_id", "date", "close_prob"]
    assert df["date"].is_monotonic_increasing
    assert not df["date"].duplicated().any()  # one close per day
    assert df["close_prob"].between(0, 1).all()
