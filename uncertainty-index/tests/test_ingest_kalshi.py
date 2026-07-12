import json
from pathlib import Path

import pandas as pd

from uindex.ingest import kalshi

FIXTURES = Path(__file__).parent / "fixtures"


def test_markets_to_df_schema():
    payload = json.loads((FIXTURES / "kalshi_markets.json").read_text())
    df = kalshi.markets_to_df(payload["markets"])
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category", "event_ticker",
        "ticker", "series_ticker", "total_volume_usd", "open_date", "close_date",
    ]
    assert (df["venue"] == "kalshi").all()
    assert df["market_id"].str.startswith("ka_").all()
    assert pd.api.types.is_datetime64_any_dtype(df["close_date"])


def test_candles_to_df_prob_and_notional():
    payload = json.loads((FIXTURES / "kalshi_candles.json").read_text())
    df = kalshi.candles_to_df(payload, market_id="ka_TEST")
    assert list(df.columns) == ["market_id", "date", "close_prob", "daily_notional_usd"]
    assert df["close_prob"].between(0, 1).all()  # cents converted
    assert (df["daily_notional_usd"] >= 0).all()
    assert not df["date"].duplicated().any()
