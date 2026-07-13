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


def test_markets_to_df_empty_list():
    df = pm.markets_to_df([])
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category",
        "yes_token_id", "total_volume_usd", "open_date", "close_date",
    ]
    assert len(df) == 0


def test_markets_to_df_all_filtered_out():
    broken = [{"id": "1", "question": "no tokens", "clobTokenIds": "[]"}]
    df = pm.markets_to_df(broken)
    assert list(df.columns) == [
        "market_id", "venue", "question", "venue_category",
        "yes_token_id", "total_volume_usd", "open_date", "close_date",
    ]
    assert len(df) == 0


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _FakeKeysetClient:
    """Serves two pages keyed by cursor, recording each request's params."""

    def __init__(self):
        self.calls = []
        self._pages = {
            None: {"markets": [{"id": "1"}, {"id": "2"}], "next_cursor": "abc"},
            "abc": {"markets": [{"id": "3"}], "next_cursor": None},
        }

    def get(self, url, params):
        assert url == pm.GAMMA_KEYSET_URL
        self.calls.append(params)
        return _FakeResponse(self._pages[params.get("cursor")])


def test_fetch_all_markets_follows_keyset_cursor(monkeypatch):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    client = _FakeKeysetClient()
    markets = pm.fetch_all_markets(client)
    assert [m["id"] for m in markets] == ["1", "2", "3"]
    assert "cursor" not in client.calls[0]  # first page has no cursor
    assert client.calls[1]["cursor"] == "abc"


def test_history_to_df_daily_close():
    payload = json.loads((FIXTURES / "pm_prices.json").read_text())
    df = pm.history_to_df(payload, market_id="pm_1")
    assert list(df.columns) == ["market_id", "date", "close_prob"]
    assert df["date"].is_monotonic_increasing
    assert not df["date"].duplicated().any()  # one close per day
    assert df["close_prob"].between(0, 1).all()
