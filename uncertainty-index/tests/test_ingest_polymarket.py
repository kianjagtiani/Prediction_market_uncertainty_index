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


def _api_market(mid):
    return {"id": mid, "question": f"q{mid}", "clobTokenIds": f'["{mid}00"]'}


class _FakeKeysetClient:
    """Serves pages keyed by cursor, recording each request's params."""

    def __init__(self):
        self.calls = []
        self._pages = {
            None: {"markets": [_api_market("1"), _api_market("2")],
                   "next_cursor": "abc"},
            "abc": {"markets": [_api_market("3")], "next_cursor": None},
        }

    def get(self, url, params):
        assert url == pm.GAMMA_KEYSET_URL
        self.calls.append(params)
        return _FakeResponse(self._pages[params.get("cursor")])


def test_fetch_all_markets_streams_pages_to_slim_df(monkeypatch):
    # Returns a converted DataFrame, not raw dicts: accumulating raw market
    # dicts for the full catalog previously drove ingestion to multi-GB RSS.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    client = _FakeKeysetClient()
    df = pm.fetch_all_markets(client)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == pm.MARKETS_COLUMNS
    assert list(df["market_id"]) == ["pm_1", "pm_2", "pm_3"]
    assert "cursor" not in client.calls[0]  # first page has no cursor
    assert client.calls[1]["cursor"] == "abc"


def test_history_todo_skips_done_and_below_volume_floor():
    from uindex import config
    floor = config.POLYMARKET_MIN_TOTAL_VOLUME_USD
    meta = pd.DataFrame({
        "market_id": ["pm_1", "pm_2", "pm_3"],
        # At-floor markets survive the universe filter (< floor excludes),
        # so they must still get history fetched.
        "total_volume_usd": [floor, floor - 1, floor * 10],
    })
    todo = pm.history_todo(meta, done={"pm_3"})
    assert list(todo["market_id"]) == ["pm_1"]


def test_history_to_df_daily_close():
    payload = json.loads((FIXTURES / "pm_prices.json").read_text())
    df = pm.history_to_df(payload, market_id="pm_1")
    assert list(df.columns) == ["market_id", "date", "close_prob"]
    assert df["date"].is_monotonic_increasing
    assert not df["date"].duplicated().any()  # one close per day
    assert df["close_prob"].between(0, 1).all()
