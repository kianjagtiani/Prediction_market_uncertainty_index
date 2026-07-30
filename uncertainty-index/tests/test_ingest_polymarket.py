import json
from pathlib import Path

import pandas as pd

from uindex.ingest import polymarket as pm
from uindex.ingest.store import MetaStore

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


def _api_market(mid, volume=100_000.0):
    return {"id": mid, "question": f"q{mid}", "clobTokenIds": f'["{mid}00"]',
            "volumeNum": volume}


class _FakeKeysetClient:
    """Serves pages keyed by cursor, recording each request's params."""

    def __init__(self, pages=None):
        self.calls = []
        self._pages = pages or {
            None: {"markets": [_api_market("1"), _api_market("2")],
                   "next_cursor": "abc"},
            "abc": {"markets": [_api_market("3")], "next_cursor": None},
        }

    def get(self, url, params):
        assert url == pm.GAMMA_KEYSET_URL
        self.calls.append(params)
        return _FakeResponse(self._pages[params.get("cursor")])


def test_crawl_markets_writes_final_parquet(monkeypatch, tmp_path):
    # Pages stream to disk shards, never a whole-catalog list: accumulating
    # the full catalog previously drove ingestion to multi-GB RSS.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    client = _FakeKeysetClient()
    store = MetaStore(tmp_path)
    assert pm.crawl_markets(client, store) is True

    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert list(df.columns) == pm.MARKETS_COLUMNS
    assert list(df["market_id"]) == ["pm_1", "pm_2", "pm_3"]
    assert "cursor" not in client.calls[0]  # first page has no cursor
    assert client.calls[1]["cursor"] == "abc"
    assert not (tmp_path / "markets_cursor.json").exists()


def test_crawl_markets_portion_resumes_from_saved_cursor(monkeypatch, tmp_path):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    store = MetaStore(tmp_path)
    assert pm.crawl_markets(_FakeKeysetClient(), store, max_pages=1) is False
    assert not store.complete

    client = _FakeKeysetClient()
    assert pm.crawl_markets(client, MetaStore(tmp_path)) is True
    assert client.calls[0]["cursor"] == "abc"  # no page refetched
    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert list(df["market_id"]) == ["pm_1", "pm_2", "pm_3"]


def test_crawl_markets_keep_filter_drops_dead_markets(monkeypatch, tmp_path):
    # Metadata far below the universe volume floor can never enter any
    # index; floor/slack headroom is kept for robustness sweeps.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    client = _FakeKeysetClient({
        None: {"markets": [_api_market("1", volume=10.0), _api_market("2")],
               "next_cursor": None},
    })
    assert pm.crawl_markets(client, MetaStore(tmp_path)) is True
    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert list(df["market_id"]) == ["pm_2"]


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
