import json
from pathlib import Path

import httpx
import pandas as pd
import pytest

from uindex.ingest import polymarket as pm
from uindex.ingest.store import MetaStore, PriceStore

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


def test_history_todo_skips_done_and_below_slack_floor():
    from uindex import config
    # Fetch extends to floor/slack so robustness sweeps that lower the
    # floor never force a re-crawl; only provably-never-eligible skipped.
    slack = config.POLYMARKET_MIN_TOTAL_VOLUME_USD / config.METADATA_VOLUME_SLACK
    meta = pd.DataFrame({
        "market_id": ["pm_1", "pm_2", "pm_3"],
        "total_volume_usd": [slack, slack - 1, slack * 10],
    })
    todo = pm.history_todo(meta, done={"pm_3"})
    assert list(todo["market_id"]) == ["pm_1"]


def test_history_to_df_float_dtype_for_integral_history():
    # Resolved markets return JSON 0/1 ints; an int64 shard would wedge the
    # schema-locked stream merge.
    payload = {"history": [{"t": 1704067200, "p": 1}, {"t": 1704153600, "p": 0}]}
    df = pm.history_to_df(payload, market_id="pm_1")
    assert df["close_prob"].dtype == "float64"


def _portion_env(monkeypatch, tmp_path, statuses):
    """Wire main() to tmp dirs with scripted per-market HTTP outcomes."""
    monkeypatch.setattr(pm, "OUT_DIR", tmp_path)
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    pd.DataFrame({
        "market_id": [f"pm_{i}" for i in range(len(statuses))],
        "venue": "polymarket", "question": "q", "venue_category": "",
        "yes_token_id": [str(i) for i in range(len(statuses))],
        "total_volume_usd": 100_000.0,
        "open_date": pd.Timestamp("2024-01-01"),
        "close_date": pd.Timestamp("2024-06-01"),
    }).to_parquet(tmp_path / "markets.parquet", index=False)

    def fetch(client, token_id):
        status = statuses[int(token_id)]
        if status == 200:
            return {"history": [{"t": 1704067200, "p": 0.4}]}
        if status == "empty":
            return {"history": []}
        req = httpx.Request("GET", pm.HISTORY_URL)
        raise httpx.HTTPStatusError(
            "err", request=req, response=httpx.Response(status, request=req))

    monkeypatch.setattr(pm, "fetch_history", fetch)


def test_main_tombstones_404_and_empty_but_not_transient(monkeypatch, tmp_path):
    _portion_env(monkeypatch, tmp_path, {0: 200, 1: 404, 2: "empty"})
    assert pm.main() is True
    assert PriceStore(tmp_path).done_ids() == {"pm_0", "pm_1", "pm_2"}
    ledger = set((tmp_path / "no_data_ids.txt").read_text().split())
    assert ledger == {"pm_1", "pm_2"}  # 404 and no-data are permanent


def test_main_raises_on_transient_http_error(monkeypatch, tmp_path):
    # 429/5xx must crash the portion for retry, never tombstone the market.
    _portion_env(monkeypatch, tmp_path, {0: 429})
    with pytest.raises(httpx.HTTPStatusError):
        pm.main()
    assert not (tmp_path / "no_data_ids.txt").exists()


def test_main_portions_and_finalizes_on_last(monkeypatch, tmp_path):
    _portion_env(monkeypatch, tmp_path, {0: 200, 1: 200, 2: 200})
    assert pm.main(markets=2) is False
    assert pm.main(markets=2) is True  # drains the last market, finalizes
    final = pd.read_parquet(tmp_path / "prices.parquet")
    assert set(final["market_id"]) == {"pm_0", "pm_1", "pm_2"}
    assert not (tmp_path / "prices_parts").exists()


def test_history_to_df_daily_close():
    payload = json.loads((FIXTURES / "pm_prices.json").read_text())
    df = pm.history_to_df(payload, market_id="pm_1")
    assert list(df.columns) == ["market_id", "date", "close_prob"]
    assert df["date"].is_monotonic_increasing
    assert not df["date"].duplicated().any()  # one close per day
    assert df["close_prob"].between(0, 1).all()
