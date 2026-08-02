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
        return _FakeResponse(self._pages[params.get("after_cursor")])


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
    assert "after_cursor" not in client.calls[0]  # first page bare
    assert client.calls[1]["after_cursor"] == "abc"
    assert not (tmp_path / "markets_cursor.json").exists()


def test_crawl_markets_portion_resumes_from_saved_cursor(monkeypatch, tmp_path):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    store = MetaStore(tmp_path)
    assert pm.crawl_markets(_FakeKeysetClient(), store, max_pages=1) is False
    assert not store.complete

    client = _FakeKeysetClient()
    assert pm.crawl_markets(client, MetaStore(tmp_path)) is True
    assert client.calls[0]["after_cursor"] == "abc"  # no page refetched
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


def test_crawl_markets_raises_when_pagination_stuck(monkeypatch, tmp_path):
    # Gamma silently ignores unknown params: a wrong cursor name replays
    # page 1 forever with a fresh next_cursor. Fail fast, not at 1.8M rows.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    page = {"markets": [_api_market("1")], "next_cursor": "always-new"}
    client = _FakeKeysetClient({None: page, "always-new": page})
    with pytest.raises(RuntimeError, match="pagination stuck"):
        pm.crawl_markets(client, MetaStore(tmp_path))


class _FakeMarketsClient:
    """Serves /markets?id=...&closed=...&limit=... from a scripted
    {id: (market, closed)} map; a request only returns markets whose
    tagged closed-state matches. Validates the limit fetch_legacy_batch is
    required to send (len(ids) + 1) unless disabled for a dedicated test."""

    def __init__(self, markets=None, check_limit=True):
        self.calls = []
        self._markets = markets or {}  # id (int) -> (api_market_dict, "true"|"false")
        self._check_limit = check_limit

    def get(self, url, params):
        assert url == pm.GAMMA_MARKETS_URL
        self.calls.append(params)
        assert len(params["id"]) <= pm.LEGACY_BATCH
        assert params["end_date_min"] == pm.config.BACKFILL_START
        if self._check_limit:
            assert params["limit"] == len(params["id"]) + 1
        closed = params["closed"]
        found = [m for i in params["id"]
                if (entry := self._markets.get(i)) and entry[1] == closed
                for m in [entry[0]]]
        return _FakeResponse(found)


def test_fetch_legacy_batch_unions_both_closed_states(monkeypatch):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    client = _FakeMarketsClient({
        1: (_api_market("1"), "true"),
        2: (_api_market("2"), "false"),
    })
    out = pm.fetch_legacy_batch(client, [1, 2, 3])
    assert {m["id"] for m in out} == {"1", "2"}
    assert [c["closed"] for c in client.calls] == ["true", "false"]
    assert all(c["limit"] == 4 for c in client.calls)  # len([1,2,3]) + 1


def test_fetch_legacy_batch_dedups_id_seen_in_both_responses(monkeypatch):
    # Defensive: a market shouldn't be both closed and open, but the union
    # must not double-count it if it somehow is.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    m = _api_market("1")
    client = _FakeMarketsClient({1: (m, "true")})
    # Force both passes to return the same market regardless of closed state.
    monkeypatch.setattr(client, "get",
                        lambda url, params: _FakeResponse([m]))
    out = pm.fetch_legacy_batch(client, [1])
    assert len(out) == 1


def test_fetch_legacy_batch_empty_ids_raises(monkeypatch):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    with pytest.raises(ValueError, match="empty"):
        pm.fetch_legacy_batch(_FakeMarketsClient(), [])


def test_fetch_legacy_batch_raises_on_probable_truncation(monkeypatch):
    # A response naming as many markets as the id set itself (or more) can
    # only mean the server truncated it, since a K-id query can match at
    # most K real markets -- this must never be silently trusted.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    ids = [1, 2, 3]

    class _TruncatingClient:
        def get(self, url, params):
            # Returns exactly `limit` markets regardless of which ids were
            # asked for, simulating a server-side page cap.
            return _FakeResponse([_api_market(str(i))
                                  for i in range(params["limit"])])

    with pytest.raises(RuntimeError, match="truncation"):
        pm.fetch_legacy_batch(_TruncatingClient(), ids)


def test_crawl_legacy_markets_sweeps_full_range_and_finalizes(monkeypatch, tmp_path):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 5)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 2)
    client = _FakeMarketsClient({
        1: (_api_market("1"), "true"),
        3: (_api_market("3"), "true"),
        5: (_api_market("5"), "true"),
    })
    store = MetaStore(tmp_path)
    assert pm.crawl_legacy_markets(client, store) is True

    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert set(df["market_id"]) == {"pm_1", "pm_3", "pm_5"}
    assert not (tmp_path / "markets_cursor.json").exists()
    # 3 batches (1-2, 3-4, 5) x 2 closed states each.
    assert len(client.calls) == 6
    assert max(i for c in client.calls for i in c["id"]) == 5


def test_crawl_legacy_markets_no_redundant_loop_level_throttle(monkeypatch, tmp_path):
    # fetch_legacy_batch already sleeps after each of its 2 requests; a
    # third, loop-level sleep per batch would silently double the gap
    # between batches without adding real throttling.
    sleeps = []
    monkeypatch.setattr(pm.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 6)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 2)
    store = MetaStore(tmp_path)
    assert pm.crawl_legacy_markets(_FakeMarketsClient({}), store) is True
    # 3 batches x 2 Gamma requests/batch = 6 sleeps, not 9.
    assert len(sleeps) == 6


def test_crawl_legacy_markets_all_empty_batches_still_progress(monkeypatch, tmp_path):
    # Most of the id space is sparse: a batch with zero hits must not be
    # mistaken for "catalog exhausted" (that rule is right for keyset,
    # wrong here -- see crawl_legacy_markets docstring).
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 6)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 2)
    client = _FakeMarketsClient({6: (_api_market("6"), "true")})
    store = MetaStore(tmp_path)
    assert pm.crawl_legacy_markets(client, store) is True
    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert list(df["market_id"]) == ["pm_6"]
    assert len(client.calls) == 6  # 3 batches x 2 closed states


def test_crawl_legacy_markets_watermark_resume(monkeypatch, tmp_path):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 6)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 2)
    markets = {
        1: (_api_market("1"), "true"),
        3: (_api_market("3"), "true"),
        5: (_api_market("5"), "true"),
    }
    store = MetaStore(tmp_path)
    assert pm.crawl_legacy_markets(_FakeMarketsClient(markets), store,
                                   max_batches=1) is False
    assert not store.complete

    client = _FakeMarketsClient(markets)
    assert pm.crawl_legacy_markets(client, MetaStore(tmp_path)) is True
    # ids 1-2 already swept; resume must start at 3, not refetch 1-2.
    assert all(i >= 3 for c in client.calls for i in c["id"])
    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert set(df["market_id"]) == {"pm_1", "pm_3", "pm_5"}


def test_crawl_legacy_markets_never_requests_past_max_id(monkeypatch, tmp_path):
    # LEGACY_MAX_ID not a multiple of LEGACY_BATCH: the last batch must be
    # short, never spilling into keyset's id space.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 5)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 3)
    client = _FakeMarketsClient({})
    store = MetaStore(tmp_path)
    assert pm.crawl_legacy_markets(client, store) is True
    assert max(i for c in client.calls for i in c["id"]) == 5


def test_crawl_legacy_markets_keep_filter_drops_dead_markets(monkeypatch, tmp_path):
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 2)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 2)
    client = _FakeMarketsClient({
        1: (_api_market("1", volume=10.0), "true"),
        2: (_api_market("2"), "true"),
    })
    store = MetaStore(tmp_path)
    assert pm.crawl_legacy_markets(client, store) is True
    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert list(df["market_id"]) == ["pm_2"]


def test_crawl_legacy_markets_defensive_guard_on_out_of_range_cursor(
        monkeypatch, tmp_path):
    # A resume cursor already beyond LEGACY_MAX_ID (corrupted state, or a
    # LEGACY_MAX_ID lowered after the fact) must finalize as "done" rather
    # than build an empty id list and send an unfiltered /markets query.
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 5)
    store = MetaStore(tmp_path)
    store.commit(pd.DataFrame(columns=pm.MARKETS_COLUMNS), cursor=10, n_seen=0)

    client = _FakeMarketsClient({})
    assert pm.crawl_legacy_markets(client, store) is True
    assert client.calls == []
    assert store.complete


def test_verify_legacy_completeness_passes_when_floor_and_spot_check_ok(
        monkeypatch, tmp_path):
    monkeypatch.setattr(pm, "LEGACY_MIN_KEPT_MARKETS", 1)
    monkeypatch.setattr(pm, "LEGACY_SPOT_CHECK_IDS", [1])
    pd.DataFrame({"market_id": ["pm_1"]}).to_parquet(
        tmp_path / "markets.parquet", index=False)
    client = _FakeMarketsClient({1: (_api_market("1"), "true")})
    pm.verify_legacy_completeness(client, MetaStore(tmp_path))  # no raise


def test_verify_legacy_completeness_raises_below_row_floor(monkeypatch, tmp_path):
    monkeypatch.setattr(pm, "LEGACY_MIN_KEPT_MARKETS", 100)
    pd.DataFrame({"market_id": ["pm_1"]}).to_parquet(
        tmp_path / "markets.parquet", index=False)
    with pytest.raises(RuntimeError, match="incomplete"):
        pm.verify_legacy_completeness(_FakeMarketsClient({}), MetaStore(tmp_path))


def test_verify_legacy_completeness_raises_when_spot_check_id_missing(
        monkeypatch, tmp_path):
    # The catalog clears the row floor but is missing a specific known,
    # currently-live market -- exactly the "batch under-returned" failure
    # mode this check exists to catch even when the count looks fine.
    monkeypatch.setattr(pm, "LEGACY_MIN_KEPT_MARKETS", 1)
    monkeypatch.setattr(pm, "LEGACY_SPOT_CHECK_IDS", [1])
    pd.DataFrame({"market_id": ["pm_2"]}).to_parquet(
        tmp_path / "markets.parquet", index=False)
    client = _FakeMarketsClient({1: (_api_market("1"), "true")})
    with pytest.raises(RuntimeError, match="missing"):
        pm.verify_legacy_completeness(client, MetaStore(tmp_path))


def _write_markets_parquet(path, market_ids, question_prefix="q"):
    pd.DataFrame({
        "market_id": market_ids, "venue": "polymarket",
        "question": [f"{question_prefix}{m}" for m in market_ids],
        "venue_category": "", "yes_token_id": [f"{m}00" for m in market_ids],
        "total_volume_usd": 100_000.0,
        "open_date": pd.Timestamp("2024-01-01"),
        "close_date": pd.Timestamp("2024-06-01"),
    }).to_parquet(path, index=False)


def test_merge_legacy_markets_unions_distinct_markets(tmp_path):
    main_final = tmp_path / "markets.parquet"
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    _write_markets_parquet(main_final, ["pm_100"])
    _write_markets_parquet(legacy_dir / "markets.parquet", ["pm_1", "pm_2"])
    flag = tmp_path / "markets_legacy_merged.flag"

    pm.merge_legacy_markets(MetaStore(legacy_dir), main_final, flag)

    df = pd.read_parquet(main_final)
    assert set(df["market_id"]) == {"pm_100", "pm_1", "pm_2"}
    assert flag.exists()


def test_merge_legacy_markets_prefers_main_catalog_on_id_collision(tmp_path):
    main_final = tmp_path / "markets.parquet"
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    _write_markets_parquet(main_final, ["pm_1"], question_prefix="keyset-")
    _write_markets_parquet(legacy_dir / "markets.parquet", ["pm_1"],
                           question_prefix="legacy-")
    flag = tmp_path / "markets_legacy_merged.flag"

    pm.merge_legacy_markets(MetaStore(legacy_dir), main_final, flag)

    df = pd.read_parquet(main_final)
    assert len(df) == 1
    assert df.iloc[0]["question"] == "keyset-pm_1"


def test_merge_legacy_markets_idempotent_rerun_before_flag(tmp_path):
    main_final = tmp_path / "markets.parquet"
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    _write_markets_parquet(main_final, ["pm_100"])
    _write_markets_parquet(legacy_dir / "markets.parquet", ["pm_1"])
    flag = tmp_path / "markets_legacy_merged.flag"

    legacy_store = MetaStore(legacy_dir)
    pm.merge_legacy_markets(legacy_store, main_final, flag)
    pm.merge_legacy_markets(legacy_store, main_final, flag)  # crash-resume replay

    df = pd.read_parquet(main_final)
    assert sorted(df["market_id"]) == ["pm_1", "pm_100"]
    assert not df.duplicated("market_id").any()


def test_merge_legacy_markets_invalidation_precedes_catalog_merge(monkeypatch, tmp_path):
    # Regression: invalidation must happen BEFORE _stream_merge updates
    # markets.parquet. If it ran after, a crash between a successful merge
    # and the (not-yet-run) deletion would leave markets.parquet already
    # grown but the volume files stale; a retry's "added" check, now
    # computed against the already-merged catalog, would see nothing new
    # and skip the deletion forever.
    main_final = tmp_path / "markets.parquet"
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    _write_markets_parquet(main_final, ["pm_100"])
    _write_markets_parquet(legacy_dir / "markets.parquet", ["pm_1"])
    flag = tmp_path / "markets_legacy_merged.flag"
    for name in pm.VOLUME_CATALOG_DERIVED_FILES:
        (tmp_path / name).write_text("stale")

    def crash(sources, final_path, key):
        raise RuntimeError("simulated crash mid-merge")
    monkeypatch.setattr(pm, "_stream_merge", crash)

    with pytest.raises(RuntimeError, match="simulated crash"):
        pm.merge_legacy_markets(MetaStore(legacy_dir), main_final, flag)

    # Deletion already happened even though the merge itself never
    # completed and the catalog on disk is unchanged.
    for name in pm.VOLUME_CATALOG_DERIVED_FILES:
        assert not (tmp_path / name).exists()
    assert sorted(pd.read_parquet(main_final)["market_id"]) == ["pm_100"]


def test_merge_legacy_markets_invalidates_volume_artifacts_when_rows_added(tmp_path):
    main_final = tmp_path / "markets.parquet"
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    _write_markets_parquet(main_final, ["pm_100"])
    _write_markets_parquet(legacy_dir / "markets.parquet", ["pm_1"])
    flag = tmp_path / "markets_legacy_merged.flag"
    for name in pm.VOLUME_CATALOG_DERIVED_FILES:
        (tmp_path / name).write_text("stale")

    pm.merge_legacy_markets(MetaStore(legacy_dir), main_final, flag)

    for name in pm.VOLUME_CATALOG_DERIVED_FILES:
        assert not (tmp_path / name).exists(), f"{name} should be invalidated"


def test_merge_legacy_markets_leaves_volume_artifacts_when_nothing_added(tmp_path):
    # A merge that adds no new market_id (idempotent re-run, or a legacy
    # catalog fully subsumed by what's already there) must not force an
    # unnecessary multi-hour polymarket_volume re-sweep.
    main_final = tmp_path / "markets.parquet"
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    _write_markets_parquet(main_final, ["pm_100", "pm_1"])
    _write_markets_parquet(legacy_dir / "markets.parquet", ["pm_1"])
    flag = tmp_path / "markets_legacy_merged.flag"
    for name in pm.VOLUME_CATALOG_DERIVED_FILES:
        (tmp_path / name).write_text("fresh")

    pm.merge_legacy_markets(MetaStore(legacy_dir), main_final, flag)

    for name in pm.VOLUME_CATALOG_DERIVED_FILES:
        assert (tmp_path / name).read_text() == "fresh"


def test_main_legacy_sweep_and_merge_feed_price_phase(monkeypatch, tmp_path):
    # End-to-end: keyset already "complete", legacy sweep runs, merges into
    # markets.parquet, and the price phase -- run fresh each portion --
    # must pick up the newly merged legacy market without any special-case.
    monkeypatch.setattr(pm, "OUT_DIR", tmp_path)
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    monkeypatch.setattr(pm, "LEGACY_MAX_ID", 5)
    monkeypatch.setattr(pm, "LEGACY_BATCH", 2)
    monkeypatch.setattr(pm, "LEGACY_MIN_KEPT_MARKETS", 1)
    monkeypatch.setattr(pm, "LEGACY_SPOT_CHECK_IDS", [3])
    _write_markets_parquet(tmp_path / "markets.parquet", ["pm_100"])

    def fake_fetch_legacy_batch(client, ids):
        return [_api_market("3")] if 3 in ids else []
    monkeypatch.setattr(pm, "fetch_legacy_batch", fake_fetch_legacy_batch)

    fetched = []

    def fake_fetch_history(client, token_id):
        fetched.append(token_id)
        return {"history": [{"t": 1704067200, "p": 0.4}]}
    monkeypatch.setattr(pm, "fetch_history", fake_fetch_history)

    done = False
    guard = 0
    while not done:
        guard += 1
        assert guard < 50, "main() did not converge"
        done = pm.main(markets=10, legacy_batches=1)

    markets_df = pd.read_parquet(tmp_path / "markets.parquet")
    assert set(markets_df["market_id"]) == {"pm_100", "pm_3"}
    prices_df = pd.read_parquet(tmp_path / "prices.parquet")
    assert set(prices_df["market_id"]) == {"pm_100", "pm_3"}
    assert (tmp_path / "markets_legacy_merged.flag").exists()
    assert (tmp_path / "legacy" / "markets.parquet").exists()


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
    # Pre-mark the legacy phase complete+merged so these price-phase-only
    # tests don't fall through to a real (unmocked) legacy Gamma crawl.
    (tmp_path / "legacy").mkdir(exist_ok=True)
    pd.DataFrame(columns=pm.MARKETS_COLUMNS).to_parquet(
        tmp_path / "legacy" / "markets.parquet", index=False)
    (tmp_path / "markets_legacy_merged.flag").write_text("")
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
