import json

import pandas as pd
import pytest

from uindex import config, normalize
from uindex.ingest import kalshi
from uindex.ingest import polymarket_volume as pv
from uindex.ingest.polymarket_volume import TokenMapStore, VolumeStore


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _fill(ts, fid, token, size):
    return {"id": fid, "timestamp": str(ts), "size": str(size),
            "market": {"id": token}}


class _FakeGoldsky:
    """Scripted GraphQL endpoint replicating the live-verified semantics:
    orderBy timestamp with id tie-break, or-cursor continuation, and the
    statement-timeout failure for pages above fail_above rows."""

    def __init__(self, fills=(), market_datas=None, fail_above=None):
        self.fills = sorted(fills, key=lambda f: (int(f["timestamp"]), f["id"]))
        self.market_datas = market_datas or {}  # token -> condition
        self.fail_above = fail_above
        self.calls = []

    def post(self, url, json):
        assert url == config.GOLDSKY_URL
        self.calls.append(json)
        q, var = json["query"], json["variables"]
        if "enrichedOrderFilleds" in q:
            return self._fills_page(var)
        if "id_in" in q:
            rows = [{"id": t, "condition": {"id": self.market_datas[t]}}
                    for t in var["ids"] if t in self.market_datas]
            return _FakeResponse({"data": {"marketDatas": rows}})
        if "condition_in" in q:
            rows = [{"id": t, "condition": {"id": c}}
                    for t, c in self.market_datas.items()
                    if c in var["conds"]]
            return _FakeResponse({"data": {"marketDatas": rows}})
        raise AssertionError(f"unexpected query: {q}")

    def _fills_page(self, var):
        if self.fail_above is not None and var["first"] > self.fail_above:
            return _FakeResponse(
                {"errors": [{"message": "statement timeout"}]})
        w = var["where"]
        if "or" in w:
            gt, eq = w["or"]
            key = (int(gt["timestamp_gt"]), eq["id_gt"])
            rows = [f for f in self.fills
                    if (int(f["timestamp"]), f["id"]) > key]
        else:
            rows = [f for f in self.fills
                    if int(f["timestamp"]) >= int(w["timestamp_gte"])]
        return _FakeResponse(
            {"data": {"enrichedOrderFilleds": rows[:var["first"]]}})


START_TS = 1704067200  # config.BACKFILL_START midnight UTC


def _no_sleep(monkeypatch):
    monkeypatch.setattr(pv.time, "sleep", lambda s: None)


def _relax_completion_guards(monkeypatch):
    """These fixtures sweep a handful of 2024 fills, so the production
    "did the sweep reach the present" guards are exercised separately."""
    monkeypatch.setattr(config, "GOLDSKY_MIN_FILLS", 0)
    monkeypatch.setattr(config, "GOLDSKY_MAX_CURSOR_LAG_S", 10 ** 12)


def test_notional_is_size_over_1e6_pinned_to_live_reconciliation(
        monkeypatch, tmp_path):
    # Live probe: sum(size) over all 243 fills of one token == the
    # subgraph's own orderbook.collateralVolume == 591728611, and
    # scaledCollateralVolume == 591.728611. So USD = size / 1e6, no price.
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    sizes = [591728611 - 39600000, 39600000]  # split of the verified total
    fills = [_fill(START_TS + i, f"0xf{i}", "7", s)
             for i, s in enumerate(sizes)]
    store = VolumeStore(tmp_path)
    assert pv.sweep(_FakeGoldsky(fills), store) is True
    df = pd.read_parquet(store.final_path)
    assert df["notional_usd"].dtype == "float64"
    assert df["notional_usd"].sum() == pytest.approx(591.728611)
    assert df.loc[0, "date"] == pd.Timestamp("2024-01-01")


def test_sweep_cursor_resumes_across_portions_without_double_count(
        monkeypatch, tmp_path):
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_PAGE_SIZE", 2)
    # Two fills share a timestamp so resume exercises the id tie-break.
    fills = [_fill(START_TS, "0xa", "1", 1_000_000),
             _fill(START_TS, "0xb", "1", 2_000_000),
             _fill(START_TS + 86400, "0xc", "1", 4_000_000)]
    assert pv.sweep(_FakeGoldsky(fills), VolumeStore(tmp_path),
                    max_pages=1) is False
    state = json.loads((tmp_path / "volumes_cursor.json").read_text())
    assert state["cursor"] == {"ts": str(START_TS), "id": "0xb"}

    client = _FakeGoldsky(fills)
    assert pv.sweep(client, VolumeStore(tmp_path)) is True
    w = client.calls[0]["variables"]["where"]  # resumed via or-cursor
    assert w["or"][0]["timestamp_gt"] == str(START_TS)
    assert w["or"][1]["id_gt"] == "0xb"
    df = pd.read_parquet(tmp_path / "volumes_by_token.parquet")
    assert df["notional_usd"].sum() == pytest.approx(7.0)  # no replay
    assert len(df) == 2  # two distinct days
    assert not (tmp_path / "volumes_cursor.json").exists()
    assert not (tmp_path / "volumes_parts").exists()


def test_flush_boundary_same_token_day_rows_are_summed(monkeypatch, tmp_path):
    # One (token, day)'s fills landing in different flush shards must SUM
    # in the merge; first-occurrence dedup would silently drop notional.
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_PAGE_SIZE", 1)
    monkeypatch.setattr(pv, "FLUSH_PAGES", 1)
    fills = [_fill(START_TS, "0xa", "1", 3_000_000),
             _fill(START_TS + 60, "0xb", "1", 5_000_000)]
    store = VolumeStore(tmp_path)
    assert pv.sweep(_FakeGoldsky(fills), store) is True
    df = pd.read_parquet(store.final_path)
    assert len(df) == 1
    assert df.loc[0, "notional_usd"] == pytest.approx(8.0)


def test_resume_prunes_uncommitted_flush_shards(monkeypatch, tmp_path):
    # Crash between shard write and cursor write: the replayed pages would
    # double-count the orphan shard's sums, so resume() must delete it.
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_PAGE_SIZE", 1)
    fills = [_fill(START_TS, "0xa", "1", 1_000_000)]
    store = VolumeStore(tmp_path)
    assert pv.sweep(_FakeGoldsky(fills), store, max_pages=1) is False
    orphan = tmp_path / "volumes_parts" / "b01" / "flush-000099.parquet"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"token_id": ["1"], "date": [pd.Timestamp("2024-01-01")],
                  "notional_usd": [999.0]}).to_parquet(orphan, index=False)
    assert pv.sweep(_FakeGoldsky(fills), store) is True
    df = pd.read_parquet(store.final_path)
    assert df["notional_usd"].sum() == pytest.approx(1.0)


def test_page_size_halves_on_statement_timeout(monkeypatch, tmp_path):
    # Live: 1000-row nested-join pages deterministically time out; 500 ok.
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_PAGE_SIZE", 400)
    monkeypatch.setattr(pv, "MIN_PAGE_SIZE", 100)
    fills = [_fill(START_TS + i, f"0x{i:02d}", "1", 1_000_000)
             for i in range(5)]
    client = _FakeGoldsky(fills, fail_above=100)
    assert pv.sweep(client, VolumeStore(tmp_path)) is True
    firsts = [c["variables"]["first"] for c in client.calls]
    assert firsts[:3] == [400, 200, 100]  # halved to the working size
    assert firsts[3:] == [100] * (len(firsts) - 3)  # remembered
    df = pd.read_parquet(tmp_path / "volumes_by_token.parquet")
    assert df["notional_usd"].sum() == pytest.approx(5.0)


def test_page_size_raises_at_floor(monkeypatch, tmp_path):
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_PAGE_SIZE", 200)
    client = _FakeGoldsky([], fail_above=0)
    with pytest.raises(pv.GoldskyQueryError):
        pv.sweep(client, VolumeStore(tmp_path))


class _ShortPageOnce(_FakeGoldsky):
    """A degraded store returns one truncated page mid-sweep, then recovers.
    The module's own docstring records that this endpoint fails
    non-deterministically under load."""

    def _fills_page(self, var):
        first = var["first"]
        if not self.calls[:-1]:  # truncate the very first page only
            var = {**var, "first": max(first // 3, 1)}
        return super()._fills_page(var)


def test_short_page_does_not_end_the_sweep(monkeypatch, tmp_path):
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_PAGE_SIZE", 6)
    fills = [_fill(START_TS + i * 86400, f"0x{i:02d}", "1", 1_000_000)
             for i in range(20)]
    store = VolumeStore(tmp_path)
    assert pv.sweep(_ShortPageOnce(fills), store) is True
    df = pd.read_parquet(store.final_path)
    assert df["notional_usd"].sum() == pytest.approx(20.0)  # nothing lost
    assert len(df) == 20


def test_stale_cursor_refuses_to_finalize(monkeypatch, tmp_path):
    """An empty page while the cursor is still months behind means the
    remaining history is missing, not absent."""
    _no_sleep(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_MIN_FILLS", 0)
    fills = [_fill(START_TS, "0xa", "1", 1_000_000)]
    store = VolumeStore(tmp_path)
    with pytest.raises(pv.SweepIncompleteError, match="days behind"):
        pv.sweep(_FakeGoldsky(fills), store)
    assert not store.complete
    assert (tmp_path / "volumes_cursor.json").exists()  # resumable


def test_too_few_fills_refuses_to_finalize(monkeypatch, tmp_path):
    """The degenerate case: the very first request returns nothing, which
    used to write an empty volumes.parquet and delete Polymarket."""
    _no_sleep(monkeypatch)
    monkeypatch.setattr(config, "GOLDSKY_MIN_FILLS", 10)
    store = VolumeStore(tmp_path)
    with pytest.raises(pv.SweepIncompleteError, match="only 0 fills"):
        pv.sweep(_FakeGoldsky([]), store)
    assert not store.complete


def test_finalize_writes_a_coverage_manifest(monkeypatch, tmp_path):
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    fills = [_fill(START_TS, "0xa", "1", 1_000_000),
             _fill(START_TS + 5 * 86400, "0xb", "2", 2_000_000)]
    store = VolumeStore(tmp_path)
    assert pv.sweep(_FakeGoldsky(fills), store) is True
    m = json.loads(store.manifest_path.read_text())
    assert m == {"first_date": "2024-01-01T00:00:00",
                 "last_date": "2024-01-06T00:00:00",
                 "n_fills": 2, "n_tokens": 2}


def test_build_token_map_pairs_tokens_and_keeps_unknown_yes(
        monkeypatch, tmp_path):
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    meta = pd.DataFrame({"market_id": ["pm_1", "pm_2"],
                         "yes_token_id": ["11", "21"]})
    # Goldsky knows pm_1's condition (yes 11, sibling 12) but not pm_2.
    client = _FakeGoldsky(market_datas={"11": "0xc1", "12": "0xc1"})
    store = TokenMapStore(tmp_path)
    pv.build_token_map(client, store, meta)
    tm = pd.read_parquet(store.final_path).set_index("token_id")["market_id"]
    assert tm.to_dict() == {"11": "pm_1", "12": "pm_1", "21": "pm_2"}
    assert not (tmp_path / "token_map_cursor.json").exists()


def test_build_token_map_resumes_from_position(monkeypatch, tmp_path):
    _no_sleep(monkeypatch)
    _relax_completion_guards(monkeypatch)
    monkeypatch.setattr(pv, "TOKEN_MAP_CHUNK", 1)
    meta = pd.DataFrame({"market_id": ["pm_1", "pm_2"],
                         "yes_token_id": ["11", "21"]})
    store = TokenMapStore(tmp_path)
    store.commit(pd.DataFrame({"token_id": ["11"], "market_id": ["pm_1"]}), 1,
                 pv._catalog_fingerprint(meta))
    client = _FakeGoldsky(market_datas={})
    pv.build_token_map(client, store, meta)
    # Only the second market's chunk was fetched after resume.
    assert [c["variables"]["ids"] for c in client.calls] == [["21"]]
    tm = pd.read_parquet(store.final_path)
    assert set(tm["token_id"]) == {"11", "21"}


def test_token_map_cursor_restarts_when_the_catalog_changed(
        monkeypatch, tmp_path):
    """`pos` is a row offset into a sorted markets.parquet that the
    documented re-crawl workflow rebuilds from zero. Resuming at `pos` in a
    different catalog silently never maps the markets before it."""
    _no_sleep(monkeypatch)
    monkeypatch.setattr(pv, "TOKEN_MAP_CHUNK", 1)
    old = pd.DataFrame({"market_id": ["pm_1", "pm_2"],
                        "yes_token_id": ["11", "21"]})
    store = TokenMapStore(tmp_path)
    store.commit(pd.DataFrame({"token_id": ["11"], "market_id": ["pm_1"]}), 1,
                 pv._catalog_fingerprint(old))

    recrawled = pd.DataFrame(
        {"market_id": ["pm_0", "pm_1", "pm_2"],
         "yes_token_id": ["01", "11", "21"]})
    client = _FakeGoldsky(market_datas={})
    pv.build_token_map(client, store, recrawled)
    assert [c["variables"]["ids"] for c in client.calls] == [["01"], ["11"],
                                                             ["21"]]
    tm = pd.read_parquet(store.final_path)
    assert set(tm["token_id"]) == {"01", "11", "21"}


def test_token_map_skips_missing_and_repeated_yes_token_ids(
        monkeypatch, tmp_path, capsys):
    """A NaN yes_token_id becomes a NaN dict key; a repeated one collapses
    two markets into whichever came last."""
    _no_sleep(monkeypatch)
    meta = pd.DataFrame({"market_id": ["pm_1", "pm_2", "pm_3"],
                         "yes_token_id": ["11", None, "11"]})
    store = TokenMapStore(tmp_path)
    pv.build_token_map(_FakeGoldsky(market_datas={}), store, meta)
    tm = pd.read_parquet(store.final_path)
    assert tm.set_index("token_id")["market_id"].to_dict() == {"11": "pm_1"}
    assert "stay unmapped" in capsys.readouterr().out


def test_assemble_sums_both_tokens_and_drops_unmapped(tmp_path):
    d = pd.Timestamp("2024-03-01")
    pd.DataFrame({
        "token_id": ["11", "12", "12", "99"],
        "date": [d, d, d + pd.Timedelta(days=1), d],
        "notional_usd": [10.0, 5.0, 2.0, 100.0],
    }).to_parquet(tmp_path / "volumes_by_token.parquet", index=False)
    pd.DataFrame({"token_id": ["11", "12"], "market_id": ["pm_1", "pm_1"]}
                 ).to_parquet(tmp_path / "token_map.parquet", index=False)
    pv.assemble(tmp_path)
    vol = pd.read_parquet(tmp_path / "volumes.parquet")
    assert list(vol.columns) == ["market_id", "date", "daily_notional_usd"]
    assert vol["daily_notional_usd"].dtype == "float64"
    assert vol.set_index("date")["daily_notional_usd"].to_dict() == {
        d: 15.0, d + pd.Timedelta(days=1): 2.0}  # token 99 unmapped -> out


def test_assemble_streams_buckets_and_leaves_no_parts(monkeypatch, tmp_path):
    """Pass 1 must never hold the whole panel: an unportioned assemble is a
    plausible watchdog kill, and a kill exits 143 which the driver reads as
    progress -> relaunch -> assemble -> kill, forever. Correctness across
    both the batch and the bucket split is what the streaming has to keep."""
    monkeypatch.setattr(pv, "ASSEMBLE_BATCH_ROWS", 2)
    monkeypatch.setattr(pv, "BUCKETS", 3)
    days = pd.date_range("2024-03-01", periods=3, freq="D")
    rows = [{"token_id": tok, "date": d, "notional_usd": 1.0}
            for tok in ("11", "12", "21", "22") for d in days]
    pd.DataFrame(rows).to_parquet(tmp_path / "volumes_by_token.parquet",
                                  index=False)
    pd.DataFrame({"token_id": ["11", "12", "21", "22"],
                  "market_id": ["pm_1", "pm_1", "pm_2", "pm_2"]}
                 ).to_parquet(tmp_path / "token_map.parquet", index=False)
    pv.assemble(tmp_path)
    vol = pd.read_parquet(tmp_path / "volumes.parquet")
    assert len(vol) == 6  # 2 markets x 3 days, both tokens summed
    assert (vol["daily_notional_usd"] == 2.0).all()
    assert not (tmp_path / "assemble_parts").exists()


def test_waiting_on_markets_parquet_is_bounded(monkeypatch, tmp_path):
    """Exit 3 resets the driver's failure counter, so an unbounded poll
    spins one process a minute forever with no failure and no alert."""
    _no_sleep(monkeypatch)
    monkeypatch.setattr(pv, "OUT_DIR", tmp_path)
    monkeypatch.setattr(pv, "MAX_MARKETS_WAITS", 2)
    pd.DataFrame({"token_id": pd.Series(dtype=str),
                  "date": pd.Series(dtype="datetime64[ns]"),
                  "notional_usd": pd.Series(dtype="float64")}
                 ).to_parquet(tmp_path / "volumes_by_token.parquet",
                              index=False)
    assert pv.main(pages=1) is False
    assert pv.main(pages=1) is False
    with pytest.raises(RuntimeError, match="markets.parquet still absent"):
        pv.main(pages=1)


def _panel_inputs():
    pm_meta = pd.DataFrame({
        "market_id": ["pm_1"], "venue": ["polymarket"],
        "question": ["Will the Fed cut rates?"], "venue_category": [""],
        "yes_token_id": ["11"], "total_volume_usd": [100000.0],
        "open_date": pd.to_datetime(["2024-01-01"]),
        "close_date": pd.to_datetime(["2024-06-01"]),
    })
    pm_prices = pd.DataFrame({
        "market_id": ["pm_1", "pm_1"],
        "date": pd.to_datetime(["2024-02-01", "2024-02-02"]),
        "close_prob": [0.4, 0.45],
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


_PM_VOLUMES = pd.DataFrame({
    "market_id": ["pm_1"], "date": pd.to_datetime(["2024-02-01"]),
    "daily_notional_usd": [321.5],
})
_FULL_COVERAGE = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-06-01"))


def test_build_panel_merges_pm_volumes_with_zero_fill_for_pm_only():
    _, panel, _ = normalize.build_panel(*_panel_inputs(), _PM_VOLUMES,
                                        _FULL_COVERAGE)
    pm = panel[panel["market_id"] == "pm_1"].set_index("date")
    assert pm.loc[pd.Timestamp("2024-02-01"), "daily_notional_usd"] == 321.5
    # Inside the sweep's coverage, a PM day with no fills is genuinely zero.
    assert pm.loc[pd.Timestamp("2024-02-02"), "daily_notional_usd"] == 0.0
    ka = panel[panel["market_id"] == "ka_X"]
    assert (ka["daily_notional_usd"] == 1500.0).all()  # Kalshi untouched


def test_build_panel_keeps_uncovered_days_nan_not_zero():
    """A truncated sweep must shrink the universe visibly, not silently
    report every un-swept market-day as "$0 traded"."""
    _, panel, _ = normalize.build_panel(
        *_panel_inputs(), _PM_VOLUMES,
        (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")))
    pm = panel[panel["market_id"] == "pm_1"].set_index("date")
    assert pm.loc[pd.Timestamp("2024-02-01"), "daily_notional_usd"] == 321.5
    assert pd.isna(pm.loc[pd.Timestamp("2024-02-02"), "daily_notional_usd"])


def test_build_panel_without_coverage_zero_fills_nothing(capsys):
    _, panel, _ = normalize.build_panel(*_panel_inputs(), _PM_VOLUMES)
    pm = panel[panel["market_id"] == "pm_1"].set_index("date")
    assert pd.isna(pm.loc[pd.Timestamp("2024-02-02"), "daily_notional_usd"])
    assert "WARNING" in capsys.readouterr().out


def test_venue_notional_definitions_differ_and_are_pinned():
    """The two venues' daily_notional_usd are built differently and a single
    eligibility floor is applied to both. Pin each construction so a change
    to either has to be a deliberate one.

    Kalshi: contracts x price, one leg per market.
    Polymarket: the USDC collateral leg of CLOB fills, summed over BOTH
    outcome tokens of the market.
    """
    ka = kalshi.candles_to_df({"candlesticks": [{
        "end_period_ts": 1709251200, "volume_fp": "1000.00",
        "price": {"close_dollars": "0.60"},
    }]}, "ka_1")
    assert ka.loc[0, "daily_notional_usd"] == pytest.approx(600.0)


def test_pm_notional_sums_both_outcome_tokens_of_a_market(tmp_path):
    """The other half of the definition: $600 of collateral recorded against
    each of a market's two tokens becomes $1200 for the market. If matched
    YES/NO fills are booked on both orderbooks, PM is ~2x Kalshi for the
    same economic activity under a shared eligibility floor — an open
    reconciliation that volumes_coverage.csv exists to settle."""
    d = pd.Timestamp("2024-03-01")
    pd.DataFrame({"token_id": ["11", "12"], "date": [d, d],
                  "notional_usd": [600.0, 600.0]}
                 ).to_parquet(tmp_path / "volumes_by_token.parquet",
                              index=False)
    pd.DataFrame({"token_id": ["11", "12"],
                  "market_id": ["pm_1", "pm_1"]}
                 ).to_parquet(tmp_path / "token_map.parquet", index=False)
    pv.assemble(tmp_path)
    vol = pd.read_parquet(tmp_path / "volumes.parquet")
    assert vol.loc[0, "daily_notional_usd"] == pytest.approx(1200.0)


def test_coverage_report_flags_markets_the_subgraph_does_not_index(tmp_path):
    """negRisk and legacy AMM fills are not in this subgraph (pm_559700: $0
    swept vs $85k Gamma). Without the report, normalize's zero-fill reads
    that as "genuinely zero notional" and the market silently disappears."""
    pm_dir = tmp_path / "polymarket"
    pm_dir.mkdir()
    pd.DataFrame({
        "market_id": ["pm_ok", "pm_ok", "pm_negrisk"],
        "date": pd.to_datetime(["2024-03-01", "2024-03-02", "2024-03-01"]),
        "daily_notional_usd": [60_000.0, 36_000.0, 10.0],
    }).to_parquet(pm_dir / "volumes.parquet", index=False)
    pd.DataFrame({
        "market_id": ["pm_ok", "pm_negrisk", "pm_missing"],
        "total_volume_usd": [100_000.0, 85_005.0, 50_000.0],
    }).to_parquet(pm_dir / "markets.parquet", index=False)

    pv.write_coverage_report(pm_dir)
    rep = pd.read_csv(pm_dir / "volumes_coverage.csv").set_index("market_id")
    assert rep.loc["pm_ok", "coverage"] == pytest.approx(0.96)
    assert bool(rep.loc["pm_ok", "covered"])
    assert not bool(rep.loc["pm_negrisk", "covered"])
    assert rep.loc["pm_missing", "subgraph_notional_usd"] == 0.0
    assert not bool(rep.loc["pm_missing", "covered"])
    assert normalize.uncovered_markets(tmp_path) == {"pm_negrisk", "pm_missing"}


def test_uncovered_markets_are_never_zero_filled():
    _, panel, _ = normalize.build_panel(
        *_panel_inputs(), _PM_VOLUMES, _FULL_COVERAGE, {"pm_1"})
    pm = panel[panel["market_id"] == "pm_1"].set_index("date")
    assert pm.loc[pd.Timestamp("2024-02-01"), "daily_notional_usd"] == 321.5
    assert pd.isna(pm.loc[pd.Timestamp("2024-02-02"), "daily_notional_usd"])


def test_volume_coverage_reads_the_sweep_manifest(tmp_path):
    pm_dir = tmp_path / "polymarket"
    pm_dir.mkdir()
    assert normalize.volume_coverage(tmp_path) is None
    manifest = pm_dir / "volumes_manifest.json"
    manifest.write_text(json.dumps({"first_date": None, "last_date": None,
                                    "n_fills": 0, "n_tokens": 0}))
    assert normalize.volume_coverage(tmp_path) is None  # empty sweep
    manifest.write_text(json.dumps({"first_date": "2024-01-01T00:00:00",
                                    "last_date": "2025-03-04T00:00:00",
                                    "n_fills": 5, "n_tokens": 2}))
    assert normalize.volume_coverage(tmp_path) == (
        pd.Timestamp("2024-01-01"), pd.Timestamp("2025-03-04"))


def test_build_panel_without_volumes_keeps_pm_nan():
    _, panel, _ = normalize.build_panel(*_panel_inputs())
    assert panel.loc[panel["market_id"] == "pm_1",
                     "daily_notional_usd"].isna().all()
    assert (panel.loc[panel["market_id"] == "ka_X",
                      "daily_notional_usd"] == 1500.0).all()
