import pandas as pd

from uindex.ingest.store import MetaStore, PriceStore


def _prices(market_id, n=2):
    return pd.DataFrame({
        "market_id": [market_id] * n,
        "date": pd.date_range("2024-01-01", periods=n),
        "close_prob": [0.5] * n,
    })


def test_checkpoint_writes_part_and_clears_buffer(tmp_path):
    store = PriceStore(tmp_path)
    store.append(_prices("pm_1"))
    store.append(_prices("pm_2"))
    store.checkpoint()

    parts = list((tmp_path / "prices_parts").glob("*.parquet"))
    assert len(parts) == 1
    assert set(pd.read_parquet(parts[0])["market_id"]) == {"pm_1", "pm_2"}

    store.checkpoint()  # empty buffer: no new part
    assert len(list((tmp_path / "prices_parts").glob("*.parquet"))) == 1


def test_done_ids_includes_legacy_file_and_parts(tmp_path):
    _prices("pm_legacy").to_parquet(tmp_path / "prices.parquet", index=False)
    store = PriceStore(tmp_path)
    store.append(_prices("pm_new"))
    store.checkpoint()

    assert PriceStore(tmp_path).done_ids() == {"pm_legacy", "pm_new"}


def test_finalize_merges_all_sources_and_removes_parts(tmp_path):
    _prices("pm_legacy").to_parquet(tmp_path / "prices.parquet", index=False)
    store = PriceStore(tmp_path)
    store.append(_prices("pm_a"))
    store.checkpoint()
    store.append(_prices("pm_b"))  # left unflushed: finalize must include it
    store.finalize()

    final = pd.read_parquet(tmp_path / "prices.parquet")
    assert set(final["market_id"]) == {"pm_legacy", "pm_a", "pm_b"}
    assert not (tmp_path / "prices_parts").exists()


def test_finalize_after_interrupted_finalize_does_not_duplicate(tmp_path):
    # Crash window: prices.parquet already merged but parts not yet unlinked.
    # Re-running finalize must not write duplicate (market_id, date) rows —
    # duplicates silently break the rolling-notional universe filter.
    store = PriceStore(tmp_path)
    store.append(_prices("pm_1"))
    store.checkpoint()
    merged = pd.read_parquet(next((tmp_path / "prices_parts").glob("*.parquet")))
    merged.to_parquet(tmp_path / "prices.parquet", index=False)  # simulated crash

    PriceStore(tmp_path).finalize()
    final = pd.read_parquet(tmp_path / "prices.parquet")
    assert len(final) == len(merged)
    assert not final.duplicated(["market_id", "date"]).any()


def test_checkpoint_numbering_survives_gaps(tmp_path):
    parts_dir = tmp_path / "prices_parts"
    parts_dir.mkdir()
    _prices("pm_a").to_parquet(parts_dir / "part-00000.parquet", index=False)
    _prices("pm_c").to_parquet(parts_dir / "part-00002.parquet", index=False)

    store = PriceStore(tmp_path)
    store.append(_prices("pm_new"))
    store.checkpoint()  # count-based naming would overwrite part-00002

    assert store.done_ids() == {"pm_a", "pm_c", "pm_new"}


def test_finalize_with_no_data_is_noop(tmp_path):
    store = PriceStore(tmp_path)
    store.finalize()
    assert not (tmp_path / "prices.parquet").exists()
    assert PriceStore(tmp_path).done_ids() == set()


def test_mark_no_data_counts_as_done(tmp_path):
    # Without this, portioned runs refetch dead markets at the head of every
    # todo list and never progress past them.
    store = PriceStore(tmp_path)
    store.mark_no_data("pm_dead")
    store.append(_prices("pm_live"))
    store.checkpoint()
    assert PriceStore(tmp_path).done_ids() == {"pm_dead", "pm_live"}


def _meta(market_id):
    return pd.DataFrame({"market_id": [market_id], "venue": ["polymarket"],
                         "total_volume_usd": [1.0]})


def test_metastore_commit_resume_roundtrip(tmp_path):
    store = MetaStore(tmp_path)
    assert store.resume() == (None, 0)
    store.commit(_meta("pm_1"), cursor="abc", n_seen=500)

    fresh = MetaStore(tmp_path)
    assert not fresh.complete
    assert fresh.resume() == ("abc", 500)


def test_metastore_empty_commit_still_advances_cursor(tmp_path):
    # A page whose rows are all keep-filtered away must still move the
    # crawl forward, or a fully-dead region of the catalog loops forever.
    store = MetaStore(tmp_path)
    store.commit(_meta("pm_1").iloc[:0], cursor="abc", n_seen=500)
    assert MetaStore(tmp_path).resume() == ("abc", 500)
    assert not (tmp_path / "markets_parts").exists()


def test_metastore_finalize_dedups_replayed_pages(tmp_path):
    # Crash window between shard write and cursor write: the next portion
    # refetches pages under the old cursor and re-commits the same markets.
    store = MetaStore(tmp_path)
    store.commit(_meta("pm_1"), cursor="abc", n_seen=1)
    store.commit(_meta("pm_1"), cursor="abc", n_seen=1)  # replayed shard
    store.commit(_meta("pm_2"), cursor=None, n_seen=2)
    store.finalize(columns=list(_meta("x").columns))

    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert list(df["market_id"]) == ["pm_1", "pm_2"]
    assert store.complete
    assert not (tmp_path / "markets_parts").exists()
    assert not (tmp_path / "markets_cursor.json").exists()


def test_crawl_flushes_intermediate_shards(monkeypatch, tmp_path):
    # The crash-resume property lives in the intermediate flushes: a
    # regression to end-only commit would still pass the end-to-end tests.
    from uindex import config
    from uindex.ingest.store import crawl
    monkeypatch.setattr(config, "INGEST_FLUSH_PAGES", 1)
    pages = {None: (_meta("pm_1"), 1, "c2"), "c2": (_meta("pm_2"), 1, None)}

    store = MetaStore(tmp_path)
    done = crawl(store, lambda cur: pages[cur], list(_meta("x").columns),
                 sleep_s=0, max_pages=1)
    assert done is False
    assert len(list((tmp_path / "markets_parts").glob("part-*.parquet"))) == 1
    assert MetaStore(tmp_path).resume()[0] == "c2"


def test_metastore_finalize_empty_catalog_writes_empty_final(tmp_path):
    store = MetaStore(tmp_path)
    store.commit(_meta("x").iloc[:0], cursor=None, n_seen=0)
    store.finalize(columns=["market_id", "venue", "total_volume_usd"])
    df = pd.read_parquet(tmp_path / "markets.parquet")
    assert df.empty
    assert list(df.columns) == ["market_id", "venue", "total_volume_usd"]
