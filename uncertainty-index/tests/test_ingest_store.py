import pandas as pd

from uindex.ingest.store import PriceStore


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
