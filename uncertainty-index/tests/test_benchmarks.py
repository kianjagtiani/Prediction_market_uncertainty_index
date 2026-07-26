import numpy as np
import pandas as pd

from uindex.validate import benchmarks


def _pair(lead_days=0):
    """Bench = our index shifted; if lead_days>0 our index LEADS the bench."""
    rng = np.random.default_rng(7)
    dates = pd.date_range("2024-01-01", periods=300, freq="D")
    ours = pd.Series(np.cumsum(rng.normal(0, 1, 300)), index=dates)
    bench = ours.shift(lead_days) + rng.normal(0, 0.1, 300)
    return ours, bench


def test_align_inner_joins_on_date():
    ours, bench = _pair()
    joined = benchmarks.align(ours, bench.dropna())
    assert list(joined.columns) == ["idx", "bench"]
    assert joined.notna().all().all()


def test_corr_detects_contemporaneous_relation():
    ours, bench = _pair(lead_days=0)
    stats = benchmarks.corr_and_leadlag(benchmarks.align(ours, bench))
    assert stats["level_corr"] > 0.9


def test_leadlag_detects_our_lead():
    ours, bench = _pair(lead_days=3)
    stats = benchmarks.corr_and_leadlag(benchmarks.align(ours, bench))
    best_lag = max(stats["leadlag"], key=stats["leadlag"].get)
    assert best_lag == -3  # our index leads by 3 days
