import numpy as np
import pandas as pd
import pytest

from uindex.validate import benchmarks


def _pair(lead_days=0, n=300, seed=7):
    """Bench = our index shifted; if lead_days>0 our index LEADS the bench."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    ours = pd.Series(np.cumsum(rng.normal(0, 1, n)), index=dates)
    bench = (ours.shift(lead_days) + rng.normal(0, 0.1, n)).dropna()
    return ours, bench


def test_align_inner_joins_on_date():
    ours, bench = _pair()
    joined = benchmarks.align(ours, bench)
    assert list(joined.columns) == ["idx", "bench"]
    assert joined.notna().all().all()


def test_corr_detects_contemporaneous_relation():
    ours, bench = _pair(lead_days=0)
    stats = benchmarks.corr_and_leadlag(ours, bench)
    assert stats["level_corr"] > 0.9
    assert stats["diff_corr"] > 0.9
    assert not stats["leads"]  # best lag is 0, no lead claim


def test_leadlag_detects_our_lead_and_clears_band():
    ours, bench = _pair(lead_days=3)
    stats = benchmarks.corr_and_leadlag(ours, bench)
    assert benchmarks.best_lag(stats["leadlag"]) == -3  # our index leads by 3
    assert stats["leads"]


def test_noise_band_is_two_over_sqrt_n():
    ours, bench = _pair()
    stats = benchmarks.corr_and_leadlag(ours, bench)
    n = len(pd.concat({"i": ours.diff().dropna(),
                       "b": bench.diff().dropna()}, axis=1).dropna())
    assert stats["noise_band"] == pytest.approx(2 / np.sqrt(n))


def test_diffs_taken_on_own_calendars():
    """Weekday-only bench: join-then-diff would fold our weekend moves into
    Monday and report corr 1.0; own-calendar diffs must not."""
    rng = np.random.default_rng(3)
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    ours = pd.Series(np.cumsum(rng.normal(0, 1, 60)), index=dates)
    bench = ours[ours.index.dayofweek < 5]
    stats = benchmarks.corr_and_leadlag(ours, bench)
    expected = pd.concat({"i": ours.diff(), "b": bench.diff()}, axis=1).dropna()
    assert stats["diff_corr"] == pytest.approx(
        float(expected["i"].corr(expected["b"])))
    assert stats["diff_corr"] < 0.999  # the folded artifact would be exactly 1
