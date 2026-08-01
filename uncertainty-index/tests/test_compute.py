import numpy as np
import pandas as pd
import pytest

from uindex import compute
from uindex.config import CLIP_HI, CLIP_LO, SEED_DAYS


def test_clip_prob_bounds():
    out = compute.clip_prob(np.array([0.0, 0.5, 1.0]))
    assert out[0] == CLIP_LO and out[2] == CLIP_HI and out[1] == 0.5


def test_logit_center_and_symmetry():
    assert compute.logit(np.array([0.5]))[0] == pytest.approx(0.0)
    l = compute.logit(np.array([0.2, 0.8]))
    assert l[0] == pytest.approx(-l[1])


def test_logit_tail_moves_dominate():
    # 2% -> 7% must register as a bigger move than 50% -> 55%
    tail = compute.logit(np.array([0.07]))[0] - compute.logit(np.array([0.02]))[0]
    mid = compute.logit(np.array([0.55]))[0] - compute.logit(np.array([0.50]))[0]
    assert tail > 3 * mid


def test_entropy_max_at_half_and_symmetric():
    assert compute.binary_entropy(np.array([0.5]))[0] == pytest.approx(1.0)
    e = compute.binary_entropy(np.array([0.05, 0.95]))
    assert e[0] == pytest.approx(e[1])
    assert e[0] < 0.5


def test_ewma_vol_spikes_on_shock():
    rng = np.random.default_rng(0)
    idx = pd.date_range("2024-01-01", periods=100, freq="D")
    innov = pd.Series(rng.normal(0, 0.02, 100), index=idx)
    innov.iloc[80] = 2.0
    vol = compute.ewma_vol(innov, halflife=10)
    assert vol.iloc[80] > 5 * vol.iloc[79]
    assert vol.iloc[:4].isna().all()  # min_periods respected


def test_ewma_vol_decays_across_a_gap_in_calendar_days():
    """A market leaves the universe for four months and comes back. The
    stale volatility must have decayed by the elapsed DAYS, not by the one
    observation that separates the two innovations in the series."""
    quiet = pd.date_range("2024-01-01", periods=25, freq="D")
    violent = pd.Series(0.8, index=quiet[:20])
    calm = pd.Series(0.0, index=quiet[20:])
    ret = pd.Series([0.0], index=[quiet[-1] + pd.Timedelta(days=120)])
    innov = pd.concat([violent, calm, ret])
    vol = compute.ewma_vol(innov, halflife=10)
    assert vol.iloc[-2] > 0.3   # still hot the day before the market leaves
    assert vol.iloc[-1] < 0.05  # 12 halflives of decay while it is away

    # Same observations, gap closed: this is what an observation-counted
    # ewm would have produced on the return day.
    compacted = innov.set_axis(pd.date_range("2024-01-01", periods=len(innov)))
    assert compute.ewma_vol(compacted, halflife=10).iloc[-1] > 10 * vol.iloc[-1]


def test_ewma_vol_rejects_positional_index():
    with pytest.raises(TypeError, match="DatetimeIndex"):
        compute.ewma_vol(pd.Series([0.1, 0.2, 0.3]))


def test_weighted_mean_ignores_nan_and_zero_weight():
    v = pd.Series([1.0, np.nan, 3.0, 100.0])
    w = pd.Series([1.0, 5.0, 1.0, 0.0])
    assert compute.weighted_mean(v, w) == pytest.approx(2.0)


def test_weighted_mean_empty_is_nan():
    assert np.isnan(compute.weighted_mean(pd.Series([np.nan]), pd.Series([1.0])))


def test_percentile_scale_seed_and_range():
    idx = pd.date_range("2024-01-01", periods=200, freq="D")
    raw = pd.Series(np.linspace(1, 2, 200), index=idx)
    scaled = compute.percentile_scale(raw, seed_days=SEED_DAYS)
    assert scaled.iloc[:SEED_DAYS].isna().all()
    # monotone series: every post-seed day is a new max -> 100
    assert (scaled.iloc[SEED_DAYS:] == 100.0).all()
    assert scaled.max() <= 100 and scaled.min() >= 0


def test_percentile_scale_current_nan_stays_nan():
    # A day with no value must stay NaN, not silently read as "0th percentile".
    idx = pd.date_range("2024-01-01", periods=5, freq="D")
    raw = pd.Series([1.0, 2.0, np.nan, 3.0, 4.0], index=idx)
    scaled = compute.percentile_scale(raw, seed_days=0)
    assert np.isnan(scaled.iloc[2])
    # index 0 is also NaN (expanding min_periods=2 has no prior history yet),
    # not the behavior under test here - check the rest are real values.
    assert not scaled.iloc[[1, 3, 4]].isna().any()


def test_percentile_scale_ignores_gaps_in_history():
    # A NaN gap in history must not count as an automatic non-match for
    # later days (raw=True positional comparison would otherwise dilute
    # the percentile of every subsequent value).
    idx = pd.date_range("2024-01-01", periods=5, freq="D")
    with_gap = pd.Series([1.0, 2.0, np.nan, 3.0, 100.0], index=idx)
    without_gap = pd.Series([1.0, 2.0, 3.0, 100.0],
                            index=idx.delete(2))
    scaled_gap = compute.percentile_scale(with_gap, seed_days=0)
    scaled_nogap = compute.percentile_scale(without_gap, seed_days=0)
    assert scaled_gap.iloc[-1] == scaled_nogap.iloc[-1] == 100.0
    assert scaled_gap.iloc[3] == scaled_nogap.iloc[2]  # value 3.0 in both
