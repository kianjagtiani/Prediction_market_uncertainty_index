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
    innov = pd.Series(rng.normal(0, 0.02, 100))
    innov.iloc[80] = 2.0
    vol = compute.ewma_vol(innov, halflife=10)
    assert vol.iloc[80] > 5 * vol.iloc[79]
    assert vol.iloc[:4].isna().all()  # min_periods respected


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
