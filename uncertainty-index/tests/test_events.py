import numpy as np
import pandas as pd
import pytest

from uindex.validate import events

DATES = pd.date_range("2024-06-01", periods=200, freq="D")


def _tidy(values):
    return pd.DataFrame({"date": DATES, "index": "GLOBAL",
                         "gauge": "turbulence", "raw": 0.1,
                         "value": values})


def _event(start_pos, end_pos, name="ev"):
    return {"name": name, "start": str(DATES[start_pos].date()),
            "end": str(DATES[end_pos].date()), "indexes": ["GLOBAL"]}


def test_spike_in_window_gives_small_p():
    rng = np.random.default_rng(0)
    vals = rng.normal(30, 2, 200)
    vals[120:125] = 95.0
    res = events.check_events(_tidy(vals), event_list=[_event(120, 124)])
    row = res.iloc[0]
    assert row["window_max"] == 95.0
    assert row["p_value"] == 0.0  # unique global max: no placebo window beats it
    assert bool(row["passed"]) and bool(row["max_ge_90"])


def test_flat_noise_gives_large_p():
    rng = np.random.default_rng(1)
    vals = rng.normal(30, 2, 200)
    end = int(pd.Series(vals).rolling(5).max().idxmin())  # quietest window
    res = events.check_events(_tidy(vals),
                              event_list=[_event(end - 4, end, "quiet")])
    row = res.iloc[0]
    assert row["p_value"] > 0.5
    assert not bool(row["passed"]) and not bool(row["max_ge_90"])


def test_nan_seed_windows_dropped_from_placebo_distribution():
    vals = np.full(200, 30.0)
    vals[:50] = np.nan       # seed period
    vals[100:105] = 85.0     # one placebo bump beating the event
    vals[190:195] = 80.0     # event window
    res = events.check_events(_tidy(vals), event_list=[_event(190, 194)])
    row = res.iloc[0]
    # valid placebo starts: 46..185 (partial-NaN ok) plus 195 -> 141 windows,
    # of which starts 96..104 contain the 85 bump. All-NaN windows excluded.
    assert row["p_value"] == pytest.approx(9 / 141)
    assert bool(row["passed"])


def test_event_inside_seed_period_is_nan_and_fails():
    vals = np.full(200, 30.0)
    vals[:50] = np.nan
    res = events.check_events(_tidy(vals), event_list=[_event(10, 14)])
    row = res.iloc[0]
    assert np.isnan(row["window_max"]) and np.isnan(row["p_value"])
    assert not bool(row["passed"])


def test_top_spike_days_sorted():
    vals = np.full(200, 30.0)
    vals[100:105] = 97.0
    top = events.top_spike_days(_tidy(vals), n=3)
    assert len(top) == 3
    assert top["value"].is_monotonic_decreasing
