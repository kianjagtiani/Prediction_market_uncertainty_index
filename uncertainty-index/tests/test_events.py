import pandas as pd

from uindex.validate import events


def _fake_indices():
    dates = pd.date_range("2024-06-01", periods=200, freq="D")
    vals = pd.Series(30.0, index=dates)
    vals["2024-11-04":"2024-11-08"] = 97.0
    return pd.DataFrame({
        "date": dates, "index": "GLOBAL", "gauge": "turbulence",
        "raw": 0.1, "value": vals.values,
    })


def test_event_passes_when_window_spikes():
    df = _fake_indices()
    res = events.check_events(df, event_list=[{
        "name": "US election week", "start": "2024-11-04",
        "end": "2024-11-08", "indexes": ["GLOBAL"],
    }])
    assert len(res) == 1
    assert bool(res.iloc[0]["passed"]) and res.iloc[0]["window_max"] == 97.0


def test_event_fails_when_quiet():
    df = _fake_indices()
    res = events.check_events(df, event_list=[{
        "name": "quiet window", "start": "2024-07-01",
        "end": "2024-07-05", "indexes": ["GLOBAL"],
    }])
    assert not bool(res.iloc[0]["passed"])


def test_top_spike_days_sorted():
    df = _fake_indices()
    top = events.top_spike_days(df, n=3)
    assert len(top) == 3
    assert top["value"].is_monotonic_decreasing
