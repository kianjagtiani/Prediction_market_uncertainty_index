"""Event study: known chaos windows must spike vs a placebo distribution.

For each (event, index) pair the observed statistic is the max of the
scaled series over the event window; the placebo distribution is the max
over every same-length window (sliding, step 1 day) fully outside ALL
event windows. p = share of placebo maxima >= observed; pass at p <= 0.10.
"""
import pandas as pd

PASS_THRESHOLD = 90.0  # legacy continuity column only; verdict is the p-value
P_THRESHOLD = 0.10

EVENTS = [
    {"name": "US election week", "start": "2024-11-04", "end": "2024-11-08",
     "indexes": ["GLOBAL", "ELECTIONS"]},
    {"name": "Liberation Day tariffs", "start": "2025-04-02", "end": "2025-04-09",
     "indexes": ["GLOBAL", "ECON_FED"]},
    {"name": "Israel-Iran war / US strikes", "start": "2025-06-13",
     "end": "2025-06-23", "indexes": ["GLOBAL", "WAR"]},
]


def _window_maxima(series: pd.Series, length: int) -> pd.Series:
    """Max over each length-day window, keyed by window start date.

    Reindexed to a full daily calendar first so window length is calendar
    days; windows with no valid observation (seed-period NaN, gaps) are NaN.
    """
    cal = pd.date_range(series.index.min(), series.index.max(), freq="D")
    s = series.reindex(cal)
    return s.rolling(length, min_periods=1).max().shift(-(length - 1))


def _placebo_p(series: pd.Series, start: pd.Timestamp, end: pd.Timestamp,
               all_events: list[dict]) -> tuple[float, float]:
    """(observed window max, placebo p-value) for one event window."""
    if series.dropna().empty:
        return float("nan"), float("nan")
    length = (end - start).days + 1
    window = series.loc[start:end].dropna()
    observed = float(window.max()) if len(window) else float("nan")

    maxima = _window_maxima(series, length)
    ok = pd.Series(True, index=maxima.index)
    for ev in all_events:  # exclude starts whose window touches any event
        a, b = pd.Timestamp(ev["start"]), pd.Timestamp(ev["end"])
        ok &= ~((maxima.index >= a - pd.Timedelta(days=length - 1))
                & (maxima.index <= b))
    placebo = maxima[ok].dropna()
    if observed != observed or placebo.empty:
        return observed, float("nan")
    return observed, float((placebo >= observed).mean())


def check_events(indices: pd.DataFrame,
                 event_list: list[dict] | None = None) -> pd.DataFrame:
    evs = event_list if event_list is not None else EVENTS
    turb = indices[indices["gauge"] == "turbulence"]
    rows = []
    for ev in evs:
        for idx in ev["indexes"]:
            series = (turb[turb["index"] == idx]
                      .assign(date=lambda d: pd.to_datetime(d["date"]))
                      .set_index("date")["value"].sort_index())
            wmax, p = _placebo_p(series, pd.Timestamp(ev["start"]),
                                 pd.Timestamp(ev["end"]), evs)
            rows.append({"event": ev["name"], "index": idx,
                         "window_max": wmax,
                         "max_ge_90": bool(wmax >= PASS_THRESHOLD),
                         "p_value": p,
                         "passed": bool(p <= P_THRESHOLD)})
    return pd.DataFrame(rows)


def top_spike_days(indices: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    glob = indices[(indices["index"] == "GLOBAL") &
                   (indices["gauge"] == "turbulence")].dropna(subset=["value"])
    return (glob.nlargest(n, "value")[["date", "value"]]
            .reset_index(drop=True))
