"""Event study: known chaos windows must register as spikes."""
import pandas as pd

PASS_THRESHOLD = 90.0

EVENTS = [
    {"name": "US election week", "start": "2024-11-04", "end": "2024-11-08",
     "indexes": ["GLOBAL", "ELECTIONS"]},
    {"name": "Liberation Day tariffs", "start": "2025-04-02", "end": "2025-04-09",
     "indexes": ["GLOBAL", "ECON_FED"]},
    {"name": "Israel-Iran war / US strikes", "start": "2025-06-13",
     "end": "2025-06-23", "indexes": ["GLOBAL", "WAR"]},
]


def check_events(indices: pd.DataFrame,
                 event_list: list[dict] | None = None) -> pd.DataFrame:
    turb = indices[indices["gauge"] == "turbulence"]
    rows = []
    for ev in (event_list if event_list is not None else EVENTS):
        for idx in ev["indexes"]:
            window = turb[(turb["index"] == idx) &
                          (turb["date"] >= ev["start"]) &
                          (turb["date"] <= ev["end"])]["value"]
            wmax = float(window.max()) if len(window) else float("nan")
            rows.append({"event": ev["name"], "index": idx,
                         "window_max": wmax,
                         "passed": bool(wmax >= PASS_THRESHOLD)})
    return pd.DataFrame(rows)


def top_spike_days(indices: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    glob = indices[(indices["index"] == "GLOBAL") &
                   (indices["gauge"] == "turbulence")].dropna(subset=["value"])
    return (glob.nlargest(n, "value")[["date", "value"]]
            .reset_index(drop=True))
