import numpy as np
import pandas as pd

from uindex import universe


def _meta_panel():
    meta = pd.DataFrame({
        "market_id": ["pm_a", "pm_small", "ka_s1", "ka_s2"],
        "venue": ["polymarket", "polymarket", "kalshi", "kalshi"],
        "question": ["Will the Fed cut rates?", "Will inflation hit 5%?",
                     "CPI above 3%?", "CPI above 4%?"],
        "category": ["ECON_FED"] * 4,
        "event_ticker": [np.nan, np.nan, "CPI-24", "CPI-24"],
        "total_volume_usd": [200000.0, 100.0, 50000.0, 9000.0],
        "open_date": pd.to_datetime(["2024-01-01"] * 4),
        "close_date": pd.to_datetime(["2024-03-01"] * 4),
    })
    dates = pd.date_range("2024-01-02", "2024-02-28", freq="D")
    frames = []
    for mid in meta["market_id"]:
        frames.append(pd.DataFrame({
            "market_id": mid, "date": dates, "close_prob": 0.5,
            "daily_notional_usd": 1000.0 if mid.startswith("ka_") else np.nan,
        }))
    return meta, pd.concat(frames, ignore_index=True)


def test_resolution_exclusion_window():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    a = out[out["market_id"] == "pm_a"]
    last3 = a[a["date"] > pd.Timestamp("2024-03-01") - pd.Timedelta(days=3)]
    assert not last3["eligible"].any()
    assert a[a["date"] == pd.Timestamp("2024-02-01")]["eligible"].all()


def test_liquidity_floor_drops_small_polymarket():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "pm_small"]["eligible"].any()


def test_strike_group_keeps_only_most_liquid():
    meta, panel = _meta_panel()
    # Base fixture gives every Kalshi row a flat 1000/day notional, which
    # never clears the 5000 rolling-notional floor (see
    # test_kalshi_rolling_notional_floor) regardless of strike-group
    # outcome. Raise ka_s1's notional so this test isolates the
    # strike-group rule instead of being vacuously blocked by the floor.
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "ka_s1"]["eligible"].any()   # 50k volume
    assert not out[out["market_id"] == "ka_s2"]["eligible"].any()  # 9k, same event


def test_kalshi_rolling_notional_floor():
    meta, panel = _meta_panel()
    # 1000/day * 7d rolling mean = 1000 < 5000 floor -> ineligible
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "ka_s1"]["eligible"].iloc[10:].any()
    # raise notional -> eligible after rolling window fills
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    out2 = universe.apply_pit_rules(meta, panel)
    assert out2[out2["market_id"] == "ka_s1"]["eligible"].iloc[10:-3].all()


def test_manual_override_dedup(tmp_path):
    meta, panel = _meta_panel()
    csv = tmp_path / "duplicates.csv"
    csv.write_text("drop_market_id,reason\npm_a,test dup\n")
    out = universe.apply_pit_rules(meta, panel, overrides_path=csv)
    assert not out[out["market_id"] == "pm_a"]["eligible"].any()


def test_weights_positive_for_eligible():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert (out.loc[out["eligible"], "weight"] > 0).all()
