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


def _mixed_venue(volume=1_000_000.0, notional=20_000.0):
    """One eligible PM and one eligible Kalshi market of comparable size."""
    meta = pd.DataFrame({
        "market_id": ["pm_big", "ka_big"],
        "venue": ["polymarket", "kalshi"],
        "question": ["Will the Fed cut rates?", "CPI above 3%?"],
        "category": ["ECON_FED"] * 2,
        "event_ticker": [np.nan, "CPI-24"],
        "total_volume_usd": [volume, volume],
        "open_date": pd.to_datetime(["2024-01-01"] * 2),
        "close_date": pd.to_datetime(["2024-03-01"] * 2),
    })
    dates = pd.date_range("2024-01-02", "2024-02-01", freq="D")
    panel = pd.concat([
        pd.DataFrame({"market_id": "pm_big", "date": dates,
                      "close_prob": 0.5, "daily_notional_usd": np.nan}),
        pd.DataFrame({"market_id": "ka_big", "date": dates,
                      "close_prob": 0.5, "daily_notional_usd": notional}),
    ], ignore_index=True)
    return meta, panel


def test_cross_venue_weights_are_commensurable():
    meta, panel = _mixed_venue()
    out = universe.apply_pit_rules(meta, panel)
    day = out[(out["date"] == pd.Timestamp("2024-02-01")) & out["eligible"]]
    assert set(day["market_id"]) == {"pm_big", "ka_big"}
    assert day["weight"].max() / day["weight"].min() < 10.0


def _pin_panel():
    dates = pd.date_range("2024-01-02", "2024-02-28", freq="D")
    collapse = pd.Timestamp("2024-01-20")
    back = pd.Timestamp("2024-02-01")
    pinned = np.where(dates < collapse, 0.4, 0.995)
    round_trip = np.where(dates < collapse, 0.4,
                          np.where(dates < back, 0.995, 0.6))
    meta = pd.DataFrame({
        "market_id": ["pm_pin", "pm_round"],
        "venue": ["polymarket"] * 2,
        "question": ["Will X resolve early?", "Will Y wobble?"],
        "category": ["ECON_FED"] * 2,
        "event_ticker": [np.nan, np.nan],
        "total_volume_usd": [200000.0, 200000.0],
        "open_date": pd.to_datetime(["2024-01-01"] * 2),
        "close_date": pd.to_datetime(["2024-06-01"] * 2),
    })
    panel = pd.concat([
        pd.DataFrame({"market_id": "pm_pin", "date": dates,
                      "close_prob": pinned, "daily_notional_usd": np.nan}),
        pd.DataFrame({"market_id": "pm_round", "date": dates,
                      "close_prob": round_trip, "daily_notional_usd": np.nan}),
    ], ignore_index=True)
    return meta, panel, collapse


def test_terminal_pin_excludes_early_resolution():
    meta, panel, collapse = _pin_panel()
    out = universe.apply_pit_rules(meta, panel)
    pin = out[out["market_id"] == "pm_pin"]
    assert pin[pin["date"] < collapse]["eligible"].all()
    assert not pin[pin["date"] >= collapse]["eligible"].any()


def test_terminal_pin_survives_a_gap_in_the_settled_tail():
    meta, panel, collapse = _pin_panel()
    tail = (panel["market_id"] == "pm_pin") & (panel["date"] > collapse)
    panel.loc[panel.index[tail][3], "close_prob"] = np.nan
    out = universe.apply_pit_rules(meta, panel)
    pin = out[out["market_id"] == "pm_pin"]
    assert not pin[pin["date"] >= collapse]["eligible"].any()


def test_non_terminal_pin_stays_eligible():
    meta, panel, _ = _pin_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "pm_round"]["eligible"].all()


def _title_pair(venues, opens, closes):
    meta = pd.DataFrame({
        "market_id": ["m_a", "m_b"],
        "venue": venues,
        "question": ["Will it rain in NYC?", "will it rain in NYC?"],
        "category": ["CLIMATE"] * 2,
        "event_ticker": [np.nan, np.nan],
        "total_volume_usd": [200000.0, 300000.0],
        "open_date": pd.to_datetime(opens),
        "close_date": pd.to_datetime(closes),
    })
    frames = []
    for mid, o, c in zip(meta["market_id"], opens, closes):
        dates = pd.date_range(o, pd.Timestamp(c) - pd.Timedelta(days=5), freq="D")
        frames.append(pd.DataFrame({
            "market_id": mid, "date": dates, "close_prob": 0.5,
            "daily_notional_usd": np.nan,
        }))
    return meta, pd.concat(frames, ignore_index=True)


def test_same_venue_repeated_title_disjoint_windows_both_survive():
    meta, panel = _title_pair(["polymarket"] * 2,
                              ["2024-01-01", "2024-04-01"],
                              ["2024-02-01", "2024-05-01"])
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "m_a"]["eligible"].any()
    assert out[out["market_id"] == "m_b"]["eligible"].any()


def test_cross_venue_same_title_overlap_dedups(tmp_path):
    meta, panel = _title_pair(["polymarket", "kalshi"],
                              ["2024-01-01", "2024-01-15"],
                              ["2024-02-01", "2024-02-15"])
    audit = tmp_path / "dedup_audit.csv"
    out = universe.apply_pit_rules(meta, panel, audit_path=audit)
    assert not out[out["market_id"] == "m_a"]["eligible"].any()  # lower volume
    rows = pd.read_csv(audit)
    assert rows["drop_market_id"].tolist() == ["m_a"]
    assert rows["keep_market_id"].tolist() == ["m_b"]


def test_missing_close_date_counted_and_reported(capsys):
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "close_date"] = pd.NaT
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "pm_a"]["eligible"].any()
    assert "1" in capsys.readouterr().out


def test_rows_before_open_date_are_ineligible():
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "open_date"] = pd.Timestamp("2024-01-15")
    out = universe.apply_pit_rules(meta, panel)
    a = out[out["market_id"] == "pm_a"]
    assert not a[a["date"] < pd.Timestamp("2024-01-15")]["eligible"].any()
    assert a[a["date"] == pd.Timestamp("2024-01-15")]["eligible"].all()


def test_missing_open_date_does_not_exclude():
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "open_date"] = pd.NaT
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "pm_a"]["eligible"].any()


def test_strike_tie_break_is_row_order_independent():
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "ka_s2", "total_volume_usd"] = 50000.0
    panel.loc[panel["market_id"].str.startswith("ka_"),
              "daily_notional_usd"] = 10000.0
    forward = universe.apply_pit_rules(meta, panel)
    reversed_ = universe.apply_pit_rules(meta.iloc[::-1].reset_index(drop=True),
                                         panel)
    def kept(out):
        ka = out[out["eligible"] & out["market_id"].str.startswith("ka_")]
        return set(ka["market_id"])

    # lexicographically first of the tied strikes, either row order
    assert kept(forward) == kept(reversed_) == {"ka_s1"}
