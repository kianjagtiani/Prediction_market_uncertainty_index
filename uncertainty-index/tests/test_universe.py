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
        "open_date": pd.to_datetime(["2024-01-01"] * 4),
        "close_date": pd.to_datetime(["2024-03-01"] * 4),
    })
    notional = {"pm_a": 10000.0, "pm_small": 100.0,
                "ka_s1": 1000.0, "ka_s2": 1000.0}
    dates = pd.date_range("2024-01-02", "2024-02-28", freq="D")
    frames = [pd.DataFrame({
        "market_id": mid, "date": dates, "close_prob": 0.5,
        "daily_notional_usd": notional[mid],
    }) for mid in meta["market_id"]]
    return meta, pd.concat(frames, ignore_index=True)


def test_resolution_exclusion_window():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    a = out[out["market_id"] == "pm_a"]
    last3 = a[a["date"] > pd.Timestamp("2024-03-01") - pd.Timedelta(days=3)]
    assert not last3["eligible_turbulence"].any()
    assert a[a["date"] == pd.Timestamp("2024-02-01")]["eligible_turbulence"].all()


def test_rolling_floor_drops_small_polymarket():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "pm_small"]["eligible_turbulence"].any()


def test_pm_nan_notional_is_ineligible():
    # normalize's backward-compatible path leaves PM daily_notional_usd NaN
    # until the volume sweep runs; NaN rolling must fail the floor.
    meta, panel = _meta_panel()
    panel.loc[panel["market_id"] == "pm_a", "daily_notional_usd"] = np.nan
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "pm_a"]["eligible_turbulence"].any()


def test_rolling_floor_same_rule_both_venues():
    meta, panel = _meta_panel()
    # 1000/day * 7d rolling mean = 1000 < 5000 floor -> ineligible
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "ka_s1"]["eligible_turbulence"].iloc[10:].any()
    # raise notional -> eligible once the rolling window fills
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    out2 = universe.apply_pit_rules(meta, panel)
    ka = out2[out2["market_id"] == "ka_s1"]
    assert not ka["eligible_turbulence"].iloc[:6].any()  # min_periods = window
    assert ka["eligible_turbulence"].iloc[10:-3].all()


def test_strike_group_keeps_only_deepest():
    meta, panel = _meta_panel()
    # Both strikes clear the floor so this isolates the per-day rep rule.
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    panel.loc[panel["market_id"] == "ka_s2", "daily_notional_usd"] = 6000.0
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "ka_s1"]["eligible_turbulence"].any()
    assert not out[out["market_id"] == "ka_s2"]["eligible_turbulence"].any()


def test_strike_rep_migrates_with_liquidity():
    meta, panel = _meta_panel()
    switch = panel["date"] >= pd.Timestamp("2024-02-01")
    s1 = panel["market_id"] == "ka_s1"
    s2 = panel["market_id"] == "ka_s2"
    panel.loc[s1, "daily_notional_usd"] = np.where(switch[s1], 6000.0, 10000.0)
    panel.loc[s2, "daily_notional_usd"] = np.where(switch[s2], 10000.0, 6000.0)
    out = universe.apply_pit_rules(meta, panel)

    def rep_on(day):
        rows = out[(out["date"] == pd.Timestamp(day)) & out["eligible_turbulence"]
                   & out["market_id"].str.startswith("ka_")]
        return rows["market_id"].tolist()

    assert rep_on("2024-01-20") == ["ka_s1"]
    assert rep_on("2024-02-15") == ["ka_s2"]  # rolling has fully migrated
    # exactly one eligible strike per event per day once the window fills
    ka_days = out[out["eligible_turbulence"] & out["market_id"].str.startswith("ka_")]
    assert (ka_days.groupby("date").size() == 1).all()


def test_strike_tie_break_is_row_order_independent():
    meta, panel = _meta_panel()
    panel.loc[panel["market_id"].str.startswith("ka_"),
              "daily_notional_usd"] = 10000.0
    forward = universe.apply_pit_rules(meta, panel)
    reversed_ = universe.apply_pit_rules(meta.iloc[::-1].reset_index(drop=True),
                                         panel)

    def kept(out):
        ka = out[out["eligible_turbulence"] & out["market_id"].str.startswith("ka_")]
        return set(ka["market_id"])

    # lexicographically first of the tied strikes, either row order
    assert kept(forward) == kept(reversed_) == {"ka_s1"}


def test_strike_rep_promotes_runner_up_when_winner_is_overridden(tmp_path):
    """The representative must be elected AFTER dedup and overrides. Electing
    it first and then dropping the winner deletes the whole event-day instead
    of promoting the runner-up."""
    meta, panel = _meta_panel()
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    panel.loc[panel["market_id"] == "ka_s2", "daily_notional_usd"] = 6000.0
    csv = tmp_path / "duplicates.csv"
    csv.write_text("drop_market_id,reason\nka_s1,test dup\n")
    out = universe.apply_pit_rules(meta, panel, overrides_path=csv)
    day = out[(out["date"] == pd.Timestamp("2024-02-01"))
              & out["eligible_turbulence"]]
    assert set(day["market_id"]) & {"ka_s1", "ka_s2"} == {"ka_s2"}


def test_strike_rep_promotes_runner_up_when_winner_is_deduped(tmp_path):
    """Same failure via rule 3: the deepest strike has a Polymarket
    exact-title twin that was listed earlier, so it is deduped away."""
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "question"] = "CPI above 3%?"
    meta.loc[meta["market_id"] == "pm_a", "open_date"] = pd.Timestamp("2023-12-01")
    panel.loc[panel["market_id"] == "ka_s1", "daily_notional_usd"] = 10000.0
    panel.loc[panel["market_id"] == "ka_s2", "daily_notional_usd"] = 6000.0
    out = universe.apply_pit_rules(meta, panel,
                                   audit_path=tmp_path / "dedup.csv")
    day = out[(out["date"] == pd.Timestamp("2024-02-01"))
              & out["eligible_turbulence"]]
    assert "ka_s1" not in set(day["market_id"])  # deduped: pm_a opened first
    assert "ka_s2" in set(day["market_id"])


def test_manual_override_dedup(tmp_path):
    meta, panel = _meta_panel()
    csv = tmp_path / "duplicates.csv"
    csv.write_text("drop_market_id,reason\npm_a,test dup\n")
    out = universe.apply_pit_rules(meta, panel, overrides_path=csv)
    assert not out[out["market_id"] == "pm_a"]["eligible_turbulence"].any()


def test_weights_positive_for_eligible():
    meta, panel = _meta_panel()
    out = universe.apply_pit_rules(meta, panel)
    assert (out.loc[out["eligible_turbulence"], "weight"] > 0).all()


def _mixed_venue(notional=20_000.0):
    """One eligible PM and one eligible Kalshi market of comparable size."""
    meta = pd.DataFrame({
        "market_id": ["pm_big", "ka_big"],
        "venue": ["polymarket", "kalshi"],
        "question": ["Will the Fed cut rates?", "CPI above 3%?"],
        "category": ["ECON_FED"] * 2,
        "event_ticker": [np.nan, "CPI-24"],
        "open_date": pd.to_datetime(["2024-01-01"] * 2),
        "close_date": pd.to_datetime(["2024-03-01"] * 2),
    })
    dates = pd.date_range("2024-01-02", "2024-02-01", freq="D")
    panel = pd.concat([
        pd.DataFrame({"market_id": mid, "date": dates, "close_prob": 0.5,
                      "daily_notional_usd": notional})
        for mid in meta["market_id"]
    ], ignore_index=True)
    return meta, panel


def test_cross_venue_weights_are_commensurable():
    meta, panel = _mixed_venue()
    out = universe.apply_pit_rules(meta, panel)
    day = out[(out["date"] == pd.Timestamp("2024-02-01")) & out["eligible_turbulence"]]
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
        "open_date": pd.to_datetime(["2024-01-01"] * 2),
        "close_date": pd.to_datetime(["2024-06-01"] * 2),
    })
    panel = pd.concat([
        pd.DataFrame({"market_id": "pm_pin", "date": dates,
                      "close_prob": pinned, "daily_notional_usd": 10000.0}),
        pd.DataFrame({"market_id": "pm_round", "date": dates,
                      "close_prob": round_trip, "daily_notional_usd": 10000.0}),
    ], ignore_index=True)
    # first day pinned exclusion can trigger: the pin_days-th pinned close
    first_out = collapse + pd.Timedelta(days=universe.config.PIN_CONSECUTIVE_DAYS - 1)
    return meta, panel, collapse, first_out


def test_causal_pin_collapse_day_stays_flatline_leaves():
    meta, panel, collapse, first_out = _pin_panel()
    out = universe.apply_pit_rules(meta, panel)
    pin = out[out["market_id"] == "pm_pin"]
    assert pin[(pin["date"] >= collapse) & (pin["date"] < first_out)]["eligible_turbulence"].all()
    assert not pin[pin["date"] >= first_out]["eligible_turbulence"].any()


def test_pin_rule_is_turbulence_only():
    """The pin rule is a settlement-artefact guard for turbulence. Applying
    it to unresolvedness would delete every confidently near-certain market,
    which is exactly what that gauge exists to measure."""
    meta, panel, collapse, first_out = _pin_panel()
    out = universe.apply_pit_rules(meta, panel)
    pin = out[out["market_id"] == "pm_pin"]
    flat = pin[pin["date"] >= first_out]
    assert not flat["eligible_turbulence"].any()
    assert flat["eligible_unresolvedness"].all()


def test_long_shot_stays_in_unresolvedness():
    """A market that trades at 0.5c all year never resolves - it is simply
    unlikely. It is pinned by level, so turbulence drops it, but it must
    keep contributing its (low) entropy to unresolvedness."""
    meta, panel, _, _ = _pin_panel()
    long_shot = panel["market_id"] == "pm_pin"
    panel.loc[long_shot, "close_prob"] = 0.005
    out = universe.apply_pit_rules(meta, panel)
    pin = out[out["market_id"] == "pm_pin"]
    late = pin[pin["date"] >= pd.Timestamp("2024-02-01")]
    assert not late["eligible_turbulence"].any()
    assert late["eligible_unresolvedness"].all()


def test_causal_pin_gap_does_not_reset_the_run():
    meta, panel, collapse, first_out = _pin_panel()
    gap = collapse + pd.Timedelta(days=2)
    panel.loc[(panel["market_id"] == "pm_pin")
              & (panel["date"] == gap), "close_prob"] = np.nan
    out = universe.apply_pit_rules(meta, panel)
    pin = out[out["market_id"] == "pm_pin"]
    # the gap day itself is ineligible (no price) but only shifts the run by
    # one observation: the 5th pinned close now lands on first_out + 1 day
    assert not pin[pin["date"] == gap]["eligible_turbulence"].any()
    assert pin[pin["date"] == first_out]["eligible_turbulence"].all()
    assert not pin[pin["date"] > first_out]["eligible_turbulence"].any()


def test_causal_pin_bounce_readmits():
    meta, panel, collapse, first_out = _pin_panel()
    back = pd.Timestamp("2024-02-01")
    out = universe.apply_pit_rules(meta, panel)
    rnd = out[out["market_id"] == "pm_round"]
    assert not rnd[(rnd["date"] >= first_out) & (rnd["date"] < back)]["eligible_turbulence"].any()
    assert rnd[rnd["date"] >= back]["eligible_turbulence"].all()


def _title_pair(venues, opens, closes):
    meta = pd.DataFrame({
        "market_id": ["m_a", "m_b"],
        "venue": venues,
        "question": ["Will it rain in NYC?", "will it rain in NYC?"],
        "category": ["CLIMATE"] * 2,
        "event_ticker": [np.nan, np.nan],
        "open_date": pd.to_datetime(opens),
        "close_date": pd.to_datetime(closes),
    })
    frames = []
    for mid, o, c in zip(meta["market_id"], opens, closes):
        dates = pd.date_range(o, pd.Timestamp(c) - pd.Timedelta(days=5), freq="D")
        frames.append(pd.DataFrame({
            "market_id": mid, "date": dates, "close_prob": 0.5,
            "daily_notional_usd": 10000.0,
        }))
    return meta, pd.concat(frames, ignore_index=True)


def test_same_venue_repeated_title_disjoint_windows_both_survive():
    meta, panel = _title_pair(["polymarket"] * 2,
                              ["2024-01-01", "2024-04-01"],
                              ["2024-02-01", "2024-05-01"])
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "m_a"]["eligible_turbulence"].any()
    assert out[out["market_id"] == "m_b"]["eligible_turbulence"].any()


def test_cross_venue_same_title_overlap_dedups_keeps_earlier_open(tmp_path):
    meta, panel = _title_pair(["polymarket", "kalshi"],
                              ["2024-01-01", "2024-01-15"],
                              ["2024-02-01", "2024-02-15"])
    audit = tmp_path / "dedup_audit.csv"
    out = universe.apply_pit_rules(meta, panel, audit_path=audit)
    assert not out[out["market_id"] == "m_b"]["eligible_turbulence"].any()  # later open
    assert out[out["market_id"] == "m_a"]["eligible_turbulence"].any()
    rows = pd.read_csv(audit)
    assert rows["drop_market_id"].tolist() == ["m_b"]
    assert rows["keep_market_id"].tolist() == ["m_a"]


def test_dedup_nat_open_date_loses(tmp_path):
    meta, panel = _title_pair(["polymarket", "kalshi"],
                              ["2024-01-01", "2024-01-15"],
                              ["2024-02-01", "2024-02-15"])
    meta.loc[meta["market_id"] == "m_a", "open_date"] = pd.NaT
    audit = tmp_path / "dedup_audit.csv"
    out = universe.apply_pit_rules(meta, panel, audit_path=audit)
    assert not out[out["market_id"] == "m_a"]["eligible_turbulence"].any()
    assert out[out["market_id"] == "m_b"]["eligible_turbulence"].any()
    assert pd.read_csv(audit)["keep_market_id"].tolist() == ["m_b"]


def test_missing_close_date_counted_and_reported(capsys):
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "close_date"] = pd.NaT
    out = universe.apply_pit_rules(meta, panel)
    assert not out[out["market_id"] == "pm_a"]["eligible_turbulence"].any()
    assert "1" in capsys.readouterr().out


def test_rows_before_open_date_are_ineligible():
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "open_date"] = pd.Timestamp("2024-01-15")
    out = universe.apply_pit_rules(meta, panel)
    a = out[out["market_id"] == "pm_a"]
    assert not a[a["date"] < pd.Timestamp("2024-01-15")]["eligible_turbulence"].any()
    assert a[a["date"] == pd.Timestamp("2024-01-15")]["eligible_turbulence"].all()


def test_missing_open_date_does_not_exclude():
    meta, panel = _meta_panel()
    meta.loc[meta["market_id"] == "pm_a", "open_date"] = pd.NaT
    out = universe.apply_pit_rules(meta, panel)
    assert out[out["market_id"] == "pm_a"]["eligible_turbulence"].any()
