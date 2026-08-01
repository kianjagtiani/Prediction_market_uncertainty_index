import numpy as np
import pandas as pd
import pytest

from uindex import pipeline, universe


def _synthetic_flagged_panel(shock_day="2024-09-01"):
    """120 days x 8 ECON_FED markets, small logit noise, one big shock day."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-06-01", periods=120, freq="D")
    frames = []
    for i in range(8):
        logit_path = np.cumsum(rng.normal(0, 0.05, 120))
        shock_idx = list(dates).index(pd.Timestamp(shock_day))
        logit_path[shock_idx:] += 2.5  # violent repricing on shock day
        probs = 1 / (1 + np.exp(-logit_path))
        frames.append(pd.DataFrame({
            "market_id": f"m{i}", "date": dates,
            "close_prob": np.clip(probs, 0.02, 0.98),
            "category": "ECON_FED", "weight": 1.0,
            "eligible_turbulence": True, "eligible_unresolvedness": True,
        }))
    return pd.concat(frames, ignore_index=True)


def test_golden_day_turbulence_spikes_on_shock():
    panel = _synthetic_flagged_panel()
    out, _ = pipeline.compute_indices(panel, params={"seed_days": 30})
    turb = out[(out["index"] == "ECON_FED") & (out["gauge"] == "turbulence")]
    turb = turb.set_index("date")["value"].dropna()
    assert turb.idxmax() == pd.Timestamp("2024-09-01")
    assert turb.max() > 95


def test_global_includes_econ_markets():
    panel = _synthetic_flagged_panel()
    out, _ = pipeline.compute_indices(panel, params={"seed_days": 30})
    glob = out[(out["index"] == "GLOBAL") & (out["gauge"] == "turbulence")]
    assert glob["raw"].notna().sum() > 50


def test_unresolvedness_present_and_bounded():
    panel = _synthetic_flagged_panel()
    out, _ = pipeline.compute_indices(panel, params={"seed_days": 30})
    unres = out[out["gauge"] == "unresolvedness"]["value"].dropna()
    assert len(unres) > 0
    assert unres.between(0, 100).all()


def test_ineligible_markets_do_not_move_index():
    panel = _synthetic_flagged_panel()
    out1, _ = pipeline.compute_indices(panel, params={"seed_days": 30})
    poisoned = panel.copy()
    extra = panel[panel["market_id"] == "m0"].copy()
    extra["market_id"] = "poison"
    extra["close_prob"] = 0.5
    extra["eligible_turbulence"] = False
    extra["eligible_unresolvedness"] = False
    out2, _ = pipeline.compute_indices(pd.concat([poisoned, extra]),
                                    params={"seed_days": 30})
    merged = out1.merge(out2, on=["date", "index", "gauge"], suffixes=("_a", "_b"))
    assert np.allclose(merged["raw_a"].dropna(), merged["raw_b"].dropna())


def test_reproducibility_byte_identical():
    panel = _synthetic_flagged_panel()
    a = pipeline.compute_indices(panel, params={"seed_days": 30})
    b = pipeline.compute_indices(panel, params={"seed_days": 30})
    pd.testing.assert_frame_equal(a[0], b[0])
    pd.testing.assert_frame_equal(a[1], b[1])


def test_n_constituents_counts_weight_bearing_only():
    panel = _synthetic_flagged_panel()
    panel.loc[panel["market_id"] == "m0", "weight"] = np.nan
    _, counts = pipeline.compute_indices(panel, params={"seed_days": 30})
    econ = counts[counts["index"] == "ECON_FED"]["n_constituents"]
    assert (econ == 7).all()  # 8 priced, 1 carries no weight


def test_gauges_read_their_own_eligibility_mask():
    """A pinned long shot is out of turbulence but must still be counted by
    unresolvedness, and must drag that gauge's raw level down."""
    panel = _synthetic_flagged_panel()
    long_shot = panel[panel["market_id"] == "m0"].copy()
    long_shot["market_id"] = "longshot"
    long_shot["close_prob"] = 0.005
    long_shot["eligible_turbulence"] = False
    both = pd.concat([panel, long_shot], ignore_index=True)

    _, counts = pipeline.compute_indices(both, params={"seed_days": 30})
    econ = counts[counts["index"] == "ECON_FED"].set_index(["gauge", "date"])
    assert (econ.loc["turbulence", "n_constituents"] == 8).all()
    assert (econ.loc["unresolvedness", "n_constituents"] == 9).all()

    base, _ = pipeline.compute_indices(panel, params={"seed_days": 30})
    with_ls, _ = pipeline.compute_indices(both, params={"seed_days": 30})

    def gauge(out, name):
        return out[(out["index"] == "ECON_FED") & (out["gauge"] == name)
                   ].set_index("date")["raw"]

    pd.testing.assert_series_equal(gauge(base, "turbulence"),
                                   gauge(with_ls, "turbulence"))
    assert (gauge(with_ls, "unresolvedness")
            < gauge(base, "unresolvedness")).all()


def test_empty_universe_raises_clearly():
    panel = _synthetic_flagged_panel()
    panel[["eligible_turbulence", "eligible_unresolvedness"]] = False
    with pytest.raises(ValueError, match="no eligible rows"):
        pipeline.compute_indices(panel, params={"seed_days": 30})


def test_causal_pin_truncation_invariance_and_flatline_exit():
    """Causal contract: the index never depends on data after t, so full and
    truncated histories agree on every day before the truncation; the
    collapse day itself enters (genuine repricing) and the settled flatline
    drops out once PIN_CONSECUTIVE_DAYS pinned closes have been observed."""
    rng = np.random.default_rng(7)
    dates = pd.date_range("2024-06-01", periods=120, freq="D")
    collapse = pd.Timestamp("2024-08-01")
    rows, prices = [], []
    for i in range(6):
        probs = 1 / (1 + np.exp(-np.cumsum(rng.normal(0, 0.05, 120))))
        rows.append((f"m{i}", np.clip(probs, 0.05, 0.95)))
    rows.append(("collapser", np.where(dates < collapse, 0.4, 0.995)))
    meta = pd.DataFrame({
        "market_id": [mid for mid, _ in rows],
        "venue": "polymarket",
        "question": [f"Q{mid}?" for mid, _ in rows],
        "category": "ECON_FED",
        "event_ticker": np.nan,
        "open_date": pd.Timestamp("2024-05-01"),
        "close_date": pd.Timestamp("2025-01-01"),
    })
    for mid, p in rows:
        prices.append(pd.DataFrame({"market_id": mid, "date": dates,
                                    "close_prob": p,
                                    "daily_notional_usd": 10000.0}))
    panel = pd.concat(prices, ignore_index=True)
    truncated = panel[~((panel["market_id"] == "collapser") &
                        (panel["date"] >= collapse))]

    flagged = universe.apply_pit_rules(meta, panel)
    coll = flagged[flagged["market_id"] == "collapser"]
    first_out = collapse + pd.Timedelta(days=universe.config.PIN_CONSECUTIVE_DAYS - 1)
    assert coll[(coll["date"] >= collapse)
                & (coll["date"] < first_out)]["eligible_turbulence"].all()
    assert not coll[coll["date"] >= first_out]["eligible_turbulence"].any()

    full, _ = pipeline.compute_indices(flagged, params={"seed_days": 30})
    cut, _ = pipeline.compute_indices(universe.apply_pit_rules(meta, truncated),
                                      params={"seed_days": 30})
    merged = full.merge(cut, on=["date", "index", "gauge"], suffixes=("_f", "_c"))
    pre = merged[merged["date"] < collapse]
    assert np.allclose(pre["raw_f"], pre["raw_c"], equal_nan=True)
