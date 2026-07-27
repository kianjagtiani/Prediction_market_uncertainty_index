import numpy as np
import pandas as pd

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
            "category": "ECON_FED", "eligible": True, "weight": 1.0,
        }))
    return pd.concat(frames, ignore_index=True)


def test_golden_day_turbulence_spikes_on_shock():
    panel = _synthetic_flagged_panel()
    out = pipeline.compute_indices(panel, params={"seed_days": 30})
    turb = out[(out["index"] == "ECON_FED") & (out["gauge"] == "turbulence")]
    turb = turb.set_index("date")["value"].dropna()
    assert turb.idxmax() == pd.Timestamp("2024-09-01")
    assert turb.max() > 95


def test_global_includes_econ_markets():
    panel = _synthetic_flagged_panel()
    out = pipeline.compute_indices(panel, params={"seed_days": 30})
    glob = out[(out["index"] == "GLOBAL") & (out["gauge"] == "turbulence")]
    assert glob["raw"].notna().sum() > 50


def test_unresolvedness_present_and_bounded():
    panel = _synthetic_flagged_panel()
    out = pipeline.compute_indices(panel, params={"seed_days": 30})
    unres = out[out["gauge"] == "unresolvedness"]["value"].dropna()
    assert len(unres) > 0
    assert unres.between(0, 100).all()


def test_ineligible_markets_do_not_move_index():
    panel = _synthetic_flagged_panel()
    out1 = pipeline.compute_indices(panel, params={"seed_days": 30})
    poisoned = panel.copy()
    extra = panel[panel["market_id"] == "m0"].copy()
    extra["market_id"] = "poison"
    extra["close_prob"] = 0.5
    extra["eligible"] = False
    out2 = pipeline.compute_indices(pd.concat([poisoned, extra]),
                                    params={"seed_days": 30})
    merged = out1.merge(out2, on=["date", "index", "gauge"], suffixes=("_a", "_b"))
    assert np.allclose(merged["raw_a"].dropna(), merged["raw_b"].dropna())


def test_reproducibility_byte_identical():
    panel = _synthetic_flagged_panel()
    a = pipeline.compute_indices(panel, params={"seed_days": 30})
    b = pipeline.compute_indices(panel, params={"seed_days": 30})
    pd.testing.assert_frame_equal(a, b)


def test_n_constituents_counts_weight_bearing_only():
    panel = _synthetic_flagged_panel()
    panel.loc[panel["market_id"] == "m0", "weight"] = np.nan
    pipeline.compute_indices(panel, params={"seed_days": 30})
    counts = pipeline.compute_indices.constituents
    econ = counts[counts["index"] == "ECON_FED"]["n_constituents"]
    assert (econ == 7).all()  # 8 priced, 1 carries no weight


def test_terminal_pin_collapse_never_reaches_the_index():
    """The M10 guard must drop the settlement jump, not just the pinned tail."""
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
        "total_volume_usd": 200000.0,
        "open_date": pd.Timestamp("2024-05-01"),
        "close_date": pd.Timestamp("2025-01-01"),
    })
    for mid, p in rows:
        prices.append(pd.DataFrame({"market_id": mid, "date": dates,
                                    "close_prob": p,
                                    "daily_notional_usd": np.nan}))
    panel = pd.concat(prices, ignore_index=True)
    truncated = panel[~((panel["market_id"] == "collapser") &
                        (panel["date"] >= collapse))]

    full = pipeline.compute_indices(universe.apply_pit_rules(meta, panel),
                                    params={"seed_days": 30})
    cut = pipeline.compute_indices(universe.apply_pit_rules(meta, truncated),
                                   params={"seed_days": 30})
    merged = full.merge(cut, on=["date", "index", "gauge"], suffixes=("_f", "_c"))
    assert np.allclose(merged["raw_f"], merged["raw_c"], equal_nan=True)
