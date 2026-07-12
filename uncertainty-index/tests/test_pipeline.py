import numpy as np
import pandas as pd

from uindex import pipeline


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
