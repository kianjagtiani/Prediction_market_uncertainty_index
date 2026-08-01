import numpy as np
import pandas as pd

from uindex import universe
from uindex.validate import robustness


def _meta_panel(n_markets=8, n_days=420):
    rng = np.random.default_rng(5)
    dates = pd.date_range("2024-06-01", periods=n_days, freq="D")
    ids = [f"pm_{i}" for i in range(n_markets)]
    meta = pd.DataFrame({
        "market_id": ids,
        "venue": "polymarket",
        "question": [f"Will thing {i} happen?" for i in range(n_markets)],
        "category": "ECON_FED",
        "event_ticker": np.nan,
        "open_date": dates[0],
        "close_date": dates[-1] + pd.Timedelta(days=30),
    })
    frames = []
    for mid in ids:
        probs = 1 / (1 + np.exp(-np.cumsum(rng.normal(0, 0.08, n_days))))
        frames.append(pd.DataFrame({
            "market_id": mid, "date": dates,
            "close_prob": np.clip(probs, 0.02, 0.98),
            "daily_notional_usd": rng.uniform(6_000, 400_000, n_days),
        }))
    return meta, pd.concat(frames, ignore_index=True)


def test_sweeps_v2_discretionary_knobs():
    """pin_days and rolling_window_days are the two knobs v2 introduced and
    the ones with no external justification; leaving them unperturbed means
    the sensitivity table says nothing about them."""
    summary = robustness.run(*_meta_panel())
    assert {"pin_days=3", "pin_days=8",
            "rolling_window_days=5", "rolling_window_days=10"
            } <= set(summary["variant"])


def test_weighting_variants_are_informational_not_gated():
    """log1p over a floor-truncated support is nearly equal-weight, so the
    equal_weight variant cannot fail the >= 0.90 gate. Weighting variants are
    reported against baseline instead of being scored by the gate."""
    summary = robustness.run(*_meta_panel()).set_index("variant")
    assert set(robustness.WEIGHTING_VARIANTS) <= set(summary.index)
    weighting = summary.loc[list(robustness.WEIGHTING_VARIANTS)]
    assert not weighting["gated"].any()
    assert weighting["min_pairwise_corr"].isna().all()
    assert weighting["corr_vs_baseline"].notna().all()
    gated = summary[summary["gated"]]
    assert gated["min_pairwise_corr"].notna().all()


def test_linear_notional_recovers_the_underlying_rolling_notional():
    meta, panel = _meta_panel(n_markets=2, n_days=40)
    base = universe.apply_pit_rules(meta, panel)
    lin = robustness.WEIGHTING_VARIANTS["linear_notional"](base["weight"])
    rolling = (panel.sort_values(["market_id", "date"])
               .groupby("market_id")["daily_notional_usd"]
               .transform(lambda s: s.rolling(7, min_periods=7).mean()))
    assert np.allclose(lin.dropna().values,
                       rolling.dropna().values, rtol=1e-9)
