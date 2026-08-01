"""Recompute GLOBAL turbulence under +/-20% param perturbations."""
import numpy as np
import pandas as pd

from .. import config, pipeline, universe
from . import events

# pipeline-level perturbations (applied to the baseline flagged panel)
PIPE_PERTURBATIONS = {
    "ewma_halflife=8": {"ewma_halflife": 8},
    "ewma_halflife=12": {"ewma_halflife": 12},
    "clip=(0.005,0.995)": {"clip_lo": 0.005, "clip_hi": 0.995},
    "clip=(0.02,0.98)": {"clip_lo": 0.02, "clip_hi": 0.98},
}
# universe-level perturbations (panel re-flagged per value)
UNIVERSE_PERTURBATIONS = {
    "resolution_exclusion_days": [2, 4],
    "pm_min_rolling_notional": [4_000, 6_000],
    "ka_min_rolling_notional": [4_000, 6_000],
    "rolling_window_days": [5, 10],
    "pin_days": [3, 8],
}

# Weighting-scheme variants. Reported, never gated: log1p(rolling) over a
# support truncated below at the $5k floor is already close to equal weight
# (a 3300x span of daily notional compresses to a 1.9x weight range), so
# corr(log1p, equal) >= 0.98 is passed by construction and tests nothing.
# linear_notional is the axis that actually discriminates, and there is no
# a-priori reason the index must agree with a whale-dominated weighting.
WEIGHTING_VARIANTS = {
    "equal_weight": lambda w: pd.Series(1.0, index=w.index),
    "linear_notional": np.expm1,  # weight = log1p(rolling) -> rolling
}


def _variant(flagged: pd.DataFrame,
             pipe_params: dict | None = None
             ) -> tuple[pd.Series, pd.DataFrame]:
    """One variant's tidy indices -> (GLOBAL turbulence series, event check)."""
    tidy, _ = pipeline.compute_indices(flagged, params=pipe_params)
    glob = tidy[(tidy["index"] == "GLOBAL") &
                (tidy["gauge"] == "turbulence")].set_index("date")["value"]
    return glob, events.check_events(tidy)


def run(meta: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    base = universe.apply_pit_rules(meta, panel)
    variants = {"baseline": _variant(base)}
    for name, pipe_params in PIPE_PERTURBATIONS.items():
        variants[name] = _variant(base, pipe_params)
    for name, values in UNIVERSE_PERTURBATIONS.items():
        for v in values:
            flagged = universe.apply_pit_rules(meta, panel, params={name: v})
            variants[f"{name}={v}"] = _variant(flagged)
    for name, reweight in WEIGHTING_VARIANTS.items():
        alt = base.copy()
        alt["weight"] = reweight(base["weight"])
        variants[name] = _variant(alt)

    wide = pd.DataFrame({name: series
                         for name, (series, _) in variants.items()}).dropna()
    corr = wide.corr()
    gated = [n for n in variants if n not in WEIGHTING_VARIANTS]
    summary = pd.DataFrame([{
        "variant": name,
        "gated": name in gated,
        # gated variants are compared only to each other: an informational
        # weighting variant must not be able to fail the parameter gate.
        "min_pairwise_corr": (float(corr.loc[gated, name].drop(name).min())
                              if name in gated else float("nan")),
        "corr_vs_baseline": float(corr.loc["baseline", name]),
        "events_passed": int(ev["passed"].sum()),
        "events_total": int(len(ev)),
    } for name, (_, ev) in variants.items()])
    print(f"min pairwise correlation: {summary['min_pairwise_corr'].min():.3f} "
          f"over {len(wide)} common days (target >= 0.90)")
    for name in WEIGHTING_VARIANTS:
        row = summary.loc[summary["variant"] == name].iloc[0]
        print(f"  {name} vs baseline: {row['corr_vs_baseline']:.3f} "
              f"(informational, not gated)")
    return summary


def main() -> None:
    norm = config.DATA_DIR / "normalized"
    summary = run(pd.read_parquet(norm / "meta.parquet"),
                  pd.read_parquet(norm / "panel.parquet"))
    summary.to_csv(config.DATA_DIR / "indices" / "robustness.csv", index=False)


if __name__ == "__main__":
    main()
