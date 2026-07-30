"""Recompute GLOBAL turbulence under +/-20% param perturbations."""
import itertools

import pandas as pd

from .. import config, pipeline, universe

PERTURBATIONS = {
    "ewma_halflife": [8, 12],
    "resolution_exclusion_days": [2, 4],
    "pm_min_total_volume": [40_000, 60_000],
    "ka_min_rolling_notional": [4_000, 6_000],
}


def _global_turbulence(flagged: pd.DataFrame,
                       pipe_params: dict | None = None) -> pd.Series:
    out, _ = pipeline.compute_indices(flagged, params=pipe_params)
    return out[(out["index"] == "GLOBAL") &
               (out["gauge"] == "turbulence")].set_index("date")["value"]


def run() -> pd.DataFrame:
    meta = pd.read_parquet(config.DATA_DIR / "normalized" / "meta.parquet")
    panel = pd.read_parquet(config.DATA_DIR / "normalized" / "panel.parquet")

    base = universe.apply_pit_rules(meta, panel)
    variants = {"baseline": _global_turbulence(base)}
    for v in PERTURBATIONS["ewma_halflife"]:
        variants[f"ewma_halflife={v}"] = _global_turbulence(
            base, {"ewma_halflife": v})
    for name, values in PERTURBATIONS.items():
        if name == "ewma_halflife":
            continue
        for v in values:
            flagged = universe.apply_pit_rules(meta, panel, params={name: v})
            variants[f"{name}={v}"] = _global_turbulence(flagged)
    eq = base.copy()  # weighting-scheme sensitivity
    eq["weight"] = 1.0
    variants["equal_weight"] = _global_turbulence(eq)

    wide = pd.DataFrame(variants).dropna()
    corr = wide.corr()
    pairs = [(a, b, corr.loc[a, b])
             for a, b in itertools.combinations(wide.columns, 2)]
    summary = pd.DataFrame(pairs, columns=["variant_a", "variant_b", "corr"])
    print(f"min pairwise correlation: {summary['corr'].min():.3f} over "
          f"{len(wide)} common days (target >= 0.90)")
    return summary


if __name__ == "__main__":
    run().to_csv(config.DATA_DIR / "indices" / "robustness.csv", index=False)
