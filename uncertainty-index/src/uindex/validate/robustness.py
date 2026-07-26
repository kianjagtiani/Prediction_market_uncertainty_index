"""Recompute GLOBAL turbulence under +/-20% param perturbations."""
import itertools

import pandas as pd

from .. import config, pipeline, universe

PERTURBATIONS = {
    "ewma_halflife": [8, 10, 12],
    "resolution_exclusion_days": [2, 3, 4],
    "pm_min_total_volume": [40_000, 50_000, 60_000],
    "ka_min_rolling_notional": [4_000, 5_000, 6_000],
}


def run() -> pd.DataFrame:
    meta = pd.read_parquet(config.DATA_DIR / "normalized" / "meta.parquet")
    panel = pd.read_parquet(config.DATA_DIR / "normalized" / "panel.parquet")

    variants = {}
    base_flagged = None
    for name, values in PERTURBATIONS.items():
        for v in values:
            uni_params = {name: v} if name != "ewma_halflife" else None
            pipe_params = {"ewma_halflife": v} if name == "ewma_halflife" else None
            flagged = universe.apply_pit_rules(meta, panel, params=uni_params)
            out = pipeline.compute_indices(flagged, params=pipe_params)
            series = out[(out["index"] == "GLOBAL") &
                         (out["gauge"] == "turbulence")].set_index("date")["value"]
            variants[f"{name}={v}"] = series
    # equal-weight variant (weighting-scheme sensitivity)
    flagged = universe.apply_pit_rules(meta, panel)
    flagged["weight"] = 1.0
    out = pipeline.compute_indices(flagged)
    variants["equal_weight"] = out[(out["index"] == "GLOBAL") &
                                   (out["gauge"] == "turbulence")
                                   ].set_index("date")["value"]

    wide = pd.DataFrame(variants).dropna()
    corr = wide.corr()
    pairs = [(a, b, corr.loc[a, b])
             for a, b in itertools.combinations(wide.columns, 2)]
    summary = pd.DataFrame(pairs, columns=["variant_a", "variant_b", "corr"])
    print(f"min pairwise correlation: {summary['corr'].min():.3f} "
          f"(target >= 0.90)")
    return summary


if __name__ == "__main__":
    run().to_csv(config.DATA_DIR / "indices" / "robustness.csv", index=False)
