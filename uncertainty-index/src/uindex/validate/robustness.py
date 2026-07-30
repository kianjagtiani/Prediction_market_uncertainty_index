"""Recompute GLOBAL turbulence under +/-20% param perturbations."""
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
}


def _variant(flagged: pd.DataFrame,
             pipe_params: dict | None = None
             ) -> tuple[pd.Series, pd.DataFrame]:
    """One variant's tidy indices -> (GLOBAL turbulence series, event check)."""
    tidy, _ = pipeline.compute_indices(flagged, params=pipe_params)
    glob = tidy[(tidy["index"] == "GLOBAL") &
                (tidy["gauge"] == "turbulence")].set_index("date")["value"]
    return glob, events.check_events(tidy)


def run() -> pd.DataFrame:
    meta = pd.read_parquet(config.DATA_DIR / "normalized" / "meta.parquet")
    panel = pd.read_parquet(config.DATA_DIR / "normalized" / "panel.parquet")

    base = universe.apply_pit_rules(meta, panel)
    variants = {"baseline": _variant(base)}
    for name, pipe_params in PIPE_PERTURBATIONS.items():
        variants[name] = _variant(base, pipe_params)
    for name, values in UNIVERSE_PERTURBATIONS.items():
        for v in values:
            flagged = universe.apply_pit_rules(meta, panel, params={name: v})
            variants[f"{name}={v}"] = _variant(flagged)
    eq = base.copy()  # weighting-scheme sensitivity
    eq["weight"] = 1.0
    variants["equal_weight"] = _variant(eq)

    wide = pd.DataFrame({name: series
                         for name, (series, _) in variants.items()}).dropna()
    corr = wide.corr()
    summary = pd.DataFrame([{
        "variant": name,
        "min_pairwise_corr": float(corr[name].drop(name).min()),
        "events_passed": int(ev["passed"].sum()),
        "events_total": int(len(ev)),
    } for name, (_, ev) in variants.items()])
    print(f"min pairwise correlation: {summary['min_pairwise_corr'].min():.3f} "
          f"over {len(wide)} common days (target >= 0.90)")
    return summary


if __name__ == "__main__":
    run().to_csv(config.DATA_DIR / "indices" / "robustness.csv", index=False)
