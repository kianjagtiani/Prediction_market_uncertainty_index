"""Turn a flagged panel into daily index series (both gauges, all indices)."""
import pandas as pd

from . import compute, config, universe

# Each gauge reads its own eligibility flag; they are not interchangeable.
GAUGE_ELIGIBILITY = {"turbulence": "eligible_turbulence",
                     "unresolvedness": "eligible_unresolvedness"}


def _weighted_rows(vals: pd.DataFrame, w: pd.DataFrame) -> pd.Series:
    """Row-wise weighted mean over weight-bearing members (NaN if none)."""
    w = w.where(vals.notna() & w.notna() & (w > 0))
    return (vals * w).sum(axis=1, min_count=1) / w.sum(axis=1, min_count=1)


def _gauge_series(sub: pd.DataFrame, gauge: str, p: dict) -> pd.DataFrame:
    """sub: the rows of one universe eligible for `gauge` (the two gauges do
    not share an eligibility mask — see universe.apply_pit_rules rule 5).
    Returns a date-indexed frame of `raw` and `n_constituents`."""
    probs = sub.pivot_table(index="date", columns="market_id",
                            values="close_prob", aggfunc="last")
    weights = sub.pivot_table(index="date", columns="market_id",
                              values="weight", aggfunc="last")

    if gauge == "turbulence":
        logits = pd.DataFrame(
            compute.logit(probs.values, p["clip_lo"], p["clip_hi"]),
            index=probs.index, columns=probs.columns)
        vals = logits.diff().apply(
            lambda col: compute.ewma_vol(col.dropna(),
                                         halflife=p["ewma_halflife"])
        ).reindex(probs.index)
    else:
        vals = pd.DataFrame(
            compute.binary_entropy(probs.values, p["clip_lo"], p["clip_hi"]),
            index=probs.index, columns=probs.columns)

    # weight-bearing members only: a priced market with no usable weight
    # contributes nothing.
    return pd.DataFrame({
        "raw": _weighted_rows(vals, weights),
        "n_constituents": (probs.notna() & weights.notna()
                           & (weights > 0)).sum(axis=1),
    })


def compute_indices(flagged_panel: pd.DataFrame,
                    params: dict | None = None
                    ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (indices, constituents) as tidy frames."""
    p = {"ewma_halflife": config.EWMA_HALFLIFE_DAYS,
         "seed_days": config.SEED_DAYS,
         "clip_lo": config.CLIP_LO, "clip_hi": config.CLIP_HI,
         **(params or {})}

    tidy, members = [], []
    for index_name, categories in config.INDEX_UNIVERSES.items():
        for gauge, flag in GAUGE_ELIGIBILITY.items():
            sub = flagged_panel[flagged_panel[flag]
                                & flagged_panel["category"].isin(categories)]
            if sub.empty:
                continue
            series = _gauge_series(sub, gauge, p)
            scaled = compute.percentile_scale(series["raw"],
                                              seed_days=p["seed_days"])
            tidy.append(pd.DataFrame({
                "date": series.index, "index": index_name, "gauge": gauge,
                "raw": series["raw"].values, "value": scaled.values,
            }))
            members.append(pd.DataFrame({
                "date": series.index, "index": index_name, "gauge": gauge,
                "n_constituents": series["n_constituents"].values,
            }))

    if not tidy:
        raise ValueError("no eligible rows in any universe")
    out = pd.concat(tidy, ignore_index=True).sort_values(
        ["index", "gauge", "date"]).reset_index(drop=True)
    return out, pd.concat(members, ignore_index=True)


def main() -> None:
    norm = config.DATA_DIR / "normalized"
    out_dir = config.DATA_DIR / "indices"
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = pd.read_parquet(norm / "meta.parquet")
    panel = pd.read_parquet(norm / "panel.parquet")
    indices, constituents = compute_indices(universe.apply_pit_rules(meta, panel))

    indices.to_parquet(out_dir / "indices.parquet", index=False)
    constituents.to_parquet(out_dir / "constituents.parquet", index=False)
    for name in config.INDEXES:
        sub = indices[(indices["index"] == name) &
                      (indices["gauge"] == "turbulence")]["value"].dropna()
        if len(sub):
            print(f"{name:10s} turbulence: {len(sub)} days, last={sub.iloc[-1]:.0f}")
        else:
            print(f"{name:10s} EMPTY")


if __name__ == "__main__":
    main()
