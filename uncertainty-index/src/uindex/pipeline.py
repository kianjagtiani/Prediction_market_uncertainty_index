"""Turn a flagged panel into daily index series (both gauges, all indices)."""
import pandas as pd

from . import compute, config, universe


def _index_series(sub: pd.DataFrame, ewma_halflife: float) -> pd.DataFrame:
    """sub: eligible rows of one universe. Returns date-indexed raw gauges."""
    probs = sub.pivot_table(index="date", columns="market_id",
                            values="close_prob", aggfunc="last")
    weights = sub.pivot_table(index="date", columns="market_id",
                              values="weight", aggfunc="last")

    logits = pd.DataFrame(compute.logit(probs.values),
                          index=probs.index, columns=probs.columns)
    vols = logits.diff().apply(
        lambda col: compute.ewma_vol(col.dropna(), halflife=ewma_halflife)
    ).reindex(probs.index)
    entropy = pd.DataFrame(compute.binary_entropy(probs.values),
                           index=probs.index, columns=probs.columns)

    rows = []
    for date in probs.index:
        w = weights.loc[date]
        rows.append({
            "date": date,
            "turbulence": compute.weighted_mean(vols.loc[date], w),
            "unresolvedness": compute.weighted_mean(entropy.loc[date], w),
            # weight-bearing members only: a priced market with no usable
            # weight contributes nothing to either gauge (compute.weighted_mean)
            "n_constituents": int((probs.loc[date].notna()
                                   & w.notna() & (w > 0)).sum()),
        })
    return pd.DataFrame(rows).set_index("date")


def compute_indices(flagged_panel: pd.DataFrame,
                    params: dict | None = None) -> pd.DataFrame:
    p = {"ewma_halflife": config.EWMA_HALFLIFE_DAYS,
         "seed_days": config.SEED_DAYS, **(params or {})}
    eligible = flagged_panel[flagged_panel["eligible"]]

    tidy, members = [], []
    for index_name, categories in config.INDEX_UNIVERSES.items():
        sub = eligible[eligible["category"].isin(categories)]
        if sub.empty:
            continue
        series = _index_series(sub, p["ewma_halflife"])
        for gauge in ("turbulence", "unresolvedness"):
            raw = series[gauge]
            scaled = compute.percentile_scale(raw, seed_days=p["seed_days"])
            tidy.append(pd.DataFrame({
                "date": series.index, "index": index_name, "gauge": gauge,
                "raw": raw.values, "value": scaled.values,
            }))
        members.append(pd.DataFrame({
            "date": series.index, "index": index_name,
            "n_constituents": series["n_constituents"].values,
        }))

    out = pd.concat(tidy, ignore_index=True).sort_values(
        ["index", "gauge", "date"]).reset_index(drop=True)
    compute_indices.constituents = pd.concat(members, ignore_index=True)
    return out


def main() -> None:
    norm = config.DATA_DIR / "normalized"
    out_dir = config.DATA_DIR / "indices"
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = pd.read_parquet(norm / "meta.parquet")
    panel = pd.read_parquet(norm / "panel.parquet")
    flagged = universe.apply_pit_rules(meta, panel)
    indices = compute_indices(flagged)

    indices.to_parquet(out_dir / "indices.parquet", index=False)
    compute_indices.constituents.to_parquet(out_dir / "constituents.parquet",
                                            index=False)
    for name in config.INDEXES:
        sub = indices[(indices["index"] == name) &
                      (indices["gauge"] == "turbulence")]["value"].dropna()
        print(f"{name:10s} turbulence: {len(sub)} days, "
              f"last={sub.iloc[-1]:.0f}" if len(sub) else f"{name:10s} EMPTY")


if __name__ == "__main__":
    main()
