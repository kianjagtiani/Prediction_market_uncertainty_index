"""Churn audit: split daily GLOBAL turbulence moves into repricing vs membership.

Reference decomposition — deliberately re-derives the pipeline's turbulence
math from compute primitives (logit-diff EWMA vols, row-wise weighted means)
so it never imports private pipeline internals. For each step t,

    repricing_t  = wmean(vol_t, w_t | C_t) - wmean(vol_{t-1}, w_{t-1} | C_t)
    membership_t = (raw_t - raw_{t-1}) - repricing_t

where C_t is the set of weight-bearing members present on BOTH t-1 and t
(each day keeps its own weights). Repricing captures news moving prices of
the standing membership; membership is the remainder driven by entries/exits.
"""
import pandas as pd

from .. import compute, config, universe


def _wmean_rows(vals: pd.DataFrame, weights: pd.DataFrame,
                mask: pd.DataFrame) -> pd.Series:
    v, w = vals.where(mask), weights.where(mask)
    return (v * w).sum(axis=1, min_count=1) / w.sum(axis=1, min_count=1)


def decompose(flagged: pd.DataFrame,
              params: dict | None = None) -> pd.DataFrame:
    """Daily frame (date-indexed): delta_raw, repricing, membership."""
    p = {"ewma_halflife": config.EWMA_HALFLIFE_DAYS,
         "clip_lo": config.CLIP_LO, "clip_hi": config.CLIP_HI,
         **(params or {})}
    sub = flagged[flagged["eligible_turbulence"] &
                  flagged["category"].isin(config.INDEX_UNIVERSES["GLOBAL"])]
    probs = sub.pivot_table(index="date", columns="market_id",
                            values="close_prob", aggfunc="last")
    weights = sub.pivot_table(index="date", columns="market_id",
                              values="weight", aggfunc="last")

    logits = pd.DataFrame(compute.logit(probs.values, p["clip_lo"], p["clip_hi"]),
                          index=probs.index, columns=probs.columns)
    vols = logits.diff().apply(
        lambda col: compute.ewma_vol(col.dropna(), halflife=p["ewma_halflife"])
    ).reindex(probs.index)

    member = vols.notna() & weights.notna() & (weights > 0)
    raw = _wmean_rows(vols, weights, member)

    common = member & member.shift(1, fill_value=False)
    repricing = (_wmean_rows(vols, weights, common)
                 - _wmean_rows(vols.shift(1), weights.shift(1), common))
    # full-turnover step: nothing to reprice, the whole move is membership
    repricing = repricing.where(common.any(axis=1), 0.0)

    out = pd.DataFrame({"delta_raw": raw.diff(), "repricing": repricing})
    out["membership"] = out["delta_raw"] - out["repricing"]
    return out.dropna(subset=["delta_raw"])


def membership_share(daily: pd.DataFrame) -> float:
    """Membership share of total |delta_raw| (guideline <= 0.20)."""
    total = daily["delta_raw"].abs().sum()
    return float(daily["membership"].abs().sum() / total) if total else float("nan")


def main() -> None:
    norm = config.DATA_DIR / "normalized"
    meta = pd.read_parquet(norm / "meta.parquet")
    panel = pd.read_parquet(norm / "panel.parquet")
    daily = decompose(universe.apply_pit_rules(meta, panel))

    out = config.DATA_DIR / "indices" / "churn.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out, index_label="date")

    share = membership_share(daily)
    print(f"membership share of total |delta raw|: {share:.3f} "
          f"(guideline <= 0.20: {'pass' if share <= 0.20 else 'CHECK'})")


if __name__ == "__main__":
    main()
