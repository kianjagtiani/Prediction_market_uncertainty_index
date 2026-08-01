"""Churn audit: split daily GLOBAL turbulence moves three ways.

Reference decomposition — deliberately re-derives the pipeline's turbulence
math from compute primitives (logit-diff EWMA vols, row-wise weighted means)
so it never imports private pipeline internals. For each step t, with C_t the
set of weight-bearing members present on BOTH t-1 and t,

    repricing_t   = wmean(vol_t,   w_{t-1} | C_t) - wmean(vol_{t-1}, w_{t-1} | C_t)
    reweighting_t = wmean(vol_t,   w_t     | C_t) - wmean(vol_t,     w_{t-1} | C_t)
    membership_t  = (raw_t - raw_{t-1}) - repricing_t - reweighting_t

Repricing holds the weights fixed at yesterday's, so it is news moving the
prices of the standing membership and nothing else. Reweighting is the
separate term the two-way split used to bury inside repricing: every
market's weight is now a daily function of trailing notional, so liquidity
drift alone moves the index and the audit has to see it. Membership is the
remainder driven by entries and exits.
"""
import pandas as pd

from .. import compute, config, universe

COMPONENTS = ["repricing", "reweighting", "membership"]


def _wmean_rows(vals: pd.DataFrame, weights: pd.DataFrame,
                mask: pd.DataFrame) -> pd.Series:
    v, w = vals.where(mask), weights.where(mask)
    return (v * w).sum(axis=1, min_count=1) / w.sum(axis=1, min_count=1)


def decompose(flagged: pd.DataFrame,
              params: dict | None = None) -> pd.DataFrame:
    """Daily frame (date-indexed): delta_raw, repricing, reweighting,
    membership."""
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
    w_prev = weights.shift(1)
    at_old_w = _wmean_rows(vols, w_prev, common)
    repricing = at_old_w - _wmean_rows(vols.shift(1), w_prev, common)
    reweighting = _wmean_rows(vols, weights, common) - at_old_w
    # full-turnover step: nothing to reprice, the whole move is membership
    keep = common.any(axis=1)
    out = pd.DataFrame({"delta_raw": raw.diff(),
                        "repricing": repricing.where(keep, 0.0),
                        "reweighting": reweighting.where(keep, 0.0)})
    out["membership"] = (out["delta_raw"] - out["repricing"]
                         - out["reweighting"])
    return out.dropna(subset=["delta_raw"])


def shares(daily: pd.DataFrame) -> dict[str, float]:
    """Each component's share of total |delta_raw|."""
    total = daily["delta_raw"].abs().sum()
    return {c: float(daily[c].abs().sum() / total) if total else float("nan")
            for c in COMPONENTS}


def main() -> None:
    norm = config.DATA_DIR / "normalized"
    meta = pd.read_parquet(norm / "meta.parquet")
    panel = pd.read_parquet(norm / "panel.parquet")
    daily = decompose(universe.apply_pit_rules(meta, panel))

    out = config.DATA_DIR / "indices" / "churn.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out, index_label="date")

    s = shares(daily)
    for name, share in s.items():
        print(f"{name:12s} share of total |delta raw|: {share:.3f}")
    print(f"membership guideline <= 0.20: "
          f"{'pass' if s['membership'] <= 0.20 else 'CHECK'}")


if __name__ == "__main__":
    main()
