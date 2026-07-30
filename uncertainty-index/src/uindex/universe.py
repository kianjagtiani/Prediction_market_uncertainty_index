"""Point-in-time eligibility and weights. Panel in, panel + flags out."""
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

DEFAULT_OVERRIDES = config.PROJECT_ROOT / "data-overrides" / "duplicates.csv"
DEFAULT_DEDUP_AUDIT = config.DATA_DIR / "universe" / "dedup_audit.csv"


def _default_params() -> dict:
    return {
        "resolution_exclusion_days": config.RESOLUTION_EXCLUSION_DAYS,
        "pm_min_total_volume": config.POLYMARKET_MIN_TOTAL_VOLUME_USD,
        "ka_min_rolling_notional": config.KALSHI_MIN_ROLLING_NOTIONAL_USD,
        "rolling_window_days": config.ROLLING_WINDOW_DAYS,
        "pin_lo": config.CLIP_LO,
        "pin_hi": config.CLIP_HI,
    }


def _normalized_question(q: str) -> str:
    return " ".join((q or "").lower().split())


def _terminal_pin(prob: pd.Series, lo: float, hi: float) -> pd.Series:
    """True over the maximal date-sorted suffix pinned at/outside [lo, hi].

    Days with no price are skipped rather than treated as unpinned, so a gap
    in the settled tail cannot reopen the run (those rows are ineligible for
    lack of a price anyway).
    """
    obs = prob.dropna()
    pinned = ((obs >= hi) | (obs <= lo))[::-1].cummin()[::-1]
    return pinned.reindex(prob.index, fill_value=False)


def _windows_overlap(a: pd.Series, b: pd.Series) -> bool:
    """Do the two markets' [open, close] lifetimes intersect? Missing bound
    means unbounded, so a missing date never blocks a dedup."""
    def bounds(m):
        lo = m["open_date"] if pd.notna(m["open_date"]) else pd.Timestamp.min
        hi = m["close_date"] if pd.notna(m["close_date"]) else pd.Timestamp.max
        return lo, hi

    lo_a, hi_a = bounds(a)
    lo_b, hi_b = bounds(b)
    return lo_a <= hi_b and lo_b <= hi_a


def _cross_venue_dups(meta: pd.DataFrame) -> list[dict]:
    """Exact-title duplicates listed on both venues over the same period.

    Same-venue title repeats are left alone: weather dailies and relisted
    questions reuse a title verbatim for genuinely different periods, and
    dropping them removes whole series from history.
    """
    m = meta.assign(qnorm=meta["question"].map(_normalized_question))
    drops = []
    for _, group in m.groupby("qnorm"):
        if group["venue"].nunique() < 2:
            continue
        kept = []
        ordered = group.sort_values(["total_volume_usd", "market_id"],
                                    ascending=[False, True])
        for _, row in ordered.iterrows():
            dup_of = next((k for k in kept if k["venue"] != row["venue"]
                           and _windows_overlap(k, row)), None)
            if dup_of is None:
                kept.append(row)
            else:
                drops.append({
                    "drop_market_id": row["market_id"],
                    "keep_market_id": dup_of["market_id"],
                    "reason": "cross-venue exact title, overlapping window",
                })
    return drops


def apply_pit_rules(meta: pd.DataFrame, panel: pd.DataFrame,
                    params: dict | None = None,
                    overrides_path: Path = DEFAULT_OVERRIDES,
                    audit_path: Path = DEFAULT_DEDUP_AUDIT) -> pd.DataFrame:
    p = {**_default_params(), **(params or {})}
    df = panel.merge(
        meta[["market_id", "venue", "category", "event_ticker",
              "total_volume_usd", "open_date", "close_date", "question"]],
        on="market_id", how="left",
    ).sort_values(["market_id", "date"])

    eligible = df["close_prob"].notna()

    # 1. Trading window. Missing open_date is lenient (no exclusion); missing
    #    close_date excludes the market for its whole life via the NaT
    #    comparison below - conservative, but silent, so it is counted.
    eligible &= df["open_date"].isna() | (df["date"] >= df["open_date"])
    cutoff = df["close_date"] - pd.Timedelta(days=p["resolution_exclusion_days"])
    eligible &= df["date"] < cutoff
    n_no_close = df.loc[df["close_date"].isna(), "market_id"].nunique()
    if n_no_close:
        print(f"universe: {n_no_close} markets have no close_date and are "
              f"excluded for their entire life")

    # 2. Early-resolution guard. A market that settles months before its
    #    close_date collapses (e.g. 0.40 -> 0.995) and pins there; the
    #    close_date-based cutoff above misses it entirely. Drop the terminal
    #    pinned run, which includes the collapse day itself, so the settlement
    #    jump never enters an eligible market's price path. Identifying the
    #    run as *terminal* uses the rest of the path, a deliberate mild
    #    look-ahead in the conservative direction: it only removes fake
    #    spikes, it never adds signal.
    pinned = df.groupby("market_id")["close_prob"].transform(
        _terminal_pin, p["pin_lo"], p["pin_hi"]
    )
    eligible &= ~pinned

    # 3. Liquidity floors + weights
    is_pm = df["venue"] == "polymarket"
    eligible &= ~(is_pm & (df["total_volume_usd"] < p["pm_min_total_volume"]))
    rolling = (
        df.groupby("market_id")["daily_notional_usd"]
        .transform(lambda s: s.rolling(p["rolling_window_days"],
                                       min_periods=p["rolling_window_days"]).mean())
    )
    is_ka = df["venue"] == "kalshi"
    eligible &= ~(is_ka & ~(rolling >= p["ka_min_rolling_notional"]))
    # log1p on both venues. The two inputs are different quantities (PM
    # lifetime USD vs Kalshi rolling daily notional) but a raw linear notional
    # (>= 5,000) would outweigh any log1p PM weight (<= ~21) by ~10^3, making
    # every mixed universe a pure-Kalshi index. Same functional form keeps the
    # venues commensurable and damps whales identically.
    df["weight"] = np.log1p(np.where(is_pm, df["total_volume_usd"], rolling))

    # 4. Kalshi strike groups: lifetime-most-liquid strike represents the
    #    event, ties broken on ticker so re-crawls are reproducible.
    ka_meta = meta[meta["venue"] == "kalshi"].dropna(subset=["event_ticker"])
    reps = (
        ka_meta.sort_values(["total_volume_usd", "market_id"],
                            ascending=[False, True])
        .groupby("event_ticker")["market_id"].first()
    )
    non_reps = set(ka_meta["market_id"]) - set(reps)
    eligible &= ~df["market_id"].isin(non_reps)

    # 5. Cross-venue exact-question dedup: keep the higher lifetime volume.
    dups = _cross_venue_dups(meta)
    if dups:
        Path(audit_path).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(dups).to_csv(audit_path, index=False)
        eligible &= ~df["market_id"].isin({d["drop_market_id"] for d in dups})

    # 6. Manual overrides
    if Path(overrides_path).exists():
        drops = set(pd.read_csv(overrides_path)["drop_market_id"])
        eligible &= ~df["market_id"].isin(drops)

    df["eligible"] = eligible.fillna(False)
    return df.drop(columns=["question"]).reset_index(drop=True)
