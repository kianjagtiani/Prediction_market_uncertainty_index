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
        "pm_min_rolling_notional": config.POLYMARKET_MIN_ROLLING_NOTIONAL_USD,
        "ka_min_rolling_notional": config.KALSHI_MIN_ROLLING_NOTIONAL_USD,
        "rolling_window_days": config.ROLLING_WINDOW_DAYS,
        "pin_lo": config.CLIP_LO,
        "pin_hi": config.CLIP_HI,
        "pin_days": config.PIN_CONSECUTIVE_DAYS,
    }


def _normalized_question(q: str) -> str:
    return " ".join((q or "").lower().split())


def _pinned_run(prob: pd.Series, lo: float, hi: float, days: int) -> pd.Series:
    """True on day t iff the last `days` observed closes (t inclusive) all sit
    at/outside [lo, hi]. Strictly causal: only closes up to t are read.

    Days with no price are skipped rather than treated as unpinned, so a gap
    inside a settled flatline cannot reset the run (those rows are ineligible
    for lack of a price anyway). A fresh collapse has run length 1, so it
    stays in until `days` pinned closes have been observed.
    """
    obs = prob.dropna()
    pinned = ((obs >= hi) | (obs <= lo)).astype(float)
    run = pinned.rolling(days, min_periods=days).min().eq(1.0)
    return run.reindex(prob.index, fill_value=False)


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
    Keeper = earliest open_date (NaT last, ties -> market_id): fixed at
    listing time, so PIT-safe, unlike volume accumulated over the life.

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
        ordered = group.sort_values(["open_date", "market_id"],
                                    na_position="last")
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
              "open_date", "close_date", "question"]],
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

    # 2. Early-resolution guard, causal. A market that settles months before
    #    its close_date collapses (e.g. 0.40 -> 0.995) and flatlines; the
    #    close_date-based cutoff above misses it entirely. The collapse day
    #    itself stays in (genuine repricing); once pin_days observed closes
    #    have all been pinned the flatline leaves, and a bounce back inside
    #    the band re-admits it. No future data is read.
    pinned = df.groupby("market_id")["close_prob"].transform(
        _pinned_run, p["pin_lo"], p["pin_hi"], p["pin_days"]
    )
    eligible &= ~pinned

    # 3. Liquidity floor + weight: trailing rolling mean of daily notional,
    #    same rule for both venues. PM rows carry daily_notional_usd = NaN
    #    when the Goldsky volume sweep hasn't run (normalize's backward-
    #    compatible path); NaN rolling fails the >= floor comparison, so
    #    those rows stay ineligible rather than silently passing.
    rolling = (
        df.groupby("market_id")["daily_notional_usd"]
        .transform(lambda s: s.rolling(p["rolling_window_days"],
                                       min_periods=p["rolling_window_days"]).mean())
    )
    floor = np.where(df["venue"] == "polymarket",
                     p["pm_min_rolling_notional"], p["ka_min_rolling_notional"])
    eligible &= rolling >= floor
    # log1p damps whales identically on both venues.
    df["weight"] = np.log1p(rolling)

    # 4. Kalshi strike groups: per day, the eligible strike with the deepest
    #    trailing notional represents the event (ties -> market_id, so
    #    re-crawls are reproducible); its siblings sit out that day.
    ka = df["venue"].eq("kalshi") & df["event_ticker"].notna()
    cand = df.loc[ka & eligible, ["event_ticker", "date", "market_id"]]
    reps = (cand.assign(roll=rolling)
            .sort_values(["roll", "market_id"], ascending=[False, True])
            .drop_duplicates(["event_ticker", "date"]))
    eligible &= ~ka | df.index.isin(reps.index)

    # 5. Cross-venue exact-question dedup: keep the earlier listing.
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
