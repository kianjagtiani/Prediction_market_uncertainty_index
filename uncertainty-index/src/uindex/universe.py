"""Point-in-time eligibility and weights. Panel in, panel + flags out."""
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

DEFAULT_OVERRIDES = config.PROJECT_ROOT / "data-overrides" / "duplicates.csv"


def _default_params() -> dict:
    return {
        "resolution_exclusion_days": config.RESOLUTION_EXCLUSION_DAYS,
        "pm_min_total_volume": config.POLYMARKET_MIN_TOTAL_VOLUME_USD,
        "ka_min_rolling_notional": config.KALSHI_MIN_ROLLING_NOTIONAL_USD,
        "rolling_window_days": config.ROLLING_WINDOW_DAYS,
    }


def _normalized_question(q: str) -> str:
    return " ".join((q or "").lower().split())


def apply_pit_rules(meta: pd.DataFrame, panel: pd.DataFrame,
                    params: dict | None = None,
                    overrides_path: Path = DEFAULT_OVERRIDES) -> pd.DataFrame:
    p = {**_default_params(), **(params or {})}
    df = panel.merge(
        meta[["market_id", "venue", "category", "event_ticker",
              "total_volume_usd", "close_date", "question"]],
        on="market_id", how="left",
    ).sort_values(["market_id", "date"])

    eligible = df["close_prob"].notna()

    # 1. Resolution-collapse guard
    cutoff = df["close_date"] - pd.Timedelta(days=p["resolution_exclusion_days"])
    eligible &= df["date"] < cutoff

    # 2. Liquidity floors + weights
    is_pm = df["venue"] == "polymarket"
    eligible &= ~(is_pm & (df["total_volume_usd"] < p["pm_min_total_volume"]))
    rolling = (
        df.groupby("market_id")["daily_notional_usd"]
        .transform(lambda s: s.rolling(p["rolling_window_days"],
                                       min_periods=p["rolling_window_days"]).mean())
    )
    is_ka = df["venue"] == "kalshi"
    eligible &= ~(is_ka & ~(rolling >= p["ka_min_rolling_notional"]))
    df["weight"] = np.where(is_pm, np.log1p(df["total_volume_usd"]), rolling)

    # 3. Kalshi strike groups: lifetime-most-liquid strike represents the event
    ka_meta = meta[meta["venue"] == "kalshi"].dropna(subset=["event_ticker"])
    reps = ka_meta.loc[
        ka_meta.groupby("event_ticker")["total_volume_usd"].idxmax(), "market_id"
    ]
    non_reps = set(ka_meta["market_id"]) - set(reps)
    eligible &= ~df["market_id"].isin(non_reps)

    # 4. Cross-venue exact-question dedup: keep higher lifetime volume
    m = meta.copy()
    m["qnorm"] = m["question"].map(_normalized_question)
    keep = m.loc[m.groupby("qnorm")["total_volume_usd"].idxmax(), "market_id"]
    eligible &= df["market_id"].isin(set(keep))

    # 5. Manual overrides
    if Path(overrides_path).exists():
        drops = set(pd.read_csv(overrides_path)["drop_market_id"])
        eligible &= ~df["market_id"].isin(drops)

    df["eligible"] = eligible.fillna(False)
    return df.drop(columns=["question"]).reset_index(drop=True)
