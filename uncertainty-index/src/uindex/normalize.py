"""Unify venue data into one schema; apply taxonomy; drop sports/unmapped."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import config

META_COLS = ["market_id", "venue", "question", "category", "event_ticker",
             "total_volume_usd", "open_date", "close_date"]


def categorize(question: str, venue_category: str) -> str:
    # isinstance, not truthiness: parquet round-trips deliver NaN floats.
    text = question.lower() if isinstance(question, str) else ""
    vcat = venue_category.strip().lower() if isinstance(venue_category, str) else ""
    if vcat in config.SPORTS_VENUE_CATEGORIES or any(
            k in text for k in config.SPORTS_KEYWORDS):
        return "SPORTS"
    for cat, keywords in config.CATEGORY_RULES.items():
        if any(k in text for k in keywords):
            return cat
    return config.VENUE_CATEGORY_MAP.get(vcat, "UNMAPPED")


def volume_coverage(raw_dir) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    """The (first, last) day the Goldsky sweep actually covered, or None.

    Written by VolumeStore.finalize. Absent, or present with null bounds,
    means nothing may be zero-filled."""
    path = Path(raw_dir) / "polymarket" / "volumes_manifest.json"
    if not path.exists():
        return None
    m = json.loads(path.read_text())
    if not m.get("first_date") or not m.get("last_date"):
        return None
    return pd.Timestamp(m["first_date"]), pd.Timestamp(m["last_date"])


def build_panel(pm_meta: pd.DataFrame, pm_prices: pd.DataFrame,
                ka_meta: pd.DataFrame, ka_prices: pd.DataFrame,
                pm_volumes: pd.DataFrame | None = None,
                coverage: tuple[pd.Timestamp, pd.Timestamp] | None = None
                ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    pm = pm_meta.copy()
    pm["event_ticker"] = np.nan
    ka = ka_meta.copy()
    meta = pd.concat([pm, ka], ignore_index=True)
    meta["category"] = [
        categorize(q, c) for q, c in zip(meta["question"], meta["venue_category"])
    ]
    dropped = meta[meta["category"].isin(["SPORTS", "UNMAPPED"])]
    meta = meta[~meta["category"].isin(["SPORTS", "UNMAPPED"])][META_COLS]

    pmp = pm_prices.copy()
    if pm_volumes is None:
        pmp["daily_notional_usd"] = np.nan
    else:
        pmp = pmp.merge(
            pm_volumes[["market_id", "date", "daily_notional_usd"]],
            on=["market_id", "date"], how="left")
        # "No fills" only means "$0 traded" where the sweep actually looked.
        # Outside its manifest range the value stays NaN, which the universe
        # already treats as ineligible rather than as a market that fell
        # below the liquidity floor. A truncated sweep therefore shrinks the
        # universe visibly instead of silently zeroing out Polymarket.
        if coverage is None:
            print("WARNING: no volumes_manifest.json - PM market-days with "
                  "no fills stay NaN (ineligible), not $0")
        else:
            inside = pmp["date"].between(*coverage)
            pmp.loc[inside, "daily_notional_usd"] = (
                pmp.loc[inside, "daily_notional_usd"].fillna(0.0))
            outside = int((~inside).sum())
            if outside:
                print(f"normalize: {outside} PM market-days fall outside the "
                      f"volume sweep's coverage {coverage[0].date()}..."
                      f"{coverage[1].date()} and stay NaN (ineligible)")
    panel = pd.concat([pmp, ka_prices], ignore_index=True)
    panel = panel.merge(meta[["market_id"]], on="market_id", how="inner")
    panel = panel.sort_values(["market_id", "date"]).reset_index(drop=True)
    return meta.reset_index(drop=True), panel, dropped


def main() -> None:
    raw = config.DATA_DIR / "raw"
    out = config.DATA_DIR / "normalized"
    out.mkdir(parents=True, exist_ok=True)

    vol_path = raw / "polymarket" / "volumes.parquet"
    pm_volumes = pd.read_parquet(vol_path) if vol_path.exists() else None
    if pm_volumes is None:
        print("WARNING: polymarket volumes.parquet missing - PM "
              "daily_notional_usd stays NaN (run ingest.polymarket_volume)")

    meta, panel, dropped = build_panel(
        pd.read_parquet(raw / "polymarket" / "markets.parquet"),
        pd.read_parquet(raw / "polymarket" / "prices.parquet"),
        pd.read_parquet(raw / "kalshi" / "markets.parquet"),
        pd.read_parquet(raw / "kalshi" / "prices.parquet"),
        pm_volumes,
        volume_coverage(raw),
    )
    meta.to_parquet(out / "meta.parquet", index=False)
    panel.to_parquet(out / "panel.parquet", index=False)

    triage = dropped[dropped["category"] == "UNMAPPED"]
    triage[["market_id", "venue", "question", "venue_category",
            "total_volume_usd"]].to_csv(out / "unmapped_triage.csv", index=False)
    n_sports = int((dropped["category"] == "SPORTS").sum())
    print(f"kept {len(meta)} markets | dropped {n_sports} sports, "
          f"{len(triage)} unmapped (see unmapped_triage.csv)")


if __name__ == "__main__":
    main()
