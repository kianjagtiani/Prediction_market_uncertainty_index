"""Unify venue data into one schema; apply taxonomy; drop sports/unmapped."""
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


def build_panel(pm_meta: pd.DataFrame, pm_prices: pd.DataFrame,
                ka_meta: pd.DataFrame, ka_prices: pd.DataFrame,
                pm_volumes: pd.DataFrame | None = None
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
        # PM rows only: an active market-day with no fills is genuinely
        # zero notional (Kalshi rows keep their own candle notional).
        pmp["daily_notional_usd"] = pmp["daily_notional_usd"].fillna(0.0)
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
