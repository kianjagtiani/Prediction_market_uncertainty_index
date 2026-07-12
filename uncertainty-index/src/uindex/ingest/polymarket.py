"""Polymarket ingestion: Gamma (metadata) + CLOB (price history)."""
import json
import time
from pathlib import Path

import httpx
import pandas as pd

from .. import config

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
HISTORY_URL = "https://clob.polymarket.com/prices-history"
OUT_DIR = config.DATA_DIR / "raw" / "polymarket"

MARKETS_COLUMNS = [
    "market_id", "venue", "question", "venue_category",
    "yes_token_id", "total_volume_usd", "open_date", "close_date",
]


def markets_to_df(markets: list[dict]) -> pd.DataFrame:
    rows = []
    for m in markets:
        token_ids = json.loads(m.get("clobTokenIds") or "[]")
        if not token_ids:
            continue
        rows.append({
            "market_id": f"pm_{m['id']}",
            "venue": "polymarket",
            "question": m.get("question") or "",
            "venue_category": (m.get("category") or "").strip(),
            "yes_token_id": str(token_ids[0]),
            "total_volume_usd": float(m.get("volumeNum") or 0.0),
            "open_date": m.get("startDate"),
            "close_date": m.get("endDate"),
        })
    if not rows:
        return pd.DataFrame(columns=MARKETS_COLUMNS)
    df = pd.DataFrame(rows)
    for col in ("open_date", "close_date"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce").dt.tz_localize(None)
    return df


def history_to_df(payload: dict, market_id: str) -> pd.DataFrame:
    pts = payload.get("history", [])
    if not pts:
        return pd.DataFrame(columns=["market_id", "date", "close_prob"])
    df = pd.DataFrame(pts)
    df["date"] = (
        pd.to_datetime(df["t"], unit="s", utc=True).dt.normalize().dt.tz_localize(None)
    )
    df = df.groupby("date", as_index=False)["p"].last()
    df = df.rename(columns={"p": "close_prob"})
    df.insert(0, "market_id", market_id)
    return df[["market_id", "date", "close_prob"]]


def fetch_all_markets(client: httpx.Client) -> list[dict]:
    out, offset = [], 0
    while True:
        r = client.get(GAMMA_URL, params={
            "limit": config.POLYMARKET_PAGE_SIZE, "offset": offset,
            "end_date_min": config.BACKFILL_START,
        })
        r.raise_for_status()
        batch = r.json()
        if not batch:
            return out
        out.extend(batch)
        offset += config.POLYMARKET_PAGE_SIZE
        time.sleep(config.POLYMARKET_SLEEP_S)


def fetch_history(client: httpx.Client, token_id: str) -> dict:
    r = client.get(HISTORY_URL, params={
        "market": token_id, "interval": "max",
        "fidelity": config.POLYMARKET_HISTORY_FIDELITY,
    })
    r.raise_for_status()
    return r.json()


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = httpx.Client(timeout=30)

    markets_path = OUT_DIR / "markets.parquet"
    if markets_path.exists():
        meta = pd.read_parquet(markets_path)
    else:
        meta = markets_to_df(fetch_all_markets(client))
        meta.to_parquet(markets_path, index=False)
    print(f"{len(meta)} polymarket markets")

    prices_path = OUT_DIR / "prices.parquet"
    done: set[str] = set()
    frames: list[pd.DataFrame] = []
    if prices_path.exists():
        existing = pd.read_parquet(prices_path)
        done = set(existing["market_id"].unique())
        frames = [existing]

    todo = meta[~meta["market_id"].isin(done)]
    for i, row in enumerate(todo.itertuples(index=False)):
        try:
            frames.append(history_to_df(fetch_history(client, row.yes_token_id),
                                        row.market_id))
        except httpx.HTTPStatusError as e:
            print(f"skip {row.market_id}: {e.response.status_code}")
        time.sleep(config.POLYMARKET_SLEEP_S)
        if i % 200 == 199:  # checkpoint so the run is resumable
            pd.concat(frames, ignore_index=True).to_parquet(prices_path, index=False)
            print(f"checkpoint: {i + 1}/{len(todo)}")
    pd.concat(frames, ignore_index=True).to_parquet(prices_path, index=False)
    print("polymarket ingestion complete")


if __name__ == "__main__":
    main()
