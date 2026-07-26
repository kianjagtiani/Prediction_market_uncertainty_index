"""Polymarket ingestion: Gamma (metadata) + CLOB (price history)."""
import json
import time
from pathlib import Path

import httpx
import pandas as pd

from .. import config
from .store import PriceStore

GAMMA_KEYSET_URL = "https://gamma-api.polymarket.com/markets/keyset"
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


def fetch_all_markets(client: httpx.Client) -> pd.DataFrame:
    # Plain offset pagination 422s past a few thousand results ("offset too
    # large, use /markets/keyset for deeper pagination" - confirmed live
    # during the Task 9 backfill run). The keyset endpoint also silently
    # caps each page at 100 regardless of the requested limit.
    # Each page is converted to a slim DataFrame immediately: accumulating
    # raw market dicts for the full catalog previously grew to multi-GB RSS.
    frames, cursor, n = [], None, 0
    while True:
        params = {"limit": config.POLYMARKET_PAGE_SIZE,
                  "end_date_min": config.BACKFILL_START}
        if cursor:
            params["cursor"] = cursor
        r = client.get(GAMMA_KEYSET_URL, params=params)
        r.raise_for_status()
        payload = r.json()
        batch = payload.get("markets", [])
        if batch:
            frames.append(markets_to_df(batch))
            n += len(batch)
        cursor = payload.get("next_cursor")
        if not batch or not cursor:
            break
        if len(frames) % 100 == 0:
            print(f"  metadata: {n} markets fetched", flush=True)
        time.sleep(config.POLYMARKET_SLEEP_S)
    if not frames:
        return pd.DataFrame(columns=MARKETS_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def history_todo(meta: pd.DataFrame, done: set[str]) -> pd.DataFrame:
    # The universe filter (universe.py) drops markets below the volume floor
    # on this same field, so skipping their history fetch is lossless.
    return meta[
        ~meta["market_id"].isin(done)
        & (meta["total_volume_usd"] >= config.POLYMARKET_MIN_TOTAL_VOLUME_USD)
    ]


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
        meta = fetch_all_markets(client)
        meta.to_parquet(markets_path, index=False)
    print(f"{len(meta)} polymarket markets")

    store = PriceStore(OUT_DIR)
    todo = history_todo(meta, store.done_ids())
    print(f"{len(todo)} markets to fetch after volume pre-filter")
    for i, row in enumerate(todo.itertuples(index=False)):
        try:
            store.append(history_to_df(fetch_history(client, row.yes_token_id),
                                       row.market_id))
        except httpx.HTTPStatusError as e:
            print(f"skip {row.market_id}: {e.response.status_code}")
        time.sleep(config.POLYMARKET_SLEEP_S)
        if i % 200 == 199:  # checkpoint so the run is resumable
            store.checkpoint()
            print(f"checkpoint: {i + 1}/{len(todo)}")
    store.finalize()
    print("polymarket ingestion complete")


if __name__ == "__main__":
    main()
