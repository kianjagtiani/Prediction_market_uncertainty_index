"""Polymarket PIT daily notional via the Goldsky orderbook subgraph.

Sweeps `enrichedOrderFilleds` globally in (timestamp, id) order, buckets
fills to (token_id, UTC day, notional_usd) aggregate shards on disk, maps
tokens to our market ids, and sums both outcome tokens per market-day into
volumes.parquet.

Live-verified facts (2026-07-29, 16 probe requests):
- `size` is the fill's USDC collateral leg scaled 1e6, NOT the token
  quantity: over all 243 fills of one token, sum(size) == the subgraph's
  own `orderbook.collateralVolume` (591728611) exactly, and
  `scaledCollateralVolume` == that / 1e6 (591.728611). So
  notional_usd = size / 1e6 and `price` plays no role (sum(size*price)
  reconciles to nothing).
- `orderBy: timestamp` results are tie-broken by id ascending, and the
  filter `{or: [{timestamp_gt: ts}, {timestamp: ts, id_gt: id}]}` resumes
  mid-timestamp exactly (verified against an overlapping page), so a
  (timestamp, id) cursor is lossless.
- 1000-row pages with the nested `market { id }` join deterministically hit
  the store's statement timeout; 500 returns in <1s, 100 in ~0.3s. Page
  size adapts downward within a portion.
- `marketData(id: token).condition.id` links token -> condition, and
  `marketDatas(where: {condition_in: [...]})` returns both outcome tokens
  per condition, so the yes/no pairing needs ~2 Goldsky requests per chunk
  of markets instead of one Gamma request per market.
- Caveat: subgraph CLOB notional can diverge wildly from Gamma `volumeNum`
  (pm_544097: $5,846 both tokens vs Gamma $95,320; pm_559700: $0 vs
  $85,005) — the probe's 96% reconciliation is not universal. The index
  uses the subgraph's PIT definition consistently.

Runs in bounded portions with the same exit protocol as the other ingest
modules (exit 3 = more work, 0 = complete) so scripts/run_backfill.sh
drives it unmodified.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .. import config
from .store import _write_part

OUT_DIR = config.DATA_DIR / "raw" / "polymarket"

BUCKETS = 16         # final sum-merge holds one bucket in RAM, never the sweep
FLUSH_PAGES = 500    # pages aggregated in RAM between shard commits
MIN_PAGE_SIZE = 100  # verified fast; a timeout below this is a real outage
TOKEN_MAP_CHUNK = 200

FILLS_QUERY = """query($first: Int!, $where: EnrichedOrderFilled_filter) {
  enrichedOrderFilleds(first: $first, orderBy: timestamp,
                       orderDirection: asc, where: $where) {
    id timestamp size market { id }
  }
}"""

# first: 1000 (the API max) on both: the implicit default is 100, which
# would silently truncate a 200-id chunk / its up-to-400 sibling rows.
CONDITIONS_QUERY = """query($ids: [ID!]) {
  marketDatas(first: 1000, where: {id_in: $ids}) { id condition { id } }
}"""

SIBLINGS_QUERY = """query($conds: [String!]) {
  marketDatas(first: 1000, where: {condition_in: $conds}) {
    id condition { id }
  }
}"""


class GoldskyQueryError(RuntimeError):
    pass


def _post(client: httpx.Client, query: str, variables: dict) -> dict:
    r = client.post(config.GOLDSKY_URL,
                    json={"query": query, "variables": variables})
    r.raise_for_status()
    j = r.json()
    if "errors" in j:  # GraphQL errors arrive as HTTP 200
        raise GoldskyQueryError(j["errors"][0].get("message", "graphql error"))
    return j["data"]


def _fetch_fills(client: httpx.Client, where: dict,
                 size: int) -> tuple[list[dict], int]:
    """Fetch one page, halving the page size on a store failure (the
    1000-row nested-join page times out deterministically, see docstring).
    Returns (rows, size_used) so the caller keeps the working size."""
    while True:
        try:
            data = _post(client, FILLS_QUERY, {"first": size, "where": where})
            return data["enrichedOrderFilleds"], size
        except GoldskyQueryError:
            if size <= MIN_PAGE_SIZE:
                raise
            size = max(size // 2, MIN_PAGE_SIZE)


def _where(cursor: dict | None) -> dict:
    if cursor is None:
        start = int(datetime.fromisoformat(config.BACKFILL_START)
                    .replace(tzinfo=timezone.utc).timestamp())
        return {"timestamp_gte": str(start)}
    return {"or": [{"timestamp_gt": cursor["ts"]},
                   {"timestamp": cursor["ts"], "id_gt": cursor["id"]}]}


class VolumeStore:
    """Checkpointed (token, day) aggregate shards, bucketed by token id so
    the final sum-merge holds one bucket in RAM. Shards commit before the
    cursor, but unlike MetaStore's rows, replayed *sums* can't be deduped
    away in finalize — so resume() deletes any shard newer than the
    committed sequence (a crash between shard and cursor writes) instead of
    letting the replayed pages double-count."""

    def __init__(self, out_dir: Path):
        self.final_path = out_dir / "volumes_by_token.parquet"
        self.parts_dir = out_dir / "volumes_parts"
        self.state_path = out_dir / "volumes_cursor.json"

    @property
    def complete(self) -> bool:
        return self.final_path.exists()

    def resume(self) -> tuple[dict | None, int, int]:
        state = (json.loads(self.state_path.read_text())
                 if self.state_path.exists()
                 else {"cursor": None, "seq": -1, "n": 0})
        if self.parts_dir.exists():
            for p in self.parts_dir.glob("b*/flush-*.parquet"):
                if int(p.stem.split("-")[1]) > state["seq"]:
                    p.unlink()
        return state["cursor"], state["seq"], state["n"]

    def commit(self, acc: dict, cursor: dict | None, seq: int,
               n: int) -> None:
        if acc:
            df = pd.DataFrame(
                [(tok, day, usd) for (tok, day), usd in acc.items()],
                columns=["token_id", "day", "notional_usd"])
            df["date"] = pd.to_datetime(df.pop("day") * 86400, unit="s")
            df["notional_usd"] = df["notional_usd"].astype("float64")
            df = df[["token_id", "date", "notional_usd"]]
            buckets = df["token_id"].map(lambda t: int(t) % BUCKETS)
            for b, grp in df.groupby(buckets):
                bdir = self.parts_dir / f"b{b:02d}"
                bdir.mkdir(parents=True, exist_ok=True)
                part = bdir / f"flush-{seq:06d}.parquet"
                tmp = part.with_name(part.name + ".tmp")
                grp.to_parquet(tmp, index=False)
                os.replace(tmp, part)
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_text(json.dumps({"cursor": cursor, "seq": seq, "n": n}))
        os.replace(tmp, self.state_path)

    def finalize(self) -> None:
        writer = None
        tmp = self.final_path.with_name(self.final_path.name + ".tmp")
        bdirs = (sorted(self.parts_dir.glob("b*"))
                 if self.parts_dir.exists() else [])
        try:
            for bdir in bdirs:
                files = sorted(bdir.glob("flush-*.parquet"))
                if not files:
                    continue
                df = pd.concat([pd.read_parquet(p) for p in files],
                               ignore_index=True)
                # A (token, day)'s fills may span a flush boundary: SUM the
                # duplicates — first-occurrence dedup would drop notional.
                df = (df.groupby(["token_id", "date"], as_index=False)
                        ["notional_usd"].sum())
                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(tmp, table.schema)
                writer.write_table(table.cast(writer.schema))
        finally:
            if writer is not None:
                writer.close()
        if writer is not None:
            os.replace(tmp, self.final_path)
        else:  # sweep saw zero fills
            pd.DataFrame({"token_id": pd.Series(dtype=str),
                          "date": pd.Series(dtype="datetime64[ns]"),
                          "notional_usd": pd.Series(dtype="float64")}
                         ).to_parquet(self.final_path, index=False)
        # State first, like MetaStore: a stale cursor must not survive.
        self.state_path.unlink(missing_ok=True)
        for bdir in bdirs:
            for p in bdir.glob("flush-*.parquet"):
                p.unlink()
            bdir.rmdir()
        if self.parts_dir.exists():
            self.parts_dir.rmdir()


def sweep(client: httpx.Client, store: VolumeStore,
          max_pages: int | None = None) -> bool:
    """Advance the global fill sweep by up to max_pages; True on exhausted."""
    cursor, seq, n = store.resume()
    size = config.GOLDSKY_PAGE_SIZE
    acc: dict[tuple[str, int], float] = {}
    pages = 0
    while max_pages is None or pages < max_pages:
        rows, size = _fetch_fills(client, _where(cursor), size)
        for f in rows:
            key = (f["market"]["id"], int(f["timestamp"]) // 86400)
            acc[key] = acc.get(key, 0.0) + int(f["size"]) / 1e6
        n += len(rows)
        if rows:
            cursor = {"ts": rows[-1]["timestamp"], "id": rows[-1]["id"]}
        pages += 1
        done = len(rows) < size
        if done or pages % FLUSH_PAGES == 0:
            seq += 1
            store.commit(acc, cursor, seq, n)
            acc = {}
            print(f"  volume sweep: {n} fills through "
                  f"{cursor['ts'] if cursor else 'start'}", flush=True)
        if done:
            store.finalize()
            return True
        time.sleep(config.GOLDSKY_SLEEP_S)
    seq += 1
    store.commit(acc, cursor, seq, n)
    return False


class TokenMapStore:
    """Checkpointed token -> market_id enrichment. Mapping rows are
    idempotent facts (unlike volume sums), so replayed chunks after a crash
    between shard and cursor are simply deduped in finalize."""

    def __init__(self, out_dir: Path):
        self.final_path = out_dir / "token_map.parquet"
        self.parts_dir = out_dir / "token_map_parts"
        self.state_path = out_dir / "token_map_cursor.json"

    @property
    def complete(self) -> bool:
        return self.final_path.exists()

    def resume(self) -> int:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())["pos"]
        return 0

    def commit(self, df: pd.DataFrame, pos: int) -> None:
        if not df.empty:
            _write_part(df, self.parts_dir)
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_text(json.dumps({"pos": pos}))
        os.replace(tmp, self.state_path)

    def finalize(self) -> None:
        parts = (sorted(self.parts_dir.glob("part-*.parquet"))
                 if self.parts_dir.exists() else [])
        # Small by construction (<= 2 rows per kept market): in-RAM merge.
        df = (pd.concat([pd.read_parquet(p) for p in parts],
                        ignore_index=True).drop_duplicates("token_id")
              if parts else
              pd.DataFrame({"token_id": pd.Series(dtype=str),
                            "market_id": pd.Series(dtype=str)}))
        tmp = self.final_path.with_name(self.final_path.name + ".tmp")
        df.to_parquet(tmp, index=False)
        os.replace(tmp, self.final_path)
        self.state_path.unlink(missing_ok=True)
        for p in parts:
            p.unlink()
        if self.parts_dir.exists():
            self.parts_dir.rmdir()


def build_token_map(client: httpx.Client, store: TokenMapStore,
                    meta: pd.DataFrame) -> None:
    meta = meta.sort_values("market_id").reset_index(drop=True)
    pos = store.resume()
    while pos < len(meta):
        chunk = meta.iloc[pos:pos + TOKEN_MAP_CHUNK]
        by_yes = dict(zip(chunk["yes_token_id"], chunk["market_id"]))
        data = _post(client, CONDITIONS_QUERY, {"ids": list(by_yes)})
        cond_to_mkt = {md["condition"]["id"]: by_yes[md["id"]]
                       for md in data["marketDatas"] if md.get("condition")}
        time.sleep(config.GOLDSKY_SLEEP_S)
        # Yes tokens map exactly from our own metadata even where Goldsky
        # has no marketData entity; siblings extend the map where it does.
        rows = dict(by_yes)
        if cond_to_mkt:
            data = _post(client, SIBLINGS_QUERY, {"conds": list(cond_to_mkt)})
            for md in data["marketDatas"]:
                rows[md["id"]] = cond_to_mkt[md["condition"]["id"]]
            time.sleep(config.GOLDSKY_SLEEP_S)
        pos += len(chunk)
        store.commit(pd.DataFrame({"token_id": list(rows),
                                   "market_id": list(rows.values())}), pos)
        print(f"  token map: {pos}/{len(meta)} markets", flush=True)
    store.finalize()


def assemble(out_dir: Path) -> None:
    """volumes_by_token + token_map -> volumes.parquet (market_id, date,
    daily_notional_usd), both outcome tokens summed per market-day."""
    token_map = pd.read_parquet(out_dir / "token_map.parquet")
    tok_to_mkt = dict(zip(token_map["token_id"], token_map["market_id"]))
    pf = pq.ParquetFile(out_dir / "volumes_by_token.parquet")
    frames = []
    for batch in pf.iter_batches(65536):
        df = batch.to_pandas()
        df["market_id"] = df["token_id"].map(tok_to_mkt)
        df = df.dropna(subset=["market_id"])
        if not df.empty:
            frames.append(df.groupby(["market_id", "date"], as_index=False)
                            ["notional_usd"].sum())
    vol = (pd.concat(frames, ignore_index=True)
             .groupby(["market_id", "date"], as_index=False)
             ["notional_usd"].sum()
           if frames else
           pd.DataFrame({"market_id": pd.Series(dtype=str),
                         "date": pd.Series(dtype="datetime64[ns]"),
                         "notional_usd": pd.Series(dtype="float64")}))
    vol = vol.rename(columns={"notional_usd": "daily_notional_usd"})
    vol["daily_notional_usd"] = vol["daily_notional_usd"].astype("float64")
    vol = vol.sort_values(["market_id", "date"]).reset_index(drop=True)
    final = out_dir / "volumes.parquet"
    tmp = final.with_name(final.name + ".tmp")
    vol.to_parquet(tmp, index=False)
    os.replace(tmp, final)
    print(f"volumes.parquet: {len(vol)} market-days")


def main(pages: int | None = None) -> bool:
    """One portion of work. Returns True when volumes.parquet is built."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if (OUT_DIR / "volumes.parquet").exists():
        print("polymarket volume ingestion complete")
        return True
    vol = VolumeStore(OUT_DIR)
    with httpx.Client(timeout=60) as client:
        if not vol.complete:
            sweep(client, vol, pages)
            return False  # mapping starts in a fresh process either way
        if not (OUT_DIR / "markets.parquet").exists():
            # The metadata crawl (ingest.polymarket) hasn't finalized; poll
            # gently instead of hot-looping the driver.
            print("waiting on markets.parquet for token mapping")
            time.sleep(60)
            return False
        tmap = TokenMapStore(OUT_DIR)
        if not tmap.complete:
            meta = pd.read_parquet(OUT_DIR / "markets.parquet",
                                   columns=["market_id", "yes_token_id"])
            build_token_map(client, tmap, meta)
    assemble(OUT_DIR)
    print("polymarket volume ingestion complete")
    return True


if __name__ == "__main__":
    import argparse
    import sys
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=5000,
                    help="sweep pages per portion (0 = unbounded)")
    a = ap.parse_args()
    sys.exit(0 if main(a.pages or None) else 3)
