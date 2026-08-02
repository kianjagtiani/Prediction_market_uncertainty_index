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
import shutil
import time
import zlib
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
MAX_MARKETS_WAITS = 10  # 60s polls for markets.parquet before giving up
ASSEMBLE_BATCH_ROWS = 65_536  # rows held in RAM per assemble pass-1 batch

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


class SweepIncompleteError(RuntimeError):
    """The sweep ran out of rows without reaching the present. Raised rather
    than finalized, because a finalized partial sweep is indistinguishable
    on disk from a complete one and normalize would read its missing days as
    "$0 traded"."""


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


def _write_json(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, path)


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
        self.manifest_path = out_dir / "volumes_manifest.json"

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
        _write_json(self.state_path, {"cursor": cursor, "seq": seq, "n": n})

    def finalize(self, n_fills: int) -> None:
        """Merge the shards and record what the sweep actually covered.

        The manifest is the only thing downstream can use to tell a complete
        sweep from a truncated one: normalize refuses to read "no fills" as
        "$0 traded" outside [first_date, last_date]."""
        writer = None
        tmp = self.final_path.with_name(self.final_path.name + ".tmp")
        bdirs = (sorted(self.parts_dir.glob("b*"))
                 if self.parts_dir.exists() else [])
        days: list[pd.Timestamp] = []
        n_tokens = 0
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
                # Buckets partition on token id, so per-bucket distinct
                # counts add up and the day bounds are a running min/max.
                n_tokens += df["token_id"].nunique()
                days += [df["date"].min(), df["date"].max()]
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
        _write_json(self.manifest_path, {
            "first_date": min(days).isoformat() if days else None,
            "last_date": max(days).isoformat() if days else None,
            "n_fills": n_fills,
            "n_tokens": n_tokens,
        })
        # State first, like MetaStore: a stale cursor must not survive.
        self.state_path.unlink(missing_ok=True)
        for bdir in bdirs:
            for p in bdir.glob("flush-*.parquet"):
                p.unlink()
            bdir.rmdir()
        if self.parts_dir.exists():
            self.parts_dir.rmdir()


def _check_exhausted(cursor: dict | None, n_fills: int) -> None:
    """A sweep is only complete if it swept a plausible number of fills AND
    its cursor reached the present. Anything else finalizes a partial
    history that normalize would launder into "$0 traded"."""
    if n_fills < config.GOLDSKY_MIN_FILLS:
        raise SweepIncompleteError(
            f"sweep returned an empty page after only {n_fills} fills "
            f"(expected >= {config.GOLDSKY_MIN_FILLS}); the subgraph is "
            f"degraded or lagging. Cursor kept — rerun to resume.")
    lag = time.time() - int(cursor["ts"])
    if lag > config.GOLDSKY_MAX_CURSOR_LAG_S:
        raise SweepIncompleteError(
            f"sweep returned an empty page {lag / 86400:.1f} days behind "
            f"now (limit {config.GOLDSKY_MAX_CURSOR_LAG_S / 3600:.0f}h); the "
            f"remaining history is missing, not absent. Cursor kept.")


def sweep(client: httpx.Client, store: VolumeStore,
          max_pages: int | None = None) -> bool:
    """Advance the global fill sweep by up to max_pages; True on exhausted.

    Exhaustion means an EMPTY page, not a short one. The subgraph fails
    non-deterministically under load (see module docstring), and a short
    page from a degraded store used to finalize the sweep, unlink the
    cursor and leave `complete` permanently True with 30% of the history —
    at which point normalize's fillna(0.0) turns the rest into "$0 traded"
    and Polymarket silently drops out of the index. One extra request per
    sweep is the price of not being able to confuse the two.
    """
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
        done = not rows
        if done or pages % FLUSH_PAGES == 0:
            seq += 1
            store.commit(acc, cursor, seq, n)
            acc = {}
            print(f"  volume sweep: {n} fills through "
                  f"{cursor['ts'] if cursor else 'start'}", flush=True)
        if done:
            _check_exhausted(cursor, n)
            store.finalize(n)
            return True
        time.sleep(config.GOLDSKY_SLEEP_S)
    seq += 1
    store.commit(acc, cursor, seq, n)
    return False


def _catalog_fingerprint(meta: pd.DataFrame) -> dict:
    """Identity of the sorted markets.parquet the cursor position indexes
    into. `pos` is a row offset, and markets.parquet is re-crawled from zero
    as a documented workflow and os.replace'd by the metadata finalize while
    this runs; without a binding, a resumed run starts at `pos` in a
    different catalog and silently never maps the markets before it."""
    return {"n_markets": int(len(meta)),
            "last_market_id": (str(meta["market_id"].iloc[-1])
                               if len(meta) else None)}


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

    def resume(self, fingerprint: dict) -> int:
        if not self.state_path.exists():
            return 0
        state = json.loads(self.state_path.read_text())
        if all(state.get(k) == v for k, v in fingerprint.items()):
            return state["pos"]
        print(f"  token map: markets.parquet changed under the cursor "
              f"(was {state.get('n_markets')} markets ending "
              f"{state.get('last_market_id')}, now {fingerprint}); "
              f"restarting from zero", flush=True)
        for p in self.parts_dir.glob("part-*.parquet"):
            p.unlink()
        self.state_path.unlink()
        return 0

    def commit(self, df: pd.DataFrame, pos: int, fingerprint: dict) -> None:
        if not df.empty:
            _write_part(df, self.parts_dir)
        _write_json(self.state_path, {"pos": pos, **fingerprint})

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
    fingerprint = _catalog_fingerprint(meta)
    pos = store.resume(fingerprint)
    while pos < len(meta):
        raw = meta.iloc[pos:pos + TOKEN_MAP_CHUNK]
        # A missing yes_token_id becomes a NaN dict key and a repeated one
        # silently collapses two markets into one; count both instead.
        chunk = raw.dropna(subset=["yes_token_id"]).drop_duplicates(
            "yes_token_id")
        if len(chunk) < len(raw):
            print(f"  token map: {len(raw) - len(chunk)}/{len(raw)} markets "
                  f"in this chunk have a missing or repeated yes_token_id "
                  f"and stay unmapped", flush=True)
        by_yes = dict(zip(chunk["yes_token_id"], chunk["market_id"]))
        cond_to_mkt = {}
        if by_yes:
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
        pos += len(raw)
        store.commit(pd.DataFrame({"token_id": list(rows),
                                   "market_id": list(rows.values())}),
                     pos, fingerprint)
        print(f"  token map: {pos}/{len(meta)} markets", flush=True)
    store.finalize()


def _bucket(key: str) -> int:
    """Stable bucket for an arbitrary id string. crc32, not int(), so a
    non-decimal id cannot raise mid-accumulation."""
    return zlib.crc32(str(key).encode()) % BUCKETS


def assemble(out_dir: Path) -> None:
    """volumes_by_token + token_map -> volumes.parquet (market_id, date,
    daily_notional_usd), both outcome tokens summed per market-day.

    Two passes over disk, bucketed by market_id, so peak RAM is one bucket.
    The single-pass version accumulated every batch's aggregate in a list
    and concatenated at the end; on an 8 GB machine that is a plausible
    2 GB watchdog kill, and a kill here exits 143, which the driver reads
    as progress — relaunch, both stores still complete, assemble again,
    killed again, forever with no output and no failure signal.

    Rows come out grouped by bucket rather than globally sorted by
    market_id. Deterministic, just not lexicographic.
    """
    token_map = pd.read_parquet(out_dir / "token_map.parquet")
    tok_to_mkt = dict(zip(token_map["token_id"], token_map["market_id"]))
    parts_dir = out_dir / "assemble_parts"
    shutil.rmtree(parts_dir, ignore_errors=True)

    pf = pq.ParquetFile(out_dir / "volumes_by_token.parquet")
    for i, batch in enumerate(pf.iter_batches(ASSEMBLE_BATCH_ROWS)):
        df = batch.to_pandas()
        df["market_id"] = df["token_id"].map(tok_to_mkt)
        df = df.dropna(subset=["market_id"])
        if df.empty:
            continue
        df = (df.groupby(["market_id", "date"], as_index=False)
                ["notional_usd"].sum())
        for b, grp in df.groupby(df["market_id"].map(_bucket)):
            bdir = parts_dir / f"b{b:02d}"
            bdir.mkdir(parents=True, exist_ok=True)
            grp.to_parquet(bdir / f"batch-{i:06d}.parquet", index=False)

    final = out_dir / "volumes.parquet"
    tmp = final.with_name(final.name + ".tmp")
    writer, n_rows = None, 0
    bdirs = sorted(parts_dir.glob("b*")) if parts_dir.exists() else []
    try:
        for bdir in bdirs:
            df = pd.concat([pd.read_parquet(p)
                            for p in sorted(bdir.glob("batch-*.parquet"))],
                           ignore_index=True)
            # A market's two outcome tokens can land in different token
            # buckets and different batches: SUM across all of them.
            df = (df.groupby(["market_id", "date"], as_index=False)
                    ["notional_usd"].sum()
                    .rename(columns={"notional_usd": "daily_notional_usd"})
                    .sort_values(["market_id", "date"]))
            df["daily_notional_usd"] = df["daily_notional_usd"].astype("float64")
            n_rows += len(df)
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp, table.schema)
            writer.write_table(table.cast(writer.schema))
    finally:
        if writer is not None:
            writer.close()
    if writer is not None:
        os.replace(tmp, final)
    else:
        pd.DataFrame({"market_id": pd.Series(dtype=str),
                      "date": pd.Series(dtype="datetime64[ns]"),
                      "daily_notional_usd": pd.Series(dtype="float64")}
                     ).to_parquet(final, index=False)
    shutil.rmtree(parts_dir, ignore_errors=True)
    print(f"volumes.parquet: {n_rows} market-days")


def _sweep_vintage(out_dir: Path) -> str:
    """The data vintage a coverage verdict was formed against: the sweep's
    last covered day, or today if there is no manifest to name one."""
    path = out_dir / "volumes_manifest.json"
    if path.exists():
        last = json.loads(path.read_text()).get("last_date")
        if last:
            return str(last)[:10]
    return datetime.now(timezone.utc).date().isoformat()


COVERAGE_COLUMNS = ["market_id", "gamma_lifetime_volume_usd",
                    "subgraph_notional_both_legs_usd",
                    "subgraph_notional_usd", "coverage_single_leg",
                    "covered", "vintage"]


def write_coverage_report(out_dir: Path) -> None:
    """Per-market swept notional vs Gamma's lifetime volume.

    Catches the known coverage hole: the subgraph does not index negRisk or
    legacy AMM fills, and without this normalize's zero-fill would convert
    "not indexed by this subgraph" into "genuinely zero notional", dropping
    those markets under the liquidity floor with nothing anywhere showing
    it. It is also the evidence trail for the open I8(a) reconciliation.

    Units. The numerator sums BOTH outcome tokens (`assemble`); Gamma's
    volumeNum is single-legged. `subgraph_notional_usd` divides the swept
    sum by config.PM_TOKEN_LEGS_PER_FILL so `coverage_single_leg` is a
    fraction of the same quantity the denominator measures. Kalshi's
    daily_notional_usd (volume_fp * close_prob) is single-legged too, which
    is what makes the shared eligibility floor meaningful.

    Verdicts are FROZEN per market. `total_volume_usd` is Gamma's lifetime
    volume as of whenever the metadata crawl ran, so recomputing it would
    let volume traded *after* a published day flip that day's eligibility on
    the next rebuild. A market already in the report keeps its verdict and
    its `vintage`; only markets seen for the first time are classified. The
    file is therefore a vintaged artefact and belongs in version control
    alongside the published index.
    """
    path = out_dir / "volumes_coverage.csv"
    vol = pd.read_parquet(out_dir / "volumes.parquet",
                          columns=["market_id", "daily_notional_usd"])
    meta = pd.read_parquet(out_dir / "markets.parquet",
                           columns=["market_id", "total_volume_usd"])
    prior = pd.read_csv(path) if path.exists() else None
    if prior is not None and not set(COVERAGE_COLUMNS) <= set(prior.columns):
        print(f"  coverage: {path.name} predates the current schema; every "
              f"market is reclassified against this vintage", flush=True)
        prior = None
    known = set(prior["market_id"]) if prior is not None else set()

    fresh = meta[~meta["market_id"].isin(known)]
    swept = vol.groupby("market_id")["daily_notional_usd"].sum()
    gamma = fresh["total_volume_usd"].astype("float64")
    both = fresh["market_id"].map(swept).fillna(0.0).astype("float64")
    single = both / config.PM_TOKEN_LEGS_PER_FILL
    rep = pd.DataFrame({
        "market_id": fresh["market_id"].values,
        "gamma_lifetime_volume_usd": gamma.values,
        "subgraph_notional_both_legs_usd": both.values,
        "subgraph_notional_usd": single.values,
        # NaN coverage (no Gamma volume to compare against) is not evidence
        # of coverage, and NaN >= x is already False.
        "coverage_single_leg": (single / gamma.where(gamma > 0)).values,
        "vintage": _sweep_vintage(out_dir),
    })
    rep["covered"] = (rep["coverage_single_leg"]
                      >= config.PM_MIN_SUBGRAPH_COVERAGE)
    out = (pd.concat([prior, rep], ignore_index=True)[COVERAGE_COLUMNS]
           if prior is not None else rep[COVERAGE_COLUMNS])
    tmp = path.with_name(path.name + ".tmp")
    out.to_csv(tmp, index=False)
    os.replace(tmp, path)

    bad = out.loc[~out["covered"].astype(bool)]
    total = float(out["gamma_lifetime_volume_usd"].sum())
    share = (float(bad["gamma_lifetime_volume_usd"].sum()) / total
             if total else 0.0)
    print(f"volumes_coverage.csv: {len(bad)}/{len(out)} PM markets swept "
          f"less than {config.PM_MIN_SUBGRAPH_COVERAGE:.0%} of their Gamma "
          f"lifetime volume (single-leg), {share:.1%} of catalog volume; "
          f"their market-days stay NaN, not $0"
          + (" -- CHECK negRisk/AMM coverage" if len(bad) else ""))


def _wait_for_markets(out_dir: Path) -> None:
    """Poll gently for the metadata crawl, but not forever. Exit 3 resets
    the driver's failure counter, so an unbounded wait spins one process a
    minute indefinitely with no failure and no alert — e.g. when only the
    `run_backfill.sh polymarket_volume` line was run."""
    path = out_dir / "volume_wait.json"
    waits = (json.loads(path.read_text())["waits"] + 1
             if path.exists() else 1)
    if waits > MAX_MARKETS_WAITS:
        raise RuntimeError(
            f"markets.parquet still absent after {MAX_MARKETS_WAITS} waits; "
            f"run scripts/run_backfill.sh polymarket first")
    _write_json(path, {"waits": waits})
    print(f"waiting on markets.parquet for token mapping "
          f"({waits}/{MAX_MARKETS_WAITS})", flush=True)
    time.sleep(60)


def main(pages: int | None = None) -> bool:
    """One portion of work. Returns True when both outputs are built.

    Completion needs the coverage report as well as volumes.parquet: it is
    written after assemble publishes the parquet, so a kill in between (the
    RSS watchdog exits 143, which the driver reads as progress) would
    otherwise leave a permanent state where the next portion short-circuits
    to "complete", the report never exists, and normalize's zero-fill runs
    unguarded — the exact laundering the report is there to prevent.
    Re-running assemble to recover is idempotent and cheap.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if ((OUT_DIR / "volumes.parquet").exists()
            and (OUT_DIR / "volumes_coverage.csv").exists()):
        print("polymarket volume ingestion complete")
        return True
    vol = VolumeStore(OUT_DIR)
    with httpx.Client(timeout=60) as client:
        if not vol.complete:
            sweep(client, vol, pages)
            return False  # mapping starts in a fresh process either way
        if not (OUT_DIR / "markets.parquet").exists():
            _wait_for_markets(OUT_DIR)
            return False
        (OUT_DIR / "volume_wait.json").unlink(missing_ok=True)
        tmap = TokenMapStore(OUT_DIR)
        if not tmap.complete:
            meta = pd.read_parquet(OUT_DIR / "markets.parquet",
                                   columns=["market_id", "yes_token_id"])
            build_token_map(client, tmap, meta)
    assemble(OUT_DIR)
    write_coverage_report(OUT_DIR)
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
