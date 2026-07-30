"""Bounded-memory storage shared by both ingest modules.

Every phase here is designed around one rule: no step may hold data
proportional to the full catalog. Pages and price frames accumulate in
parquet part-files, crawl cursors are committed to disk so a fresh process
resumes exactly where the last one exited, and merges stream one part at a
time through a ParquetWriter. Peak memory is one flush interval or one
part-file, never the dataset.
"""
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def _stream_merge(sources: list[Path], final_path: Path) -> None:
    """Merge parquet files into final_path one file at a time, keeping the
    first occurrence of each market_id. Callers order sources so the
    authoritative copy comes first."""
    seen: set[str] = set()
    writer = None
    tmp = final_path.with_name(final_path.name + ".tmp")
    try:
        for path in sources:
            df = pd.read_parquet(path)
            df = df[~df["market_id"].isin(seen)]
            if df.empty:
                continue
            seen.update(df["market_id"].unique())
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp, table.schema)
            else:
                table = table.cast(writer.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if writer is not None:
        os.replace(tmp, final_path)


def _next_part(parts_dir: Path) -> Path:
    parts_dir.mkdir(parents=True, exist_ok=True)
    taken = [int(p.stem.split("-")[1]) for p in parts_dir.glob("part-*.parquet")]
    n = max(taken, default=-1) + 1  # gaps must not cause overwrites
    return parts_dir / f"part-{n:05d}.parquet"


class MetaStore:
    """Checkpointed metadata crawl. Shards are committed before the cursor,
    so a crash in between replays a few pages under the old cursor; the
    replayed rows are deduped in finalize()."""

    def __init__(self, out_dir: Path):
        self.final_path = out_dir / "markets.parquet"
        self.parts_dir = out_dir / "markets_parts"
        self.state_path = out_dir / "markets_cursor.json"

    @property
    def complete(self) -> bool:
        return self.final_path.exists()

    def resume(self) -> tuple[str | None, int]:
        if self.state_path.exists():
            s = json.loads(self.state_path.read_text())
            return s["cursor"], s["n_seen"]
        return None, 0

    def commit(self, df: pd.DataFrame, cursor: str | None, n_seen: int) -> None:
        if not df.empty:
            df.to_parquet(_next_part(self.parts_dir), index=False)
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_text(json.dumps({"cursor": cursor, "n_seen": n_seen}))
        os.replace(tmp, self.state_path)

    def finalize(self, columns: list[str]) -> None:
        parts = sorted(self.parts_dir.glob("part-*.parquet")) \
            if self.parts_dir.exists() else []
        _stream_merge(parts, self.final_path)
        if not self.final_path.exists():  # catalog had no keepable rows
            pd.DataFrame(columns=columns).to_parquet(self.final_path, index=False)
        for p in parts:
            p.unlink()
        if self.parts_dir.exists():
            self.parts_dir.rmdir()
        self.state_path.unlink(missing_ok=True)


class PriceStore:
    def __init__(self, out_dir: Path):
        self.final_path = out_dir / "prices.parquet"
        self.parts_dir = out_dir / "prices_parts"
        self.no_data_path = out_dir / "no_data_ids.txt"
        self._buffer: list[pd.DataFrame] = []

    def _sources(self) -> list[Path]:
        legacy = [self.final_path] if self.final_path.exists() else []
        parts = sorted(self.parts_dir.glob("part-*.parquet")) \
            if self.parts_dir.exists() else []
        return legacy + parts

    def done_ids(self) -> set[str]:
        done: set[str] = set()
        for path in self._sources():
            done.update(pd.read_parquet(path, columns=["market_id"])["market_id"])
        if self.no_data_path.exists():
            done.update(line for line in
                        self.no_data_path.read_text().splitlines() if line)
        return done

    def append(self, df: pd.DataFrame) -> None:
        if not df.empty:
            self._buffer.append(df)

    def mark_no_data(self, market_id: str) -> None:
        # Markets that yield nothing (404s, empty candle histories) must
        # still count as done, or a portioned run refetches them at the
        # head of every todo list and never progresses past them.
        with open(self.no_data_path, "a") as f:
            f.write(market_id + "\n")

    def checkpoint(self) -> None:
        if not self._buffer:
            return
        pd.concat(self._buffer, ignore_index=True).to_parquet(
            _next_part(self.parts_dir), index=False)
        self._buffer = []

    def finalize(self) -> None:
        self.checkpoint()
        parts = [p for p in self._sources() if p != self.final_path]
        if parts:
            # Parts before legacy: after a crash between the merge write and
            # the part unlinks, the parts are the authoritative copy. A
            # market's rows always live in exactly one source file, so
            # first-occurrence dedup by market_id is exact.
            legacy = [self.final_path] if self.final_path.exists() else []
            _stream_merge(parts + legacy, self.final_path)
            for p in parts:
                p.unlink()
            self.parts_dir.rmdir()
