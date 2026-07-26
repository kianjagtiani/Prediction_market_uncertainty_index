"""Bounded-memory price storage shared by both ingest modules.

Holding every price frame in memory and rewriting one growing parquet at
each checkpoint made ingestion RSS grow with the full dataset; part-files
keep the in-memory buffer at checkpoint-interval size and defer the single
merge to finalize().
"""
import os
from pathlib import Path

import pandas as pd


class PriceStore:
    def __init__(self, out_dir: Path):
        self.final_path = out_dir / "prices.parquet"
        self.parts_dir = out_dir / "prices_parts"
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
        return done

    def append(self, df: pd.DataFrame) -> None:
        if not df.empty:
            self._buffer.append(df)

    def checkpoint(self) -> None:
        if not self._buffer:
            return
        self.parts_dir.mkdir(parents=True, exist_ok=True)
        taken = [int(p.stem.split("-")[1])
                 for p in self.parts_dir.glob("part-*.parquet")]
        n = max(taken, default=-1) + 1  # gaps must not cause overwrites
        pd.concat(self._buffer, ignore_index=True).to_parquet(
            self.parts_dir / f"part-{n:05d}.parquet", index=False)
        self._buffer = []

    def finalize(self) -> None:
        self.checkpoint()
        parts = [p for p in self._sources() if p != self.final_path]
        if parts:
            # Dedup guards against a rerun after a crash between the merge
            # write and the part unlinks; the temp + replace keeps the sole
            # copy of prior data intact if this write itself dies.
            merged = (
                pd.concat([pd.read_parquet(p) for p in self._sources()],
                          ignore_index=True)
                .drop_duplicates(["market_id", "date"], keep="last")
            )
            tmp = self.final_path.with_name(self.final_path.name + ".tmp")
            merged.to_parquet(tmp, index=False)
            os.replace(tmp, self.final_path)
        if self.parts_dir.exists():
            for p in self.parts_dir.glob("part-*.parquet"):
                p.unlink()
            self.parts_dir.rmdir()
