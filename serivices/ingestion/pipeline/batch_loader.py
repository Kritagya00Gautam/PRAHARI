from __future__ import annotations
import asyncio
import csv
import json
import logging
import re
import time
from dataclasses import dataclass, fields
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence, Type
from prahari_schemas import TelemetryData  # packages/schemas
from ingestion.pipeline.stream_publisher import IngestionStreamPublisher
from ingestion.validators.schema_validator import Reject, validate_batch

log = logging.getLogger("prahari.ingestion.batch_loader")

UNPARSEABLE_KEY = "__unparseable__"

@dataclass
class BatchReport:
    total: int = 0
    schema_rejected: int = 0
    stage_dropped: int = 0
    published: int = 0
    duplicates: int = 0
    publish_failed: int = 0
    dead_lettered: int = 0
    dlq_failed: int = 0
    duration_s: float = 0.0

    def merge(self, other: "BatchReport") -> None:
        for f in fields(self):
            setattr(self, f.name, getattr(self, f.name) + getattr(other, f.name))

    @property
    def clean(self) -> bool:
        return self.publish_failed == 0 and self.dlq_failed == 0

    def summary(self) -> str:
        return (
            f"total={self.total} published={self.published} duplicates={self.duplicates} "
            f"schema_rejected={self.schema_rejected} stage_dropped={self.stage_dropped} "
            f"publish_failed={self.publish_failed} dead_lettered={self.dead_lettered} "
            f"dlq_failed={self.dlq_failed} in {self.duration_s:.2f}s"
        )

class BatchLoader:
    def __init__(
        self,
        publisher: IngestionStreamPublisher,
        stages: Sequence[Stage] = (),
        model: Type[Any] = TelemetryData,
        chunk_size: int = 500,
        failed_log: Optional[Path] = None,
    ):
        self.publisher = publisher
        self.stages = list(stages)
        self.model = model
        self.chunk_size = chunk_size
        self.failed_log = Path(failed_log) if failed_log else None

    
    @staticmethod
    def iter_rows(path: Path) -> Iterator[dict]:
       
        suffix = path.suffix.lower()

        if suffix == ".csv":
            with open(path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    # Empty cells become None; extra columns (key None) are ignored.
                    yield {k: (v if v != "" else None) for k, v in row.items() if k is not None}

        elif suffix in (".jsonl", ".ndjson"):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        yield {UNPARSEABLE_KEY: line[:200]}
                        continue
                    yield obj if isinstance(obj, dict) else {UNPARSEABLE_KEY: line[:200]}

        elif suffix == ".json":
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for obj in (data if isinstance(data, list) else [data]):
                yield obj if isinstance(obj, dict) else {UNPARSEABLE_KEY: str(obj)[:200]}

        else:
            raise ValueError(f"unsupported file type: {suffix!r} (use .csv, .jsonl, .ndjson, .json)")

    async def load_file(self, path: str | Path, source: Optional[str] = None) -> BatchReport:
       
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(path)
        source = source or re.sub(r"[^A-Za-z0-9_-]", "_", path.stem)[:64] or "batch"

        report = BatchReport()
        chunks = self.iter_chunks(path)
        while True:
            chunk = await asyncio.to_thread(next, chunks, None)
            if chunk is None:
                break
            report.merge(await self.process_rows(chunk, source))
            log.info("%s: %d rows processed (published=%d rejected=%d)",
                     path.name, report.total, report.published, report.schema_rejected)
        return report

    async def process_rows(self, rows: list[dict], source: str) -> BatchReport:
        started = time.monotonic()
        report = BatchReport(total=len(rows))
 
   
        valid, rejects = validate_batch(rows, self.model, source)
        report.schema_rejected = len(rejects)
 

        kept = []
        for rec in valid:
            out = self._apply_stages(rec, source)
            if out is None:
                report.stage_dropped += 1
            else:
                kept.append(out)
 
        if kept:
            result = await self.publisher.publish_batch(kept)
            report.published = result.published
            report.duplicates = result.duplicates
            report.publish_failed = len(result.failed)
            if result.failed:
                self._spill(result.failed, source)
 
        await self._dead_letter(rejects, report)
 
        report.duration_s = time.monotonic() - started
        return report

    def _apply_stages(self, rec: TelemetryData, source: str) -> Optional[TelemetryData]:
        for stage in self.stages:
            try:
                rec = stage(rec)
            except Exception:
                # A buggy stage must not take down the pipeline; the record is dropped.
                log.exception("stage %s raised for a record from %s; dropping it",
                              getattr(stage, "__name__", stage), source)
                return None
            if rec is None:
                return None
        return rec
 
    async def _dead_letter(self, rejects: list[Reject], report: BatchReport) -> None:
        if not rejects:
            return
 
        async def send(rej: Reject) -> None:
            raw = json.dumps(rej.raw, default=str).encode("utf-8")
            reason = "; ".join(f"{e.path}: {e.message}" for e in rej.errors) or "unknown"
            await self.publisher.publish_dead_letter(rej.source, raw, reason)
 
        outcomes = await asyncio.gather(*(send(r) for r in rejects), return_exceptions=True)
        for out in outcomes:
            if isinstance(out, Exception):
                report.dlq_failed += 1
                log.error("dead-letter publish failed: %s", out)
            else:
                report.dead_lettered += 1

    def _spill(self, failed: list[tuple[Any, str]], source: str) -> None:
        """Records that could not be published are written locally so they can be replayed
        with `main.py backfill <failed_log>`."""
        if self.failed_log is None:
            log.error("%d records from %s failed to publish and no failed_log is set",
                      len(failed), source)
            return
        with open(self.failed_log, "a", encoding="utf-8") as f:
            for rec, _reason in failed:
                f.write(json.dumps(rec.model_dump(mode="json")) + "\n")
        log.warning("spilled %d unpublished records to %s", len(failed), self.failed_log)
    
