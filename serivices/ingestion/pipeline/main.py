from __future__ import annotations
import argparse
import asyncio
import logging
import os
import signal
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from ingestion.connectors.base import BaseConnector, Observation
from ingestion.pipeline.batch_loader import BatchLoader, BatchReport, Stage
from ingestion.pipeline.stream_publisher import IngestionStreamPublisher, PublisherConfig

log = logging.getLogger("prahari.ingestion.main")

ConnectorFactory = Callable[[], BaseConnector]

@dataclass
class AppConfig:
    publisher: PublisherConfig
    batch_size: int = 200
    flush_interval_s: float = 2.0
    queue_max: int = 5000
    failed_log: Path = Path("failed_publish.jsonl")
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "AppConfig":
        urls = [u.strip() for u in os.getenv("NATS_URL", "nats://localhost:4222").split(",") if u.strip()]
        return cls(
            publisher=PublisherConfig(urls=urls),
            batch_size=int(os.getenv("BATCH_SIZE", "200")),
            flush_interval_s=float(os.getenv("FLUSH_INTERVAL_S", "2.0")),
            queue_max=int(os.getenv("QUEUE_MAX", "5000")),
            failed_log=Path(os.getenv("FAILED_LOG", "failed_publish.jsonl")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),)

def build_connectors() -> list[ConnectorFactory]:
    """Return one factory per connector. A factory (not an instance) lets the
    supervisor build a fresh connector after a crash.
 
    Example (adjust class names and configs to your own connector files):
 
        from ingestion.connectors.base import ConnectorConfig, ObservationKind
        from ingestion.connectors.gauges import GaugeConnector
 
        return [
            lambda: GaugeConnector(ConnectorConfig(
                name="dhm_river_gauge", kind=ObservationKind.RIVER_LEVEL,
                base_url=os.environ["GAUGE_API_URL"], poll_interval_s=300)),
        ]
    """
    return []

def build_stages() -> list[Stage]:
    """Extra per-record stages run after schema validation.
    A stage takes a record and returns it (or None to drop it).
 
        from ingestion.validators.anomaly_cleaner import clean_anomalies
        from ingestion.validators.spatial_checker import check_inside_nepal
        return [check_inside_nepal, clean_anomalies]
    """
    return []

def observation_to_payload(obs: Observation) -> dict:
    """Convert a connector's Observation into the flat dict that TelemetryData expects.
    ADJUST the keys below to match the fields of your TelemetryData model."""
    payload = dict(obs.payload)
    payload.setdefault("station_id", obs.source)
    payload["sensor_type"] = obs.kind.value
    payload["timestamp"] = obs.observed_at.isoformat()
    if obs.location is not None:
        payload.setdefault("lat", obs.location.lat)
        payload.setdefault("lon", obs.location.lon)
        if obs.location.elevation_m is not None:
            payload.setdefault("elevation_m", obs.location.elevation_m)
    return payload

async def _run_connector(factory: ConnectorFactory, queue: asyncio.Queue, stop: asyncio.Event) -> None:
    """Supervisor: run one connector, restart it with backoff if it crashes."""
    loop = asyncio.get_running_loop()
    backoff = 1.0
    while not stop.is_set():
        conn = factory()
        name = conn.config.name
        started = loop.time()
        try:
            async with conn:
                async for obs in conn.stream():
                    await queue.put((name, observation_to_payload(obs)))
            return  # stream ended normally
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("connector %s crashed; restarting in %.0fs", name, backoff)
        if loop.time() - started > 60:
            backoff = 1.0  # it ran for a while, so treat the next failure as fresh
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)
 
async def _collect(queue: asyncio.Queue, max_items: int, max_wait_s: float) -> list:
    """Gather up to max_items, waiting at most max_wait_s in total."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_s
    items: list = []
    while len(items) < max_items:
        remaining = deadline - loop.time()
        if remaining <= 0:
            break
        try:
            items.append(await asyncio.wait_for(queue.get(), timeout=remaining))
        except asyncio.TimeoutError:
            break
    return items

async def _consume(queue: asyncio.Queue, loader: BatchLoader, cfg: AppConfig, stop: asyncio.Event) -> None:
    """Turn the stream of (source, payload) into micro-batches and push them through the loader.
    Keeps running until stop is set AND the queue is empty, so nothing queued is lost on shutdown."""
    while True:
        batch = await _collect(queue, cfg.batch_size, cfg.flush_interval_s)
        if not batch:
            if stop.is_set() and queue.empty():
                return
            continue
 
        by_source: dict[str, list[dict]] = defaultdict(list)
        for source, payload in batch:
            by_source[source].append(payload)
 
        for source, rows in by_source.items():
            try:
                report = await loader.process_rows(rows, source)
                log.info("[%s] %s", source, report.summary())
            except Exception:
                log.exception("[%s] batch of %d rows failed unexpectedly", source, len(rows))

def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            # Windows: add_signal_handler is unavailable. Fall back for Ctrl+C.
            if sig == signal.SIGINT:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

 
async def run_live(cfg: AppConfig) -> int:
    factories = build_connectors()
    if not factories:
        log.error("No connectors registered. Edit build_connectors() in main.py.")
        return 2
 
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_max)
 
    async with IngestionStreamPublisher(cfg.publisher) as publisher:
        loader = BatchLoader(publisher, stages=build_stages(),
                             chunk_size=cfg.batch_size, failed_log=cfg.failed_log)
 
        producers = [asyncio.create_task(_run_connector(f, queue, stop), name=f"connector-{i}")
                     for i, f in enumerate(factories)]
        consumer = asyncio.create_task(_consume(queue, loader, cfg, stop), name="consumer")
        log.info("live ingestion started with %d connector(s)", len(producers))
 
        await stop.wait()
        log.info("shutdown requested; stopping connectors and draining the queue")
        for t in producers:
            t.cancel()
        await asyncio.gather(*producers, return_exceptions=True)
        await consumer  # drains whatever is still queued
 
        log.info("final publisher stats: %s", publisher.stats)
    return 0
 
async def run_backfill(cfg: AppConfig, paths: list[Path]) -> int:
    total = BatchReport()
    async with IngestionStreamPublisher(cfg.publisher) as publisher:
        loader = BatchLoader(publisher, stages=build_stages(),
                             chunk_size=cfg.batch_size, failed_log=cfg.failed_log)
        for path in paths:
            try:
                report = await loader.load_file(path)
            except (FileNotFoundError, ValueError) as exc:
                log.error("skipping %s: %s", path, exc)
                total.publish_failed += 1
                continue
            log.info("%s done: %s", path.name, report.summary())
            total.merge(report)
 
    log.info("backfill complete: %s", total.summary())
    return 0 if total.clean else 1
 
