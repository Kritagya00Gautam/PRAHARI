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
