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
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),

