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
