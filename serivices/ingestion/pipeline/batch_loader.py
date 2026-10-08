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

