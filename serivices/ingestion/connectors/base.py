from __future__ import annotations
import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncIterator, Optional



class ObservationKind(str, Enum):
    """What kind of signal this record carries."""
    RIVER_LEVEL = "river_level"
    RAINFALL = "rainfall"
    SEISMIC = "seismic"
    SATELLITE_SCENE = "satellite_scene"
    CAMERA_FRAME = "camera_frame"
    HEALTH_REPORT = "health_report"
    ROAD_STATUS = "road_status"
    OTHER = "other"


@dataclass(frozen=True)
class GeoPoint:
    lat: float
    lon: float
    elevation_m: Optional[float] = None

    def is_valid(self) -> bool:
        return -90.0 <= self.lat <= 90.0 and -180.0 <= self.lon <= 180.0


@dataclass
class Observation:
    
    source: str                          # connector name, e.g. "dhm_river_gauge"
    kind: ObservationKind
    observed_at: datetime                # when the real-world event happened (UTC)
    payload: dict[str, Any]              # parsed, source-specific values
    location: Optional[GeoPoint] = None
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_ref: Optional[str] = None        # pointer to the stored raw artifact (URL, S3 key, file path)
    quality_flags: list[str] = field(default_factory=list)  # filled in by validators


@dataclass
class ConnectorConfig:
    name: str
    kind: ObservationKind
    base_url: Optional[str] = None
    poll_interval_s: float = 60.0
    timeout_s: float = 20.0
    max_retries: int = 4
    backoff_base_s: float = 1.0          # 1s, 2s, 4s, 8s ... with jitter
    backoff_max_s: float = 60.0
    max_requests_per_minute: int = 30    # simple client-side rate limit
    max_consecutive_failures: int = 10   # stream() raises after this many failed polls in a row
    extra: dict[str, Any] = field(default_factory=dict)  # API keys, station ids, etc.


@dataclass
class ConnectorStats:
    fetches_ok: int = 0
    fetches_failed: int = 0
    records_emitted: int = 0
    records_dropped: int = 0
    last_success_at: Optional[datetime] = None
    last_error: Optional[str] = None
    consecutive_failures: int = 0



class ConnectorError(Exception):
    """Base class for connector failures."""


class TransientFetchError(ConnectorError):
    """Safe to retry (timeout, 5xx, connection reset)."""


class PermanentFetchError(ConnectorError):
    """Do not retry (bad credentials, 404 on a fixed endpoint, schema change)."""


class ParseError(ConnectorError):
    """The payload was fetched but could not be understood."""



class BaseConnector(ABC):
    """
    Subclasses implement only two methods:

        async def fetch(self) -> Any                     # get raw data
        def parse(self, raw: Any) -> list[Observation]       # turn it into records

    Usage:
        async with MyConnector(config) as conn:
            async for obs in conn.stream():
                ...
    """

    def __init__(self, config: ConnectorConfig, logger: Optional[logging.Logger] = None):
        self.config = config
        self.log = logger or logging.getLogger(f"prahari.ingestion.{config.name}")
        self.stats = ConnectorStats()
        self._request_times: list[float] = []
        self._closed = False



    @abstractmethod
    async def fetch(self) -> Any:
        """Retrieve raw data from the source. Raise TransientFetchError or
        PermanentFetchError so the base class knows whether to retry."""

    @abstractmethod
    def parse(self, raw: Any) -> list[Observation]:
        """Convert raw data into Observation records. Raise ParseError on
        malformed input. Must not perform network calls."""



    async def open(self) -> None:
        """Create HTTP sessions, open sockets, authenticate."""

    async def close(self) -> None:
        """Release resources. Called once, even if open() failed."""

    async def health_check(self) -> bool:
        """Cheap liveness probe. Default: succeeded within 3 poll intervals."""
        if self.stats.last_success_at is None:
            return False
        age = (datetime.now(timezone.utc) - self.stats.last_success_at).total_seconds()
        return age < self.config.poll_interval_s * 3

    

    async def __aenter__(self) -> "BaseConnector":
        self._closed = False                 # allows re-entering after a shutdown
        self.stats.consecutive_failures = 0
        await self.open()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.shutdown()

    async def shutdown(self) -> None:
        if not self._closed:
            self._closed = True
            await self.close()

  

    async def poll_once(self) -> list[Observation]:
        """One fetch + parse cycle with retries. Returns records, or [] on failure."""
        try:
            raw = await self._fetch_with_retry()
        except ConnectorError as exc:
            self.stats.fetches_failed += 1
            self.stats.consecutive_failures += 1
            self.stats.last_error = str(exc)
            self.log.error("fetch failed: %s", exc)
            return []

        try:
            records = self.parse(raw)
        except ParseError as exc:
            self.stats.fetches_failed += 1
            self.stats.consecutive_failures += 1
            self.stats.last_error = f"parse: {exc}"
            self.log.error("parse failed: %s", exc)
            return []

        valid = [r for r in records if self._basic_sanity(r)]
        dropped = len(records) - len(valid)
        if dropped:
            self.stats.records_dropped += dropped
            self.log.warning("dropped %d malformed records", dropped)

        self.stats.fetches_ok += 1
        self.stats.consecutive_failures = 0
        self.stats.records_emitted += len(valid)
        self.stats.last_success_at = datetime.now(timezone.utc)
        self.stats.last_error = None
        return valid

    async def stream(self) -> AsyncIterator[Observation]:
        """Poll forever, yielding records. Cancel the task to stop.

        Raises ConnectorError after `max_consecutive_failures` failed polls in a row, so a
        supervisor (see main.py) can rebuild the connector with backoff instead of this loop
        retrying a dead source forever."""
        while not self._closed:
            started = time.monotonic()
            for obs in await self.poll_once():
                yield obs
            if self.stats.consecutive_failures >= self.config.max_consecutive_failures:
                raise ConnectorError(
                    f"{self.config.name}: {self.stats.consecutive_failures} consecutive "
                    f"failures (last: {self.stats.last_error})"
                )
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, self.config.poll_interval_s - elapsed))

  

    async def _fetch_with_retry(self) -> Any:
        attempt = 0
        while True:
            await self._respect_rate_limit()
            try:
                return await asyncio.wait_for(self.fetch(), timeout=self.config.timeout_s)
            except asyncio.TimeoutError:
                err: ConnectorError = TransientFetchError(f"timeout after {self.config.timeout_s}s")
            except TransientFetchError as exc:
                err = exc
            # PermanentFetchError and other ConnectorErrors propagate immediately.

            attempt += 1
            if attempt > self.config.max_retries:
                raise err
            delay = min(self.config.backoff_max_s, self.config.backoff_base_s * 2 ** (attempt - 1))
            delay *= random.uniform(0.5, 1.5)  # jitter avoids synchronized retries
            self.log.warning("transient error (%s); retry %d/%d in %.1fs",
                             err, attempt, self.config.max_retries, delay)
            await asyncio.sleep(delay)

    async def _respect_rate_limit(self) -> None:
        """Sliding-window limiter: at most N requests in any 60s window."""
        limit = self.config.max_requests_per_minute
        now = time.monotonic()
        self._request_times = [t for t in self._request_times if now - t < 60.0]
        if len(self._request_times) >= limit:
            wait = 60.0 - (now - self._request_times[0])
            self.log.debug("rate limit reached; sleeping %.1fs", wait)
            await asyncio.sleep(max(0.0, wait))
        self._request_times.append(time.monotonic())

    def _basic_sanity(self, obs: Observation) -> bool:
        """Structural checks only. Domain checks (ranges, spikes) belong in validators/."""
        if obs.observed_at.tzinfo is None:
            self.log.debug("naive timestamp from %s", obs.source)
            return False
        if obs.observed_at > datetime.now(timezone.utc):
            return False
        if obs.location is not None and not obs.location.is_valid():
            return False
        return bool(obs.payload)