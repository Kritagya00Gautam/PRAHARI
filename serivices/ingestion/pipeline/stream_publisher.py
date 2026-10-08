from __future__ import annotations
import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence
import nats
from nats.errors import ConnectionClosedError, NoServersError, TimeoutError as NatsTimeout
from nats.js.api import RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import APIError, NotFoundError
from prahari_schemas import TelemetryData  # packages/schemas

log = logging.getLogger("prahari.ingestion.stream_publisher")

_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")



@dataclass
class PublisherConfig:
    urls: list[str]
    client_name: str = "prahari-ingestion"
    stream_name: str = "TELEMETRY"
    subject_prefix: str = "telemetry"
    dlq_prefix: str = "dlq.telemetry"
    stream_max_age_s: float = 7 * 24 * 3600     # keep 7 days of history
    dedupe_window_s: float = 120.0              # ignore duplicate message IDs for 2 min
    connect_timeout_s: float = 5.0
    publish_timeout_s: float = 2.0
    max_retries: int = 3
    retry_base_s: float = 0.2
    max_concurrency: int = 50                   # in-flight publishes during a batch


class PublishError(Exception):
    """Base class for publisher failures."""


class InvalidSubjectError(PublishError):
    """sensor_type or station_id cannot be used safely as a subject token."""


class NotConnectedError(PublishError):
    """publish was called before connect()."""


@dataclass
class PublishStats:
    published: int = 0
    duplicates: int = 0
    failed: int = 0
    retries: int = 0
    reconnects: int = 0


@dataclass
class BatchResult:
    published: int = 0
    duplicates: int = 0
    failed: list[tuple[TelemetryData, str]] = field(default_factory=list)  # (record, reason)




class IngestionStreamPublisher:
    def __init__(
        self,
        config: PublisherConfig,
        msg_id_fn: Optional[Callable[[TelemetryData, bytes], str]] = None,
    ):
        self.config = config
        self.stats = PublishStats()
        self._nc: Optional[nats.NATS] = None
        self._js = None
        # Default ID = hash of the payload, so an identical re-send is de-duplicated.
        # Override with a domain key (e.g. station_id + observed_at) if you prefer.
        self._msg_id_fn = msg_id_fn or (lambda _t, payload: hashlib.sha256(payload).hexdigest())

   

    async def __aenter__(self) -> "IngestionStreamPublisher":
        await self.connect()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def connect(self) -> None:
        cfg = self.config
        try:
            self._nc = await nats.connect(
                servers=cfg.urls,
                name=cfg.client_name,
                connect_timeout=cfg.connect_timeout_s,
                max_reconnect_attempts=-1,          # keep trying forever
                reconnect_time_wait=2,
                error_cb=self._on_error,
                disconnected_cb=self._on_disconnected,
                reconnected_cb=self._on_reconnected,
                closed_cb=self._on_closed,
            )
        except (NoServersError, OSError, asyncio.TimeoutError) as exc:
            raise PublishError(f"could not connect to NATS at {cfg.urls}: {exc}") from exc

        self._js = self._nc.jetstream()
        await self._ensure_stream()
        log.info("connected to %s, stream=%s", cfg.urls, cfg.stream_name)

    async def close(self) -> None:
        """Flush pending messages, then close cleanly."""
        if self._nc is not None and not self._nc.is_closed:
            await self._nc.drain()
        self._nc = None
        self._js = None

    async def health_check(self) -> bool:
        return self._nc is not None and self._nc.is_connected



    async def publish_telemetry(self, telemetry: TelemetryData) -> bool:
        """
        Publish one record. Returns True if newly stored, False if it was a
        duplicate. Raises PublishError if it could not be stored after retries.
        """
        self._require_connection()
        subject = self._build_subject(telemetry)
        payload = telemetry.model_dump_json().encode("utf-8")
        headers = {
            "Nats-Msg-Id": self._msg_id_fn(telemetry, payload),   # JetStream de-duplication
            "Content-Type": "application/json",
            "Schema": type(telemetry).__name__,
            "Published-At": datetime.now(timezone.utc).isoformat(),
        }

        for attempt in range(self.config.max_retries + 1):
            try:
                ack = await self._js.publish(
                    subject, payload, headers=headers, timeout=self.config.publish_timeout_s
                )
            except (NatsTimeout, APIError, ConnectionClosedError) as exc:
                if attempt == self.config.max_retries:
                    self.stats.failed += 1
                    raise PublishError(f"publish to {subject} failed: {exc}") from exc
                self.stats.retries += 1
                delay = self.config.retry_base_s * 2 ** attempt
                log.warning("publish retry %d/%d for %s in %.1fs (%s)",
                            attempt + 1, self.config.max_retries, subject, delay, exc)
                await asyncio.sleep(delay)
                continue

            if ack.duplicate:
                self.stats.duplicates += 1
                return False
            self.stats.published += 1
            return True

        raise PublishError("unreachable")  # loop always returns or raises

    async def publish_batch(self, records: Sequence[TelemetryData]) -> BatchResult:
        """Publish many records concurrently (bounded). Never raises per-record."""
        sem = asyncio.Semaphore(self.config.max_concurrency)
        result = BatchResult()

        async def one(rec: TelemetryData) -> None:
            async with sem:
                try:
                    if await self.publish_telemetry(rec):
                        result.published += 1
                    else:
                        result.duplicates += 1
                except PublishError as exc:
                    result.failed.append((rec, str(exc)))

        await asyncio.gather(*(one(r) for r in records))
        if result.failed:
            log.error("batch finished: %d published, %d failed", result.published, len(result.failed))
        return result

    async def publish_dead_letter(self, source: str, raw: bytes, reason: str) -> None:
        """Send a rejected payload to the dead-letter subject for later inspection."""
        self._require_connection()
        token = source if _TOKEN_RE.match(source) else "unknown"
        await self._js.publish(
            f"{self.config.dlq_prefix}.{token}", raw,
            headers={"Reason": reason[:500]}, timeout=self.config.publish_timeout_s,
        )

   

    def _build_subject(self, t: TelemetryData) -> str:
        sensor, station = str(t.sensor_type), str(t.station_id)
        for name, tok in (("sensor_type", sensor), ("station_id", station)):
            if not _TOKEN_RE.match(tok):
                raise InvalidSubjectError(f"{name}={tok!r} is not a valid subject token")
        return f"{self.config.subject_prefix}.{sensor}.{station}"

    def _require_connection(self) -> None:
        if self._js is None or self._nc is None:
            raise NotConnectedError("call connect() or use 'async with' first")

    async def _ensure_stream(self) -> None:
        cfg = self.config
        stream_cfg = StreamConfig(
            name=cfg.stream_name,
            subjects=[f"{cfg.subject_prefix}.>", f"{cfg.dlq_prefix}.>"],
            retention=RetentionPolicy.LIMITS,
            storage=StorageType.FILE,
            max_age=cfg.stream_max_age_s,
            duplicate_window=cfg.dedupe_window_s,
        )
        try:
            await self._js.stream_info(cfg.stream_name)
            await self._js.update_stream(stream_cfg)
        except NotFoundError:
            await self._js.add_stream(stream_cfg)

  

    async def _on_error(self, exc: Exception) -> None:
        log.error("NATS error: %s", exc)

    async def _on_disconnected(self) -> None:
        log.warning("NATS disconnected; client will auto-reconnect")

    async def _on_reconnected(self) -> None:
        self.stats.reconnects += 1
        log.info("NATS reconnected")

    async def _on_closed(self) -> None:
        log.info("NATS connection closed")