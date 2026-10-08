"""
services/ingestion/validators/anomaly_cleaner.py

Per-record cleaning stage for BatchLoader. Runs AFTER schema validation.

    Stage = Callable[[TelemetryData], Optional[TelemetryData]]
            return the record (possibly flagged)  -> keep
            return None                           -> drop

Policy (important for an early-warning system):

  DROP  values that are physically impossible or are "no data" markers:
        NaN/inf, sentinel codes (-9999 ...), outside the physical range, future timestamps.

  FLAG  values that are only statistically unusual:
        sudden spikes and stuck sensors. A real flood looks like a spike, so these records
        are passed on with a quality flag and the forecasting layer decides what to do.
        Set drop_suspect=True if you prefer hard removal.

State: this stage keeps a short rolling history per (sensor_type, station_id), so use ONE
instance for the whole stream and feed backfill files in time order.
"""

from __future__ import annotations

import logging
import math
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Optional

from prahari_schemas import TelemetryData  # packages/schemas

log = logging.getLogger("prahari.ingestion.anomaly_cleaner")

# Field names on TelemetryData. EDIT if your schema uses different names.
F_SENSOR = "sensor_type"
F_STATION = "station_id"
F_VALUE = "value"
F_TIME = "timestamp"
F_FLAGS = "quality_flags"   # list[str] field; add it to TelemetryData so flags are kept

# Starting values. Calibrate per station and confirm the units your connectors emit.
DEFAULT_RANGES: dict[str, tuple[float, float]] = {
    "river_level": (0.0, 50.0),    # metres
    "rainfall": (0.0, 400.0),      # mm per hour
    "seismic": (-2.0, 10.0),       # magnitude
}

# Common "missing data" codes. -1 is deliberately absent: it is valid for some sensors.
SENTINELS: frozenset[float] = frozenset({-99999.0, -9999.0, -999.0, 999.0, 9999.0, 99999.0})


@dataclass
class CleanerConfig:
    ranges: dict[str, tuple[float, float]] = field(default_factory=lambda: dict(DEFAULT_RANGES))
    sentinels: frozenset[float] = SENTINELS

    # Spike detection uses a modified z-score (median/MAD), which a single outlier cannot distort.
    # Rainfall is excluded on purpose: it is zero most of the time, so every storm would look
    # like a spike.
    spike_types: frozenset[str] = frozenset({"river_level"})
    window: int = 48                  # readings kept per series
    min_history: int = 10             # no spike judgement before this many readings
    spike_threshold: float = 6.0

    # Stuck sensor: the same value N times in a row. Only meaningful for continuously
    # varying signals (rainfall can legitimately be 0.0 for days).
    stuck_types: frozenset[str] = frozenset({"river_level"})
    stuck_after: int = 12

    drop_suspect: bool = False        # True: drop flagged records instead of passing them on
    max_future_skew: timedelta = timedelta(minutes=5)


class AnomalyCleaner:
    def __init__(self, config: Optional[CleanerConfig] = None):
        self.cfg = config or CleanerConfig()
        self.stats: Counter[str] = Counter()
        self._history: dict[tuple[str, str], Deque[float]] = defaultdict(
            lambda: deque(maxlen=self.cfg.window)
        )
        self._run: dict[tuple[str, str], tuple[float, int]] = {}   # last value, run length
        self._warned_no_flag_field = False

    def __repr__(self) -> str:
        return "AnomalyCleaner()"

    # ---- the stage ------------------------------------------------------- #

    def __call__(self, rec: TelemetryData) -> Optional[TelemetryData]:
        cfg = self.cfg
        self.stats["seen"] += 1

        sensor = str(getattr(rec, F_SENSOR, ""))
        station = str(getattr(rec, F_STATION, ""))
        value = getattr(rec, F_VALUE, None)

        # 1. hard checks: anything failing these never reaches the rolling history
        if self._is_future(getattr(rec, F_TIME, None)):
            return self._drop("future_timestamp", sensor, station)

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            self.stats["passed"] += 1
            return rec                      # non-numeric payloads are not this stage's job
        v = float(value)

        if not math.isfinite(v):
            return self._drop("non_finite", sensor, station)
        if v in cfg.sentinels:
            return self._drop("sentinel", sensor, station)
        limits = cfg.ranges.get(sensor)
        if limits and not (limits[0] <= v <= limits[1]):
            return self._drop("out_of_range", sensor, station)

        # 2. soft checks: flag, do not drop
        key = (sensor, station)
        flags: list[str] = []
        if sensor in cfg.spike_types and self._is_spike(key, v):
            flags.append("suspect_spike")
        if sensor in cfg.stuck_types and self._is_stuck(key, v):
            flags.append("stuck_sensor")

        # Flagged values still enter the history, so a genuine regime change
        # (e.g. a sustained flood rise) becomes the new baseline instead of being flagged forever.
        self._history[key].append(v)

        if flags:
            if cfg.drop_suspect:
                return self._drop("+".join(flags), sensor, station)
            for f in flags:
                self.stats[f"flag:{f}"] += 1
                rec = self._with_flag(rec, f)

        self.stats["passed"] += 1
        return rec

    # ---- checks ---------------------------------------------------------- #

    def _is_spike(self, key: tuple[str, str], v: float) -> bool:
        hist = self._history[key]
        if len(hist) < self.cfg.min_history:
            return False
        med = statistics.median(hist)
        mad = statistics.median(abs(x - med) for x in hist)
        # Floor on the scale so a perfectly flat history does not make every change infinite.
        scale = max(mad, 0.02 * abs(med), 1e-3)
        return 0.6745 * abs(v - med) / scale > self.cfg.spike_threshold

    def _is_stuck(self, key: tuple[str, str], v: float) -> bool:
        last, n = self._run.get(key, (None, 0))
        n = n + 1 if last == v else 1
        self._run[key] = (v, n)
        return n >= self.cfg.stuck_after

    def _is_future(self, ts: Any) -> bool:
        dt = _as_utc(ts)
        return dt is not None and dt > datetime.now(timezone.utc) + self.cfg.max_future_skew

    # ---- helpers --------------------------------------------------------- #

    def _drop(self, reason: str, sensor: str, station: str) -> None:
        self.stats[f"dropped:{reason}"] += 1
        log.debug("dropped %s/%s: %s", sensor, station, reason)
        return None

    def _with_flag(self, rec: TelemetryData, flag: str) -> TelemetryData:
        current = getattr(rec, F_FLAGS, None)
        if current is None:
            if not self._warned_no_flag_field:
                log.warning("TelemetryData has no %r field, so quality flags are only counted, "
                            "not stored. Add `%s: list[str] = []` to the schema.", F_FLAGS, F_FLAGS)
                self._warned_no_flag_field = True
            return rec
        return rec.model_copy(update={F_FLAGS: [*current, flag]})

    def summary(self) -> str:
        return ", ".join(f"{k}={v}" for k, v in sorted(self.stats.items())) or "no records seen"


def _as_utc(ts: Any) -> Optional[datetime]:
    """Best-effort timestamp parsing. Returns None if it cannot be interpreted
    (schema validation is responsible for rejecting malformed timestamps)."""
    try:
        if isinstance(ts, datetime):
            dt = ts
        elif isinstance(ts, (int, float)) and not isinstance(ts, bool):
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        elif isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            return None
    except (ValueError, OverflowError, OSError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# Ready-to-use instance, matching the example in main.py's build_stages().
clean_anomalies = AnomalyCleaner()