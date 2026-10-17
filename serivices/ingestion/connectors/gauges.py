from __future__ import annotations
import asyncio
import csv
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo
from ingestion.connectors.base import (
    BaseConnector,
    ConnectorConfig,
    GeoPoint,
    Observation,
    ObservationKind,
    ParseError,
    PermanentFetchError,
)
from ingestion.connectors._common import (
    dig as _dig,
    http_get,
    normalize_id as normalize_station_id,
    to_float as _to_float,
    validate_http_url,
)

log = logging.getLogger("prahari.ingestion.gauges")

_UNIT_TO_M = {"m": 1.0, "cm": 0.01, "mm": 0.001, "ft": 0.3048}


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

@dataclass
class GaugeStation:
    station_id: str                        # as spelled in the feed
    name: str = ""
    lat: Optional[float] = None
    lon: Optional[float] = None
    elevation_m: Optional[float] = None
    warning_level_m: Optional[float] = None
    danger_level_m: Optional[float] = None


@dataclass
class GaugeFieldMap:
    """Where things live in the JSON. Paths are dotted: "data.readings"."""
    records_path: str = ""                 # "" = the top-level value is the list of readings
    station_id: str = "station_id"
    value: str = "water_level"
    timestamp: str = "timestamp"
    station_id_from_key: bool = False      # records are {"<station id>": {...}, ...}


@dataclass
class GaugeSettings:
    url: str
    field_map: GaugeFieldMap = field(default_factory=GaugeFieldMap)
    value_unit: str = "m"                  # unit of the values in the feed: m, cm, mm, ft
    stations: dict[str, GaugeStation] = field(default_factory=dict)   # empty = accept any station

    headers: dict[str, str] = field(default_factory=dict)   # e.g. {"Authorization": "Bearer ..."}
    params: dict[str, str] = field(default_factory=dict)    # query string parameters
    http_timeout_s: float = 15.0

    timestamp_tz: str = "Asia/Kathmandu"   # zone for timestamps that carry no offset (UTC+5:45)
    timestamp_formats: tuple[str, ...] = ()  # strptime formats tried before ISO-8601
    max_age_s: Optional[float] = 6 * 3600  # older readings get the "stale_reading" flag
    emit_history_on_first_poll: bool = False  # False: first poll emits only the latest per station

    def __post_init__(self) -> None:
        validate_http_url(self.url)
        if self.value_unit not in _UNIT_TO_M:
            raise ValueError(f"value_unit must be one of {sorted(_UNIT_TO_M)}")
        ZoneInfo(self.timestamp_tz)        # fails early on a bad zone name


def load_gauge_stations(path: str | Path) -> dict[str, GaugeStation]:
    """CSV columns: station_id,name,lat,lon,elevation_m,warning_level_m,danger_level_m
    (only station_id is required; blank cells are fine)."""
    def num(x: Optional[str]) -> Optional[float]:
        return float(x) if x not in (None, "") else None

    out: dict[str, GaugeStation] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sid = row["station_id"].strip()
            out[sid] = GaugeStation(
                station_id=sid, name=(row.get("name") or "").strip(),
                lat=num(row.get("lat")), lon=num(row.get("lon")),
                elevation_m=num(row.get("elevation_m")),
                warning_level_m=num(row.get("warning_level_m")),
                danger_level_m=num(row.get("danger_level_m")),
            )
    return out


# --------------------------------------------------------------------------- #
# Small parsing helpers
# --------------------------------------------------------------------------- #

def parse_timestamp(value: Any, tz: ZoneInfo, formats: tuple[str, ...] = ()) -> datetime:
    """Epoch seconds/milliseconds, a configured strptime format, or ISO-8601 -> aware UTC datetime.
    Timestamps without an offset are interpreted in `tz`."""
    if isinstance(value, bool):
        raise ValueError("boolean is not a timestamp")
    if isinstance(value, (int, float)) or (isinstance(value, str) and re.fullmatch(r"\d{9,13}(\.\d+)?", value.strip())):
        n = float(value)
        return datetime.fromtimestamp(n / 1000.0 if n > 1e11 else n, tz=timezone.utc)
    if not isinstance(value, str):
        raise ValueError(f"unsupported timestamp: {value!r}")

    text = value.strip()
    dt: Optional[datetime] = None
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if dt is None:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))   # ValueError if it is not ISO
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# Connector
# --------------------------------------------------------------------------- #

class GaugeConnector(BaseConnector):
    """
    Example:
        settings = GaugeSettings(
            url=os.environ["GAUGE_API_URL"],
            headers={"Authorization": f"Bearer {os.environ['GAUGE_API_TOKEN']}"},
            field_map=GaugeFieldMap(records_path="data.readings", station_id="station",
                                    value="level", timestamp="time"),
            value_unit="cm",
            timestamp_formats=("%Y-%m-%d %H:%M",),
            stations=load_gauge_stations("data/stations.csv"),
        )
        connector = GaugeConnector(
            ConnectorConfig(name="river_gauges", kind=ObservationKind.RIVER_LEVEL,
                            poll_interval_s=600, timeout_s=30),
            settings)
    """

    def __init__(
        self,
        config: ConnectorConfig,
        settings: GaugeSettings,
        fetcher: Optional[Callable[[], bytes]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(config, logger)
        self.settings = settings
        self._fetcher = fetcher or self._http_get
        self._tz = ZoneInfo(settings.timestamp_tz)
        self._last_seen: dict[str, datetime] = {}
        self._first_poll = True
        self._warned_unknown: set[str] = set()
        self.feed_stats: Counter[str] = Counter()

    # ---- fetch ----------------------------------------------------------- #

    async def fetch(self) -> bytes:
        return await asyncio.to_thread(self._fetcher)

    def _http_get(self) -> bytes:
        s = self.settings
        return http_get(s.url, headers=s.headers, params=s.params, timeout_s=s.http_timeout_s)

    # ---- parse ----------------------------------------------------------- #

    def parse(self, raw: bytes) -> list[Observation]:
        s, fm = self.settings, self.settings.field_map

        try:
            doc = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ParseError(f"response is not valid JSON: {exc}") from exc

        try:
            rows = _dig(doc, fm.records_path)
        except KeyError as exc:
            raise ParseError(f"records_path {fm.records_path!r} not found (missing {exc}); "
                             "has the feed format changed?") from exc
        if isinstance(rows, dict):
            if fm.station_id_from_key:
                rows = [{**v, fm.station_id: k} for k, v in rows.items() if isinstance(v, dict)]
            else:
                rows = [rows]
        if not isinstance(rows, list):
            raise ParseError(f"expected a list of readings, got {type(rows).__name__}")

        candidates: list[Observation] = []
        malformed_before = self.feed_stats["rows_malformed"]
        for row in rows:
            self.feed_stats["rows"] += 1
            obs = self._row_to_observation(row)
            if obs is not None:
                candidates.append(obs)

        # Rows that are well-formed but carry no usable value ("NA", unknown station) are normal.
        # Every row being malformed is not: the feed layout has probably changed.
        if rows and self.feed_stats["rows_malformed"] - malformed_before == len(rows):
            raise ParseError(f"none of {len(rows)} rows could be read; has the feed format changed?")

        return self._only_new(candidates)

    def _row_to_observation(self, row: Any) -> Optional[Observation]:
        s, fm = self.settings, self.settings.field_map
        if not isinstance(row, dict):
            self.feed_stats["rows_malformed"] += 1
            return None
        try:
            raw_id = str(_dig(row, fm.station_id)).strip()
            raw_value = _dig(row, fm.value)
            observed_at = parse_timestamp(_dig(row, fm.timestamp), self._tz, s.timestamp_formats)
            value = _to_float(raw_value)
        except (KeyError, ValueError, TypeError, OverflowError, OSError):
            self.feed_stats["rows_malformed"] += 1
            return None

        station_id = normalize_station_id(raw_id)
        if not station_id:
            self.feed_stats["rows_malformed"] += 1
            return None
        if value is None:
            self.feed_stats["no_reading"] += 1        # the gauge reported "no data": normal, skip
            return None

        station = s.stations.get(raw_id)
        if s.stations and station is None:
            self.feed_stats["unknown_station"] += 1
            if raw_id not in self._warned_unknown:
                self._warned_unknown.add(raw_id)
                self.log.warning("reading from station %r which is not in the station registry; "
                                 "ignoring it (add it to your stations file)", raw_id)
            return None

        metres = value * _UNIT_TO_M[s.value_unit]
        flags: list[str] = []
        if s.max_age_s is not None and (datetime.now(timezone.utc) - observed_at).total_seconds() > s.max_age_s:
            flags.append("stale_reading")

        payload: dict[str, Any] = {
            "station_id": station_id,
            "source_station_id": raw_id,
            "value": round(metres, 4),
            "unit": "m",
            "raw_value": raw_value,
            "raw_unit": s.value_unit,
            "quality_flags": list(flags),
        }
        location = None
        if station is not None:
            if station.name:
                payload["station_name"] = station.name
            if station.lat is not None and station.lon is not None:
                location = GeoPoint(station.lat, station.lon, station.elevation_m)
            if station.warning_level_m is not None:
                payload["warning_level_m"] = station.warning_level_m
            if station.danger_level_m is not None:
                payload["danger_level_m"] = station.danger_level_m
            payload["level_status"] = self._status(metres, station)

        return Observation(
            source=self.config.name, kind=ObservationKind.RIVER_LEVEL, observed_at=observed_at,
            payload=payload, location=location, quality_flags=list(flags),
        )

    @staticmethod
    def _status(metres: float, st: GaugeStation) -> str:
        if st.danger_level_m is not None and metres >= st.danger_level_m:
            return "danger"
        if st.warning_level_m is not None and metres >= st.warning_level_m:
            return "warning"
        return "normal"

    def _only_new(self, candidates: list[Observation]) -> list[Observation]:
        """Gauges update every 15-60 minutes but we poll more often: emit each reading once."""
        candidates.sort(key=lambda o: o.observed_at)
        newest: dict[str, datetime] = {}
        for o in candidates:
            sid = o.payload["station_id"]
            newest[sid] = max(newest.get(sid, o.observed_at), o.observed_at)

        out: list[Observation] = []
        if self._first_poll and not self.settings.emit_history_on_first_poll:
            latest_per_station: dict[str, Observation] = {}
            for o in candidates:                      # sorted, so the last one wins
                latest_per_station[o.payload["station_id"]] = o
            out = list(latest_per_station.values())
            self.feed_stats["history_skipped"] += len(candidates) - len(out)
        else:
            for o in candidates:
                last = self._last_seen.get(o.payload["station_id"])
                if last is not None and o.observed_at <= last:
                    self.feed_stats["not_new"] += 1
                else:
                    out.append(o)

        for sid, ts in newest.items():
            self._last_seen[sid] = max(ts, self._last_seen.get(sid, ts))
        self._first_poll = False
        self.feed_stats["emitted"] += len(out)
        return out