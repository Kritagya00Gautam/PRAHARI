from __future__ import annotations
import asyncio
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from ingestion.connectors._common import (
    http_get,
    normalize_id,
    to_float as _to_float,
    validate_http_url,
)
from ingestion.connectors.base import (
    BaseConnector,
    ConnectorConfig,
    GeoPoint,
    Observation,
    ObservationKind,
    ParseError,
)

log = logging.getLogger("prahari.ingestion.weather_api")


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

@dataclass
class WeatherLocation:
    location_id: str                        # becomes the station_id; letters, digits, _ and -
    lat: float
    lon: float
    name: str = ""
    elevation_m: Optional[float] = None


@dataclass
class WeatherSettings:
    locations: list[WeatherLocation]
    base_url: str = "https://api.open-meteo.com/v1/forecast"
    variables: tuple[str, ...] = ("precipitation", "temperature_2m", "relative_humidity_2m",
                                  "wind_speed_10m", "surface_pressure")
    forecast_variables: tuple[str, ...] = ("precipitation",)
    past_hours: int = 6                     # how much recent history each poll asks for
    forecast_hours: int = 48                # 0 switches forecasts off
    chunk_size: int = 50                    # locations per request
    forecast_change_tol: float = 0.2        # re-emit a forecast only if it moved by at least this (its own unit)
    emit_history_on_first_poll: bool = True

    api_key: Optional[str] = None           # commercial plans only; keep it in an environment variable
    headers: dict[str, str] = field(default_factory=dict)
    http_timeout_s: float = 20.0

    def __post_init__(self) -> None:
        validate_http_url(self.base_url)
        if not self.locations:
            raise ValueError("at least one location is required")
        if not self.variables:
            raise ValueError("at least one variable is required")
        ids = set()
        for loc in self.locations:
            if normalize_id(loc.location_id) != loc.location_id or not loc.location_id:
                raise ValueError(f"location_id {loc.location_id!r} must match [A-Za-z0-9_-]{{1,64}}")
            if loc.location_id in ids:
                raise ValueError(f"duplicate location_id {loc.location_id!r}")
            ids.add(loc.location_id)
            if not (-90 <= loc.lat <= 90 and -180 <= loc.lon <= 180):
                raise ValueError(f"location {loc.location_id!r} has invalid coordinates")
        if not 1 <= self.chunk_size <= 200:
            raise ValueError("chunk_size must be between 1 and 200")
        if self.past_hours < 0 or self.forecast_hours < 0:
            raise ValueError("past_hours and forecast_hours cannot be negative")
        unknown = set(self.forecast_variables) - set(self.variables)
        if unknown:
            raise ValueError(f"forecast_variables {sorted(unknown)} are not in variables")


# --------------------------------------------------------------------------- #
# Connector
# --------------------------------------------------------------------------- #

class WeatherApiConnector(BaseConnector):
    """
    Example:
        settings = WeatherSettings(locations=[
            WeatherLocation("pokhara", 28.2096, 83.9856, "Pokhara"),
            WeatherLocation("jomsom", 28.7806, 83.7231, "Jomsom"),
        ])
        connector = WeatherApiConnector(
            ConnectorConfig(name="weather", kind=ObservationKind.WEATHER,
                            poll_interval_s=3600, timeout_s=60),
            settings)
    Set ConnectorConfig.timeout_s high enough for all chunks (one request each, run in sequence).
    """

    def __init__(
        self,
        config: ConnectorConfig,
        settings: WeatherSettings,
        fetcher: Optional[Callable[[dict[str, str]], bytes]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(config, logger)
        self.settings = settings
        self._fetcher = fetcher or self._http_get
        n = settings.chunk_size
        self._chunks = [settings.locations[i:i + n] for i in range(0, len(settings.locations), n)]
        self._seen_past: set[tuple[str, str, int]] = set()
        self._last_forecast: dict[tuple[str, str, int], float] = {}
        self._first_poll = True
        self._warned_missing: set[str] = set()
        self.feed_stats: Counter[str] = Counter()

    # ---- fetch ----------------------------------------------------------- #

    def _params(self, chunk: list[WeatherLocation]) -> dict[str, str]:
        s = self.settings
        p = {
            "latitude": ",".join(f"{loc.lat:.5f}" for loc in chunk),
            "longitude": ",".join(f"{loc.lon:.5f}" for loc in chunk),
            "hourly": ",".join(s.variables),
            "timeformat": "unixtime",       # epoch seconds in UTC: no time-zone ambiguity
            "wind_speed_unit": "ms",
            "past_hours": str(s.past_hours),
            # forecast_hours must be > 0; with forecasts off we ask for 1 hour and ignore it.
            "forecast_hours": str(max(1, s.forecast_hours)),
        }
        if any(loc.elevation_m is not None for loc in chunk):
            p["elevation"] = ",".join("nan" if loc.elevation_m is None else f"{loc.elevation_m:.0f}"
                                      for loc in chunk)
        if s.api_key:
            p["apikey"] = s.api_key
        return p

    async def fetch(self) -> list[bytes]:
        def run() -> list[bytes]:
            return [self._fetcher(self._params(chunk)) for chunk in self._chunks]
        return await asyncio.to_thread(run)

    def _http_get(self, params: dict[str, str]) -> bytes:
        s = self.settings
        return http_get(s.base_url, headers=s.headers, params=params, timeout_s=s.http_timeout_s)

    # ---- parse ----------------------------------------------------------- #

    def parse(self, raw: list[bytes]) -> list[Observation]:
        if len(raw) != len(self._chunks):
            raise ParseError(f"expected {len(self._chunks)} responses, got {len(raw)}")
        now = datetime.now(timezone.utc).replace(microsecond=0)

        out: list[Observation] = []
        for chunk, body in zip(self._chunks, raw):
            try:
                doc = json.loads(body)
            except (ValueError, UnicodeDecodeError) as exc:
                raise ParseError(f"response is not valid JSON: {exc}") from exc
            if isinstance(doc, dict) and doc.get("error"):
                raise ParseError(f"API error: {doc.get('reason', 'unknown')}")
            entries = doc if isinstance(doc, list) else [doc]
            if len(entries) != len(chunk):
                raise ParseError(f"asked for {len(chunk)} locations, got {len(entries)} in the response")
            for loc, entry in zip(chunk, entries):
                out.extend(self._location_records(loc, entry, now))

        self._first_poll = False
        self._prune(now)
        self.feed_stats["emitted"] += len(out)
        return out

    def _location_records(self, loc: WeatherLocation, entry: Any, now: datetime) -> list[Observation]:
        s = self.settings
        try:
            hourly = entry["hourly"]
            times = hourly["time"]
        except (KeyError, TypeError) as exc:
            raise ParseError(f"location {loc.location_id}: no hourly data in the response "
                             f"(missing {exc}); has the API changed?") from exc
        units = entry.get("hourly_units") or {}
        point = GeoPoint(loc.lat, loc.lon, loc.elevation_m)
        records: list[Observation] = []

        for var in s.variables:
            values = hourly.get(var)
            if values is None:
                self.feed_stats["variable_missing"] += 1
                if var not in self._warned_missing:
                    self._warned_missing.add(var)
                    self.log.warning("variable %r is not in the API response; check the name", var)
                continue
            if not isinstance(values, list) or len(values) != len(times):
                raise ParseError(f"location {loc.location_id}: {var} has {len(values) if isinstance(values, list) else '?'} "
                                 f"values for {len(times)} timestamps")
            unit = units.get(var, "")

            for t, v in zip(times, values):
                if isinstance(t, bool) or not isinstance(t, (int, float)):
                    raise ParseError(f"unexpected timestamp {t!r}; the connector requests timeformat=unixtime")
                try:
                    value = _to_float(v)
                except ValueError:
                    self.feed_stats["values_malformed"] += 1
                    continue
                if value is None:
                    self.feed_stats["values_null"] += 1
                    continue
                at = datetime.fromtimestamp(t, tz=timezone.utc)

                if at <= now:
                    obs = self._past_record(loc, point, var, value, unit, at)
                elif s.forecast_hours > 0 and var in s.forecast_variables:
                    obs = self._forecast_record(loc, point, var, value, unit, at, now)
                else:
                    obs = None
                if obs is not None:
                    records.append(obs)
        return records

    def _past_record(self, loc, point, var, value, unit, at) -> Optional[Observation]:
        key = (loc.location_id, var, int(at.timestamp()))
        if key in self._seen_past:
            self.feed_stats["not_new"] += 1
            return None
        self._seen_past.add(key)
        if self._first_poll and not self.settings.emit_history_on_first_poll:
            self.feed_stats["history_skipped"] += 1
            return None
        kind = ObservationKind.RAINFALL if var == "precipitation" else ObservationKind.WEATHER
        return Observation(
            source=self.config.name, kind=kind, observed_at=at, location=point,
            payload=self._payload(loc, var, value, unit, origin="model_analysis"),
        )

    def _forecast_record(self, loc, point, var, value, unit, valid_at, now) -> Optional[Observation]:
        key = (loc.location_id, var, int(valid_at.timestamp()))
        previous = self._last_forecast.get(key)
        if previous is not None and abs(previous - value) < self.settings.forecast_change_tol:
            self.feed_stats["forecast_unchanged"] += 1
            return None
        self._last_forecast[key] = value
        payload = self._payload(loc, var, value, unit, origin="forecast_model")
        payload.update({
            "valid_at": valid_at.isoformat(),
            "lead_hours": round((valid_at - now).total_seconds() / 3600),
            "issued_at": now.isoformat(),
        })
        if previous is not None:
            payload["previous_value"] = round(previous, 3)
        return Observation(source=self.config.name, kind=ObservationKind.FORECAST,
                           observed_at=now, location=point, payload=payload)

    @staticmethod
    def _payload(loc: WeatherLocation, var: str, value: float, unit: str, origin: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "station_id": loc.location_id,
            "variable": var,
            "value": round(value, 3),
            "unit": unit,
            "data_source": "open-meteo",
            "origin": origin,
            "quality_flags": [],
        }
        if loc.name:
            payload["location_name"] = loc.name
        return payload

    def _prune(self, now: datetime) -> None:
        """Keep the 'already seen' tables bounded."""
        horizon = now - timedelta(hours=self.settings.past_hours + 24)
        self._seen_past = {k for k in self._seen_past if k[2] >= horizon.timestamp()}
        cutoff = (now - timedelta(hours=1)).timestamp()
        self._last_forecast = {k: v for k, v in self._last_forecast.items() if k[2] >= cutoff}