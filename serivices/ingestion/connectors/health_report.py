from __future__ import annotations
import asyncio
import csv
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo
from ingestion.connectors._common import (
    dig as _dig,
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

log = logging.getLogger("prahari.ingestion.health_report")

_WEEK_RE = re.compile(r"^(\d{4})-?W(\d{1,2})$", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Periods
# --------------------------------------------------------------------------- #

def parse_period(text: Any) -> tuple[date, date]:
    """Reporting period -> (first day, last day), both inclusive. ValueError if not understood."""
    s = str(text).strip()
    if m := _WEEK_RE.match(s):
        start = date.fromisocalendar(int(m.group(1)), int(m.group(2)), 1)   # ISO weeks start on Monday
        return start, start + timedelta(days=6)
    if re.fullmatch(r"\d{8}", s):
        d = datetime.strptime(s, "%Y%m%d").date()
        return d, d
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        d = date.fromisoformat(s)
        return d, d
    if re.fullmatch(r"\d{6}|\d{4}-\d{2}", s):
        y, mth = int(s[:4]), int(s[-2:])
        start = date(y, mth, 1)                                              # ValueError if month > 12
        nxt = date(y + (mth == 12), mth % 12 + 1, 1)
        return start, nxt - timedelta(days=1)
    raise ValueError(f"unrecognised period: {s!r}")


def condition_slug(text: Any) -> str:
    """'Acute Watery Diarrhoea (AWD)' -> 'acute_watery_diarrhoea_awd'"""
    return re.sub(r"[^a-z0-9]+", "_", str(text).strip().lower()).strip("_")


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

@dataclass
class HealthFacility:
    facility_id: str                       # as spelled in the feed
    name: str = ""
    district: str = ""
    lat: Optional[float] = None
    lon: Optional[float] = None


@dataclass
class HealthFieldMap:
    records_path: str = ""                 # dotted path to the list (objects) or the table object
    layout: str = "objects"                # "objects" or "table"
    facility: str = "facility_id"
    condition: str = "condition"
    count: str = "cases"
    period: str = "period"
    headers_key: str = "headers"           # table layout only
    rows_key: str = "rows"
    header_name_key: str = "name"          # each header is {"name": "..."} (or a plain string)


@dataclass
class HealthSettings:
    url: str
    field_map: HealthFieldMap = field(default_factory=HealthFieldMap)
    facilities: dict[str, HealthFacility] = field(default_factory=dict)   # empty = accept any facility
    allow_unknown_facilities: bool = False  # True: keep reports from unlisted facilities (flagged)

    condition_aliases: dict[str, str] = field(default_factory=dict)       # feed name -> canonical name
    alert_thresholds: dict[str, int] = field(default_factory=dict)        # canonical name -> cases
    # e.g. {"cholera": 1, "acute_watery_diarrhoea": 20}: reaching it sets threshold_exceeded=True

    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    http_timeout_s: float = 20.0

    period_tz: str = "Asia/Kathmandu"
    emit_history_on_first_poll: bool = True   # reports are aggregates; recent periods are useful baselines
    max_tracked: int = 100_000                # memory bound for the "already seen" table

    def __post_init__(self) -> None:
        validate_http_url(self.url)
        if self.field_map.layout not in ("objects", "table"):
            raise ValueError('layout must be "objects" or "table"')
        ZoneInfo(self.period_tz)
        # Normalise alias keys once so lookups are case-insensitive.
        self.condition_aliases = {condition_slug(k): condition_slug(v)
                                  for k, v in self.condition_aliases.items()}


def load_health_facilities(path: str | Path) -> dict[str, HealthFacility]:
    """CSV columns: facility_id,name,district,lat,lon  (only facility_id is required)."""
    def num(x: Optional[str]) -> Optional[float]:
        return float(x) if x not in (None, "") else None

    out: dict[str, HealthFacility] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            fid = row["facility_id"].strip()
            out[fid] = HealthFacility(fid, (row.get("name") or "").strip(),
                                      (row.get("district") or "").strip(),
                                      num(row.get("lat")), num(row.get("lon")))
    return out


# --------------------------------------------------------------------------- #
# Connector
# --------------------------------------------------------------------------- #

class HealthReportConnector(BaseConnector):
    """
    Example:
        settings = HealthSettings(
            url=os.environ["HEALTH_REPORTS_URL"],
            headers={"Authorization": f"Bearer {os.environ['HEALTH_REPORTS_TOKEN']}"},
            field_map=HealthFieldMap(records_path="reports", facility="health_post",
                                     condition="disease", count="cases", period="week"),
            facilities=load_health_facilities("data/health_posts.csv"),
            condition_aliases={"Acute watery diarrhoea": "diarrhoea", "AWD": "diarrhoea"},
            alert_thresholds={"cholera": 1},
        )
        connector = HealthReportConnector(
            ConnectorConfig(name="health_reports", kind=ObservationKind.HEALTH_REPORT,
                            poll_interval_s=3600, timeout_s=40),
            settings)
    """

    def __init__(
        self,
        config: ConnectorConfig,
        settings: HealthSettings,
        fetcher: Optional[Callable[[], bytes]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(config, logger)
        self.settings = settings
        self._fetcher = fetcher or self._http_get
        self._tz = ZoneInfo(settings.period_tz)
        # (station_id, condition, period) -> (last count, period end) for new/revised detection
        self._seen: dict[tuple[str, str, str], tuple[int, datetime]] = {}
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
        fm = self.settings.field_map
        try:
            doc = json.loads(raw)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ParseError(f"response is not valid JSON: {exc}") from exc
        try:
            node = _dig(doc, fm.records_path)
        except KeyError as exc:
            raise ParseError(f"records_path {fm.records_path!r} not found (missing {exc}); "
                             "has the feed format changed?") from exc

        rows = self._table_to_rows(node) if fm.layout == "table" else node
        if not isinstance(rows, list):
            raise ParseError(f"expected a list of reports, got {type(rows).__name__}")

        candidates: list[tuple[Observation, tuple[str, str, str], int]] = []
        malformed_before = self.feed_stats["rows_malformed"]
        for row in rows:
            self.feed_stats["rows"] += 1
            item = self._row_to_observation(row)
            if item is not None:
                candidates.append(item)

        if rows and self.feed_stats["rows_malformed"] - malformed_before == len(rows):
            raise ParseError(f"none of {len(rows)} rows could be read; has the feed format changed?")
        return self._select_changed(candidates)

    def _table_to_rows(self, node: Any) -> list[dict]:
        fm = self.settings.field_map
        if not isinstance(node, dict):
            raise ParseError("table layout: expected an object with headers and rows")
        try:
            headers, body = node[fm.headers_key], node[fm.rows_key]
        except KeyError as exc:
            raise ParseError(f"table layout: missing {exc}") from exc
        if not isinstance(headers, list) or not isinstance(body, list):
            raise ParseError("table layout: headers and rows must be lists")
        names = [str(h.get(fm.header_name_key)) if isinstance(h, dict) else str(h) for h in headers]
        rows: list[dict] = []
        for r in body:
            # A row of the wrong width is passed on as-is and counted as malformed later.
            rows.append(dict(zip(names, r)) if isinstance(r, list) and len(r) == len(names) else {})
        return rows

    def _row_to_observation(self, row: Any) -> Optional[tuple[Observation, tuple[str, str, str], int]]:
        s, fm = self.settings, self.settings.field_map
        if not isinstance(row, dict):
            self.feed_stats["rows_malformed"] += 1
            return None
        try:
            raw_facility = str(_dig(row, fm.facility)).strip()
            raw_condition = str(_dig(row, fm.condition)).strip()
            raw_count = _dig(row, fm.count)
            period_text = str(_dig(row, fm.period)).strip()
            period_start, period_end = parse_period(period_text)
            count_f = _to_float(raw_count)
        except (KeyError, ValueError, TypeError):
            self.feed_stats["rows_malformed"] += 1
            return None

        station_id = normalize_id(raw_facility)
        condition = condition_slug(raw_condition)
        condition = s.condition_aliases.get(condition, condition)
        if not station_id or not condition:
            self.feed_stats["rows_malformed"] += 1
            return None
        if count_f is None:
            self.feed_stats["no_report"] += 1         # blank/NA is "not reported", which is not zero
            return None
        if count_f < 0 or not count_f.is_integer():
            self.feed_stats["rows_malformed"] += 1    # a case count is a non-negative whole number
            return None
        count = int(count_f)

        facility = s.facilities.get(raw_facility)
        flags: list[str] = []
        if s.facilities and facility is None:
            if not s.allow_unknown_facilities:
                self.feed_stats["unknown_facility"] += 1
                if raw_facility not in self._warned_unknown:
                    self._warned_unknown.add(raw_facility)
                    self.log.warning("report from facility %r which is not in the facility registry; "
                                     "ignoring it (add it, or set allow_unknown_facilities=True)", raw_facility)
                return None
            flags.append("unregistered_facility")

        now = datetime.now(timezone.utc)
        period_close = datetime.combine(period_end + timedelta(days=1), time.min, tzinfo=self._tz) \
            .astimezone(timezone.utc)
        if period_close > now:
            observed_at = now
            flags.append("partial_period")            # the count can still grow
        else:
            observed_at = period_close

        payload: dict[str, Any] = {
            "station_id": station_id,
            "source_facility_id": raw_facility,
            "condition": condition,
            "source_condition": raw_condition,
            "value": count,
            "unit": "cases",
            "period": period_text,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "quality_flags": list(flags),
        }
        location = None
        if facility is not None:
            if facility.name:
                payload["facility_name"] = facility.name
            if facility.district:
                payload["district"] = facility.district
            if facility.lat is not None and facility.lon is not None:
                location = GeoPoint(facility.lat, facility.lon)
        threshold = s.alert_thresholds.get(condition)
        if threshold is not None:
            payload["alert_threshold"] = threshold
            payload["threshold_exceeded"] = count >= threshold

        obs = Observation(source=self.config.name, kind=ObservationKind.HEALTH_REPORT,
                          observed_at=observed_at, payload=payload, location=location,
                          quality_flags=list(flags))
        return obs, (station_id, condition, period_text), count

    def _select_changed(self, candidates: list[tuple[Observation, tuple[str, str, str], int]]) -> list[Observation]:
        """Emit reports that are new, or whose count changed since we last saw them."""
        out: list[Observation] = []
        for obs, key, count in candidates:
            previous = self._seen.get(key)
            if previous is None:
                if self._first_poll and not self.settings.emit_history_on_first_poll:
                    self.feed_stats["history_skipped"] += 1
                else:
                    out.append(obs)
            elif previous[0] != count:
                obs.payload["revised"] = True
                obs.payload["previous_value"] = previous[0]
                self.feed_stats["revised"] += 1
                out.append(obs)
            else:
                self.feed_stats["not_new"] += 1
            self._seen[key] = (count, datetime.fromisoformat(obs.payload["period_end"]).replace(tzinfo=timezone.utc))

        self._first_poll = False
        self._prune()
        self.feed_stats["emitted"] += len(out)
        return out

    def _prune(self) -> None:
        """Keep memory bounded: when the table is too big, forget the oldest periods."""
        limit = self.settings.max_tracked
        if len(self._seen) <= limit:
            return
        newest_first = sorted(self._seen.items(), key=lambda kv: kv[1][1], reverse=True)
        self._seen = dict(newest_first[: limit // 2])
        self.log.info("pruned the seen-reports table to %d entries", len(self._seen))