from __future__ import annotations
import asyncio
import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urljoin, urlsplit
from ingestion.connectors._common import http_get, http_post_json, normalize_id, validate_http_url
from ingestion.connectors.base import (
    BaseConnector,
    ConnectorConfig,
    GeoPoint,
    Observation,
    ObservationKind,
    ParseError,
)

log = logging.getLogger("prahari.ingestion.satellite_sentinel")

_FRACTION_RE = re.compile(r"(\.\d{6})\d+")


def parse_iso(text: Any) -> datetime:
    """ISO-8601 (with Z or an offset, any fractional precision) -> aware UTC datetime."""
    s = _FRACTION_RE.sub(r"\1", str(text).strip().replace("Z", "+00:00"))
    dt = datetime.fromisoformat(s)
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

@dataclass
class SentinelAOI:
    aoi_id: str                                  # becomes the station_id; letters, digits, _ and -
    bbox: tuple[float, float, float, float]      # west, south, east, north (degrees)
    name: str = ""

    def __post_init__(self) -> None:
        if not self.aoi_id or normalize_id(self.aoi_id) != self.aoi_id:
            raise ValueError(f"aoi_id {self.aoi_id!r} must match [A-Za-z0-9_-]{{1,64}}")
        if len(self.bbox) != 4:
            raise ValueError("bbox must be (west, south, east, north)")
        w, s, e, n = self.bbox
        if not (-180 <= w < e <= 180 and -90 <= s < n <= 90):
            raise ValueError(f"AOI {self.aoi_id!r}: bbox must satisfy west < east and south < north, within range")

    @property
    def centroid(self) -> GeoPoint:
        w, s, e, n = self.bbox
        return GeoPoint((s + n) / 2, (w + e) / 2)


@dataclass
class SentinelSettings:
    aois: list[SentinelAOI]
    search_url: str = "https://earth-search.aws.element84.com/v1/search"
    collections: tuple[str, ...] = ("sentinel-2-l2a", "sentinel-1-grd")
    lookback_hours: float = 72.0              # each poll searches this far back; scenes are emitted once
    high_cloud_cover_pct: float = 60.0
    page_limit: int = 100                     # items per page
    max_pages: int = 5                        # safety cap per AOI per poll
    asset_keys: tuple[str, ...] = ("thumbnail", "visual", "vv", "vh")   # hrefs to carry along (missing ones ignored)
    headers: dict[str, str] = field(default_factory=dict)
    http_timeout_s: float = 30.0
    max_tracked: int = 50_000                 # memory bound for the "already seen" table

    def __post_init__(self) -> None:
        validate_http_url(self.search_url)
        if not self.aois:
            raise ValueError("at least one AOI is required")
        if len({a.aoi_id for a in self.aois}) != len(self.aois):
            raise ValueError("duplicate aoi_id")
        if not self.collections:
            raise ValueError("at least one collection is required")
        if not 1 <= self.page_limit <= 1000 or self.max_pages < 1 or self.lookback_hours <= 0:
            raise ValueError("page_limit must be 1-1000, max_pages >= 1, lookback_hours > 0")


# --------------------------------------------------------------------------- #
# Connector
# --------------------------------------------------------------------------- #

class SentinelConnector(BaseConnector):
    """
    Example:
        settings = SentinelSettings(aois=[
            SentinelAOI("kaski", (83.6, 28.0, 84.4, 28.6), "Kaski district"),
            SentinelAOI("koshi_barrage", (86.8, 26.4, 87.2, 26.7), "Koshi barrage"),
        ])
        connector = SentinelConnector(
            ConnectorConfig(name="sentinel", kind=ObservationKind.SATELLITE_SCENE,
                            poll_interval_s=3 * 3600, timeout_s=120),
            settings)
    Set ConnectorConfig.timeout_s high enough for every AOI and page (requests run in sequence).
    """

    def __init__(self, config: ConnectorConfig, settings: SentinelSettings,
                 logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        self.settings = settings
        self._seen: dict[tuple[str, str], datetime] = {}      # (aoi_id, scene_id) -> acquired_at
        self._warned: set[str] = set()
        self.feed_stats: Counter[str] = Counter()

    # ---- fetch ----------------------------------------------------------- #

    async def fetch(self) -> list[tuple[str, list[bytes]]]:
        return await asyncio.to_thread(self._search_all)

    def _search_all(self) -> list[tuple[str, list[bytes]]]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=self.settings.lookback_hours)
        window = f"{start:%Y-%m-%dT%H:%M:%SZ}/{end:%Y-%m-%dT%H:%M:%SZ}"
        return [(aoi.aoi_id, self._search_aoi(aoi, window)) for aoi in self.settings.aois]

    def _search_aoi(self, aoi: SentinelAOI, window: str) -> list[bytes]:
        s = self.settings
        first_body = {"collections": list(s.collections), "bbox": list(aoi.bbox),
                      "datetime": window, "limit": s.page_limit}
        method, url, body = "POST", s.search_url, first_body
        pages: list[bytes] = []
        for _ in range(s.max_pages):
            if method == "POST":
                raw = http_post_json(url, body, headers=s.headers, timeout_s=s.http_timeout_s,
                                     accept="application/geo+json, application/json")
            else:
                raw = http_get(url, headers=s.headers, timeout_s=s.http_timeout_s,
                               accept="application/geo+json, application/json")
            pages.append(raw)
            nxt = self._resolve_next(raw, first_body)
            if nxt is None:
                break
            method, url, body = nxt
        return pages

    def _resolve_next(self, raw: bytes, first_body: dict) -> Optional[tuple[str, str, dict]]:
        """Follow the STAC 'next' link, but only to the same host as the search URL: a catalogue
        must not be able to send this connector (and its headers) somewhere else."""
        try:
            links = json.loads(raw).get("links") or []
        except (ValueError, AttributeError, UnicodeDecodeError):
            return None                       # parse() will report the bad page
        link = next((l for l in links if isinstance(l, dict) and l.get("rel") == "next"), None)
        if link is None or not isinstance(link.get("href"), str):
            return None
        href = urljoin(self.settings.search_url, link["href"])
        if urlsplit(href).scheme not in ("http", "https") or \
                urlsplit(href).netloc != urlsplit(self.settings.search_url).netloc:
            self._warn_once("next-host", "ignoring a 'next' link that points to a different host")
            return None
        method = str(link.get("method", "GET")).upper()
        if method == "POST":
            body = link.get("body") or {}
            if link.get("merge"):             # STAC: merge=true means "add this to the original request body"
                body = {**first_body, **body}
            return "POST", href, body
        return "GET", href, {}

    # ---- parse ----------------------------------------------------------- #

    def parse(self, raw: list[tuple[str, list[bytes]]]) -> list[Observation]:
        s = self.settings
        now = datetime.now(timezone.utc)
        aois = {a.aoi_id: a for a in s.aois}
        out: list[Observation] = []
        features_total = 0
        malformed_before = self.feed_stats["scenes_malformed"]

        for aoi_id, pages in raw:
            aoi = aois[aoi_id]
            for i, body in enumerate(pages):
                try:
                    doc = json.loads(body)
                except (ValueError, UnicodeDecodeError) as exc:
                    raise ParseError(f"AOI {aoi_id}: response is not valid JSON: {exc}") from exc
                features = doc.get("features") if isinstance(doc, dict) else None
                if not isinstance(features, list):
                    raise ParseError(f"AOI {aoi_id}: not a STAC FeatureCollection (no 'features' list)")
                if len(pages) == s.max_pages and i == len(pages) - 1 and self._has_next(doc):
                    self.feed_stats["truncated"] += 1
                    self._warn_once(f"trunc-{aoi_id}", f"AOI {aoi_id}: more than {s.max_pages} pages of results; "
                                    "raise max_pages or shorten lookback_hours")
                for feature in features:
                    features_total += 1
                    obs = self._feature_to_observation(aoi, feature, now)
                    if obs is not None:
                        out.append(obs)

        malformed_total = self.feed_stats["scenes_malformed"] - malformed_before
        if features_total and malformed_total == features_total:
            raise ParseError(f"none of {features_total} scenes could be read; has the catalogue format changed?")
        self._prune()
        self.feed_stats["emitted"] += len(out)
        return out

    @staticmethod
    def _has_next(doc: dict) -> bool:
        return any(isinstance(l, dict) and l.get("rel") == "next" for l in (doc.get("links") or []))

    def _feature_to_observation(self, aoi: SentinelAOI, feature: Any, now: datetime) -> Optional[Observation]:
        s = self.settings
        if not isinstance(feature, dict):
            self.feed_stats["scenes_malformed"] += 1
            return None
        props = feature.get("properties")
        scene_id = feature.get("id")
        try:
            acquired = parse_iso(props["datetime"])
            if not isinstance(scene_id, str) or not scene_id:
                raise ValueError("no id")
        except (KeyError, TypeError, ValueError):
            self.feed_stats["scenes_malformed"] += 1
            return None

        key = (aoi.aoi_id, scene_id)
        if key in self._seen:
            self.feed_stats["not_new"] += 1
            return None
        self._seen[key] = acquired

        flags: list[str] = []
        payload: dict[str, Any] = {
            "station_id": aoi.aoi_id,
            "scene_id": scene_id,
            "collection": feature.get("collection"),
            "acquired_at": acquired.isoformat(),
            "discovered_at": now.isoformat(),
            "lag_hours": round((now - acquired).total_seconds() / 3600, 2),
        }
        if aoi.name:
            payload["aoi_name"] = aoi.name
        for src, dst in (("platform", "platform"), ("sat:orbit_state", "orbit_state"),
                         ("sat:relative_orbit", "relative_orbit"), ("sar:polarizations", "polarizations"),
                         ("sar:instrument_mode", "instrument_mode")):
            if props.get(src) is not None:
                payload[dst] = props[src]
        cloud = props.get("eo:cloud_cover")
        if isinstance(cloud, (int, float)) and not isinstance(cloud, bool):
            payload["cloud_cover"] = round(float(cloud), 1)
            if cloud >= s.high_cloud_cover_pct:
                flags.append("high_cloud_cover")
        bbox = feature.get("bbox")
        if isinstance(bbox, list) and len(bbox) in (4, 6) and all(isinstance(v, (int, float)) for v in bbox):
            payload["scene_bbox"] = bbox
        assets = feature.get("assets") if isinstance(feature.get("assets"), dict) else {}
        payload["assets"] = {k: assets[k]["href"] for k in s.asset_keys
                             if isinstance(assets.get(k), dict) and isinstance(assets[k].get("href"), str)}
        self_link = next((l.get("href") for l in feature.get("links") or []
                          if isinstance(l, dict) and l.get("rel") == "self" and isinstance(l.get("href"), str)), None)
        if self_link:
            payload["stac_item_url"] = self_link
        payload["quality_flags"] = list(flags)

        return Observation(
            source=self.config.name, kind=ObservationKind.SATELLITE_SCENE, observed_at=acquired,
            payload=payload, location=aoi.centroid, raw_ref=self_link, quality_flags=list(flags),
        )

    # ---- housekeeping ---------------------------------------------------- #

    def _warn_once(self, key: str, message: str) -> None:
        if key not in self._warned:
            self._warned.add(key)
            self.log.warning(message)

    def _prune(self) -> None:
        if len(self._seen) <= self.settings.max_tracked:
            return
        newest = sorted(self._seen.items(), key=lambda kv: kv[1], reverse=True)
        self._seen = dict(newest[: self.settings.max_tracked // 2])
        self.log.info("pruned the seen-scenes table to %d entries", len(self._seen))