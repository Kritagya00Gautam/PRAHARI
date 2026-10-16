from __future__ import annotations
import asyncio
import hashlib
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol
from ingestion.connectors.base import (
    BaseConnector,
    ConnectorConfig,
    GeoPoint,
    Observation,
    ObservationKind,
    ParseError,
    PermanentFetchError,
    TransientFetchError,
)

log = logging.getLogger("prahari.ingestion.camera_stream")

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")          # must be a valid NATS subject token
_CREDS_RE = re.compile(r"(://)[^/@\s]+@")


def redact_url(url: str) -> str:
    """rtsp://user:pass@host/x -> rtsp://***@host/x"""
    return _CREDS_RE.sub(r"\1***@", url)



@dataclass
class CameraSettings:
    camera_id: str                       # becomes the station_id / NATS subject token
    url: str                             # rtsp://..., http://.../video.mjpg, a file path, or "0"
    lat: Optional[float] = None
    lon: Optional[float] = None
    elevation_m: Optional[float] = None

    frames_dir: Path = Path("data/frames")
    save_frames: bool = True
    jpeg_quality: int = 85
    max_width: int = 1280                # larger frames are downscaled before saving/analysis

    max_frame_age_s: float = 10.0        # a frame older than this means the camera has stalled
    new_frame_wait_s: float = 2.0        # how long to wait for a frame newer than the last one
    reconnect_wait_s: float = 2.0        # initial reconnect delay (doubles up to 30s)

    min_brightness: float = 25.0         # mean gray level (0-255) below which a frame is "dark"
    min_sharpness: float = 40.0          # variance of the Laplacian below which it is "blurry"
    frozen_after: int = 3                # identical frames in a row before flagging "frozen"

    def __post_init__(self) -> None:
        if not _ID_RE.match(self.camera_id):
            raise ValueError(f"camera_id {self.camera_id!r} must match [A-Za-z0-9_-]{{1,64}}")
        if not self.url:
            raise ValueError("camera url must not be empty")
        if not 1 <= self.jpeg_quality <= 100:
            raise ValueError("jpeg_quality must be between 1 and 100")
        if self.max_width < 64:
            raise ValueError("max_width must be at least 64")
        self.frames_dir = Path(self.frames_dir)




class FrameSource(Protocol):
    """Anything that can hand over the most recent frame. Swap in a fake for tests."""
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def latest(self) -> Optional[tuple[Any, float]]:
        """(frame as a numpy BGR array, capture time as epoch seconds), or None if no frame yet."""


class OpenCVFrameSource:
    """Reads a stream continuously in a background thread and keeps only the newest frame.
    Reconnects automatically (with backoff) when the stream drops."""

    def __init__(self, url: str, reconnect_wait_s: float = 2.0):
        self._url = url
        self._reconnect_wait_s = reconnect_wait_s
        self._lock = threading.Lock()
        self._frame: Any = None
        self._ts = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.connected = False

    def start(self) -> None:
        import cv2  # noqa: F401  (fail early with ImportError if OpenCV is missing)
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="camera-reader", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def latest(self) -> Optional[tuple[Any, float]]:
        with self._lock:
            return None if self._frame is None else (self._frame, self._ts)

    def _loop(self) -> None:
        import cv2

        is_live = self._url.isdigit() or "://" in self._url
        wait = self._reconnect_wait_s
        cap = None
        while not self._stop.is_set():
            if cap is None:
                cap = cv2.VideoCapture(int(self._url) if self._url.isdigit() else self._url)
                if not cap.isOpened():
                    cap.release()
                    cap = None
                    self.connected = False
                    self._stop.wait(wait)
                    wait = min(wait * 2, 30.0)
                    continue
                try:
                    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)   # not honoured by every backend
                except Exception:
                    pass
                self.connected = True
                wait = self._reconnect_wait_s

            ok, frame = cap.read()
            if not ok or frame is None:
                cap.release()                             # end of file or dropped stream
                cap = None
                self.connected = False
                self._stop.wait(wait)
                continue

            with self._lock:
                self._frame, self._ts = frame, time.time()

            if not is_live:                               # video file: play at its own speed
                fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
                self._stop.wait(1.0 / fps)

        if cap is not None:
            cap.release()




class CameraStreamConnector(BaseConnector):
    """
    Example:
        cam = CameraSettings(camera_id="cam_phewa_01", url=os.environ["PHEWA_CAM_URL"],
                             lat=28.2096, lon=83.9856)
        cfg = ConnectorConfig(name="cam_phewa_01", kind=ObservationKind.CAMERA_FRAME,
                              poll_interval_s=10)
        connector = CameraStreamConnector(cfg, cam)
    """

    def __init__(
        self,
        config: ConnectorConfig,
        camera: CameraSettings,
        source: Optional[FrameSource] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(config, logger)
        self.camera = camera
        self._source: FrameSource = source or OpenCVFrameSource(camera.url, camera.reconnect_wait_s)
        self._last_ts = 0.0
        self._last_hash: Optional[str] = None
        self._repeat_count = 0

    

    async def open(self) -> None:
        self.log.info("starting camera %s (%s)", self.camera.camera_id, redact_url(self.camera.url))
        try:
            self._source.start()
        except ImportError as exc:
            raise PermanentFetchError(
                "OpenCV is not installed (pip install opencv-python-headless numpy)") from exc
        if self.camera.save_frames:
            self.camera.frames_dir.mkdir(parents=True, exist_ok=True)

    async def close(self) -> None:
        await asyncio.to_thread(self._source.stop)

  

    async def fetch(self) -> dict[str, Any]:
        cam = self.camera
        deadline = time.time() + cam.new_frame_wait_s
        while True:
            latest = self._source.latest()
            if latest is not None and latest[1] != self._last_ts:
                break
            if time.time() >= deadline:
                what = "no frame received yet (camera connecting?)" if latest is None \
                    else f"no new frame within {cam.new_frame_wait_s:.0f}s"
                raise TransientFetchError(what)
            await asyncio.sleep(0.05)

        frame, ts = latest
        age = time.time() - ts
        if age > cam.max_frame_age_s:
            raise TransientFetchError(f"stale frame ({age:.0f}s old): camera stalled or disconnected")

        try:
            result = await asyncio.to_thread(self._process, frame, ts)
        except OSError as exc:                       # disk full, permissions, ...
            raise TransientFetchError(f"could not save frame: {exc}") from exc
        self._last_ts = ts
        return result

    def parse(self, raw: dict[str, Any]) -> list[Observation]:
        cam = self.camera
        try:
            captured_at: datetime = raw["captured_at"]
            frame_hash: str = raw["frame_hash"]
            brightness: float = raw["brightness"]
            sharpness: float = raw["sharpness"]
        except (KeyError, TypeError) as exc:
            raise ParseError(f"malformed frame record: {exc}") from exc

        # Frozen-camera detection: the exact same pixels again and again.
        self._repeat_count = self._repeat_count + 1 if frame_hash == self._last_hash else 1
        self._last_hash = frame_hash

        flags: list[str] = []
        if brightness < cam.min_brightness:
            flags.append("dark")
        if sharpness < cam.min_sharpness:
            flags.append("blurry")
        if self._repeat_count >= cam.frozen_after:
            flags.append("frozen_frame")
        if cam.save_frames and raw.get("frame_path") is None:
            flags.append("frame_not_saved")

        location = GeoPoint(cam.lat, cam.lon, cam.elevation_m) \
            if cam.lat is not None and cam.lon is not None else None

        return [Observation(
            source=self.config.name,
            kind=ObservationKind.CAMERA_FRAME,
            observed_at=captured_at,
            location=location,
            raw_ref=raw.get("frame_path"),
            quality_flags=list(flags),
            payload={
                "station_id": cam.camera_id,
                "camera_id": cam.camera_id,
                "width": raw["width"],
                "height": raw["height"],
                "brightness": round(brightness, 2),
                "sharpness": round(sharpness, 2),
                "frame_hash": frame_hash,
                "frame_path": raw.get("frame_path"),
                "repeat_count": self._repeat_count,
                "quality_flags": list(flags),
            },
        )]


    def _process(self, frame: Any, ts: float) -> dict[str, Any]:
        import cv2

        cam = self.camera
        h, w = frame.shape[:2]
        if w > cam.max_width:
            scale = cam.max_width / w
            frame = cv2.resize(frame, (cam.max_width, max(1, int(h * scale))),
                               interpolation=cv2.INTER_AREA)
            h, w = frame.shape[:2]

        gray = frame if frame.ndim == 2 else cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        brightness = float(gray.mean())
        sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        # Hash the full-resolution pixels: sensor noise makes a live static scene differ
        # from frame to frame, while a frozen stream repeats exactly.
        frame_hash = hashlib.blake2b(gray.tobytes(), digest_size=8).hexdigest()

        captured_at = datetime.fromtimestamp(ts, tz=timezone.utc)
        frame_path: Optional[str] = None
        if cam.save_frames:
            frame_path = self._save_jpeg(frame, captured_at)

        return {
            "captured_at": captured_at,
            "width": w,
            "height": h,
            "brightness": brightness,
            "sharpness": sharpness,
            "frame_hash": frame_hash,
            "frame_path": frame_path,
        }

    def _save_jpeg(self, frame: Any, captured_at: datetime) -> str:
        import cv2

        cam = self.camera
        ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, cam.jpeg_quality])
        if not ok:
            raise OSError("JPEG encoding failed")
        folder = cam.frames_dir / cam.camera_id / captured_at.strftime("%Y-%m-%d")
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{captured_at.strftime('%H%M%S_%f')[:-3]}.jpg"
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(buf.tobytes())
        os.replace(tmp, path)                        # atomic: readers never see a half-written file
        return str(path)