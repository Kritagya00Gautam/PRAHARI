from __future__ import annotations
import json
import re
import socket
import urllib.error
import urllib.request
from typing import Any, Mapping, Optional
from urllib.parse import urlencode, urlsplit
from ingestion.connectors.base import PermanentFetchError, TransientFetchError

MAX_BODY_BYTES = 5 * 1024 * 1024
_MISSING = {"", "na", "n/a", "nan", "null", "none", "-", "--"}


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

def validate_http_url(url: str) -> None:
    """Only http(s). urllib would also open file:// and ftp:// URLs, which must never be possible
    from configuration."""
    if urlsplit(url).scheme not in ("http", "https"):
        raise ValueError("url must start with http:// or https://")


def safe_location(url: str) -> str:
    """scheme://host[:port]/path for logs: no user:password@, no query string."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    return f"{parts.scheme}://{host}{port}{parts.path}"


def _request(
    method: str,
    url: str,
    *,
    headers: Optional[Mapping[str, str]],
    params: Optional[Mapping[str, str]],
    data: Optional[bytes],
    timeout_s: float,
    max_bytes: int,
    accept: str,
) -> bytes:
    validate_http_url(url)
    full = url + (("&" if "?" in url else "?") + urlencode(params) if params else "")
    hdrs = {"Accept": accept, "User-Agent": "prahari-ingestion/1.0"}
    if data is not None:
        hdrs["Content-Type"] = "application/json"
    hdrs.update(headers or {})
    req = urllib.request.Request(full, data=data, method=method, headers=hdrs)
    where = safe_location(url)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:
        if exc.code in (400, 401, 403, 404, 410, 422):
            raise PermanentFetchError(f"HTTP {exc.code} from {where}") from exc
        raise TransientFetchError(f"HTTP {exc.code} from {where}") from exc
    except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as exc:
        raise TransientFetchError(f"cannot reach {where}: {getattr(exc, 'reason', exc)}") from exc
    if len(body) > max_bytes:
        raise PermanentFetchError(f"response from {where} exceeds {max_bytes} bytes")
    return body


def http_get(
    url: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    params: Optional[Mapping[str, str]] = None,
    timeout_s: float = 15.0,
    max_bytes: int = MAX_BODY_BYTES,
    accept: str = "application/json",
) -> bytes:
    """Blocking GET (run it with asyncio.to_thread). Failure mapping:
         400/401/403/404/410/422, oversized body -> PermanentFetchError  (retrying will not help)
         5xx, 429, timeouts, connection errors   -> TransientFetchError  (safe to retry)
    """
    return _request("GET", url, headers=headers, params=params, data=None,
                    timeout_s=timeout_s, max_bytes=max_bytes, accept=accept)


def http_post_json(
    url: str,
    body: Any,
    *,
    headers: Optional[Mapping[str, str]] = None,
    timeout_s: float = 15.0,
    max_bytes: int = MAX_BODY_BYTES,
    accept: str = "application/json",
) -> bytes:
    """Blocking POST of a JSON body, with the same failure mapping as http_get."""
    return _request("POST", url, headers=headers, params=None, data=json.dumps(body).encode("utf-8"),
                    timeout_s=timeout_s, max_bytes=max_bytes, accept=accept)


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

def dig(obj: Any, path: str) -> Any:
    """Follow a dotted path through nested dicts ("data.readings"). KeyError if a step is missing."""
    for part in filter(None, path.split(".")):
        if not isinstance(obj, dict) or part not in obj:
            raise KeyError(part)
        obj = obj[part]
    return obj


def to_float(value: Any) -> Optional[float]:
    """None means 'the feed has no value' (blank, NA, null). ValueError means malformed."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if text.lower() in _MISSING:
        return None
    return float(text)


def normalize_id(raw: Any) -> str:
    """Make an id safe as a NATS subject token: letters, digits, _ and - only ("283.5" -> "283_5")."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", str(raw).strip())[:64]