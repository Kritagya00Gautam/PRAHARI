
import asyncio
import hmac
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Optional, Protocol, Union
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException
from inference.api.routes_health import router as health_router

try:  # metrics are optional
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

    HTTP_REQUESTS = Counter("inference_http_requests_total", "HTTP requests",
                            ["method", "route", "status"])
    PREDICT_SECONDS = Histogram("inference_predict_seconds", "Prediction latency", ["model"])
    PREDICT_FAILURES = Counter("inference_predict_failures_total", "Failed predictions",
                               ["model", "kind"])
    METRICS_AVAILABLE = True
except ImportError:  # pragma: no cover
    METRICS_AVAILABLE = False

log = logging.getLogger("prahari.inference.api")

_MODEL_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_IMAGE_TYPES = {"image/jpeg", "image/png"}
_ERROR_CODES = {
    400: "bad_request", 401: "unauthorized", 404: "not_found", 405: "method_not_allowed",
    413: "payload_too_large", 415: "unsupported_media_type", 422: "invalid_input",
    429: "too_many_requests", 503: "unavailable", 504: "timeout",
}


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in ("1", "true", "yes", "on")


def _env_list(name: str) -> tuple:
    return tuple(x.strip() for x in os.getenv(name, "").split(",") if x.strip())


@dataclass(frozen=True)
class ApiSettings:
    api_keys: tuple = ()
    auth_disabled: bool = False
    cors_origins: tuple = ()
    docs_enabled: bool = True
    metrics_public: bool = False             # False: /metrics also needs the API key
    max_body_bytes: int = 10 * 1024 * 1024
    max_batch_items: int = 64
    max_concurrent_predictions: int = 4
    queue_timeout_s: float = 2.0             # wait this long for a free slot, then answer 429
    predict_timeout_s: float = 30.0

    @classmethod
    def from_env(cls) -> "ApiSettings":
        return cls(
            api_keys=_env_list("API_KEYS"),
            auth_disabled=_env_bool("API_AUTH_DISABLED", False),
            cors_origins=_env_list("API_CORS_ORIGINS"),
            docs_enabled=_env_bool("API_DOCS", True),
            metrics_public=_env_bool("API_METRICS_PUBLIC", False),
            max_body_bytes=int(os.getenv("API_MAX_BODY_BYTES", 10 * 1024 * 1024)),
            max_batch_items=int(os.getenv("API_MAX_BATCH_ITEMS", 64)),
            max_concurrent_predictions=int(os.getenv("API_MAX_CONCURRENT_PREDICTIONS", 4)),
            queue_timeout_s=float(os.getenv("API_QUEUE_TIMEOUT_S", 2.0)),
            predict_timeout_s=float(os.getenv("API_PREDICT_TIMEOUT_S", 30.0)),
        )


# --------------------------------------------------------------------------- #
# What the API needs from the rest of the service
# --------------------------------------------------------------------------- #

class ModelInfo(BaseModel):
    name: str
    version: str
    status: Literal["loading", "loaded", "failed"] = "loaded"
    device: Optional[str] = None
    loaded_at: Optional[datetime] = None
    input_spec: Optional[dict] = None        # e.g. {"type": "image", "size": [640, 640]}
    metadata: dict = Field(default_factory=dict)


class ModelInputError(ValueError):
    """Raise from a model when the CLIENT sent something unusable. Becomes HTTP 422."""


class LoadedModel(Protocol):
    info: ModelInfo

    async def predict(self, inputs: Any, options: dict) -> Any:
        """Return something JSON-serialisable."""

    # Optional: vision models also implement
    #   async def predict_image(self, data: bytes, content_type: str, options: dict) -> Any


class ModelRuntime(Protocol):
    def list_models(self) -> list: ...
    def get_model(self, name: str) -> Optional[LoadedModel]: ...
    async def reload_model(self, name: str) -> ModelInfo: ...
    def is_ready(self) -> bool: ...
    def nats_connected(self) -> bool: ...


class EmptyRuntime:
    """Default when no runtime is supplied: boots fine, reports not ready."""

    def list_models(self) -> list:
        return []

    def get_model(self, name: str) -> Optional[LoadedModel]:
        return None

    async def reload_model(self, name: str) -> ModelInfo:
        raise KeyError(name)

    def is_ready(self) -> bool:
        return False

    def nats_connected(self) -> bool:
        return False


# --------------------------------------------------------------------------- #
# Request / response models
# --------------------------------------------------------------------------- #

class PredictRequest(BaseModel):
    inputs: Union[dict, list]
    options: dict = Field(default_factory=dict)
    request_id: Optional[str] = Field(default=None, max_length=64)


class PredictResponse(BaseModel):
    model: str
    version: str
    request_id: str
    latency_ms: float
    outputs: Any


# --------------------------------------------------------------------------- #
# Small pure helpers (no FastAPI involved, easy to unit-test)
# --------------------------------------------------------------------------- #

def is_authorized(provided: Optional[str], keys: tuple) -> bool:
    """Constant-time comparison against every configured key (no early exit on a match)."""
    if not provided:
        return False
    matches = [hmac.compare_digest(provided.encode("utf-8"), k.encode("utf-8")) for k in keys]
    return any(matches)


def clean_request_id(value: Optional[str]) -> Optional[str]:
    """Accept a caller-supplied request id only if it is short and boring (it ends up in logs)."""
    return value if value and _REQUEST_ID_RE.match(value) else None


def format_validation_errors(errors: list) -> list:
    """Field, message and type only. Pydantic's own output also echoes the offending input."""
    return [{"loc": [str(p) for p in e.get("loc", ())], "msg": e.get("msg", ""), "type": e.get("type", "")}
            for e in errors]


def _error_response(request: Request, status: int, message: str, headers: Optional[dict] = None,
                    details: Optional[list] = None, code: Optional[str] = None) -> JSONResponse:
    body: dict = {"error": {"code": code or _ERROR_CODES.get(status, "error"), "message": message,
                            "request_id": getattr(request.state, "request_id", None)}}
    if details:
        body["error"]["details"] = details
    return JSONResponse(status_code=status, content=body, headers=headers)


# --------------------------------------------------------------------------- #
# Dependencies
# --------------------------------------------------------------------------- #

def require_api_key(request: Request, x_api_key: Optional[str] = Header(default=None)) -> None:
    settings: ApiSettings = request.app.state.settings
    if settings.auth_disabled:
        return
    if not settings.api_keys:                                   # fail closed, never "open by accident"
        raise HTTPException(503, "authentication is not configured (set API_KEYS)")
    if not is_authorized(x_api_key, settings.api_keys):
        raise HTTPException(401, "invalid or missing API key", headers={"WWW-Authenticate": "ApiKey"})


def _runtime(request: Request) -> ModelRuntime:
    return request.app.state.runtime


def _get_model_or_404(request: Request, name: str) -> LoadedModel:
    if not _MODEL_NAME_RE.match(name):
        raise HTTPException(404, "model not found")
    model = _runtime(request).get_model(name)
    if model is None:
        raise HTTPException(404, f"model {name!r} is not loaded")
    return model


def _slots(app: FastAPI) -> asyncio.Semaphore:
    """Concurrency limiter, created lazily so it binds to the running event loop."""
    if getattr(app.state, "predict_slots", None) is None:
        app.state.predict_slots = asyncio.Semaphore(app.state.settings.max_concurrent_predictions)
    return app.state.predict_slots


async def _read_body_capped(request: Request, limit: int) -> bytes:
    """Read the body, refusing as soon as it exceeds `limit` (also covers chunked uploads)."""
    chunks, total = [], 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise HTTPException(413, f"body larger than {limit} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


async def _run_prediction(request: Request, name: str, version: str, make_call,
                          request_id: Optional[str]) -> PredictResponse:
    settings: ApiSettings = request.app.state.settings
    slots = _slots(request.app)
    try:
        await asyncio.wait_for(slots.acquire(), timeout=settings.queue_timeout_s)
    except asyncio.TimeoutError:
        _count_failure(name, "busy")
        raise HTTPException(429, "all prediction slots are busy, retry shortly",
                            headers={"Retry-After": "1"})
    started = time.perf_counter()
    try:
        outputs = await asyncio.wait_for(make_call(), timeout=settings.predict_timeout_s)
    except asyncio.TimeoutError:
        _count_failure(name, "timeout")
        raise HTTPException(504, f"prediction exceeded {settings.predict_timeout_s:.0f}s")
    except ModelInputError:
        _count_failure(name, "bad_input")
        raise
    except Exception:
        _count_failure(name, "error")
        raise
    finally:
        slots.release()

    elapsed = time.perf_counter() - started
    if METRICS_AVAILABLE:
        PREDICT_SECONDS.labels(model=name).observe(elapsed)
    return PredictResponse(model=name, version=version,
                           request_id=request_id or getattr(request.state, "request_id", ""),
                           latency_ms=round(elapsed * 1000, 2), outputs=outputs)


def _count_failure(model: str, kind: str) -> None:
    if METRICS_AVAILABLE:
        PREDICT_FAILURES.labels(model=model, kind=kind).inc()


# --------------------------------------------------------------------------- #
# Routers
# --------------------------------------------------------------------------- #

models = APIRouter(prefix="/v1/models", tags=["models"], dependencies=[Depends(require_api_key)])
predict = APIRouter(prefix="/v1/predict", tags=["predict"], dependencies=[Depends(require_api_key)])


@models.get("", response_model=list[ModelInfo])
async def list_models(request: Request) -> list:
    return _runtime(request).list_models()


@models.get("/{name}", response_model=ModelInfo)
async def get_model(name: str, request: Request) -> ModelInfo:
    return _get_model_or_404(request, name).info


@models.post("/{name}/reload", response_model=ModelInfo)
async def reload_model(name: str, request: Request) -> ModelInfo:
    if not _MODEL_NAME_RE.match(name):
        raise HTTPException(404, "model not found")
    try:
        info = await _runtime(request).reload_model(name)
    except KeyError:
        raise HTTPException(404, f"model {name!r} is not known to the registry")
    log.info("model %s reloaded: version %s (request %s)", name, info.version,
             getattr(request.state, "request_id", "?"))
    return info


@predict.post("/{name}", response_model=PredictResponse)
async def predict_json(name: str, body: PredictRequest, request: Request) -> PredictResponse:
    model = _get_model_or_404(request, name)
    settings: ApiSettings = request.app.state.settings
    if isinstance(body.inputs, list) and len(body.inputs) > settings.max_batch_items:
        raise HTTPException(413, f"at most {settings.max_batch_items} items per request")
    return await _run_prediction(request, name, model.info.version,
                                 lambda: model.predict(body.inputs, body.options), body.request_id)


@predict.post("/{name}/image", response_model=PredictResponse)
async def predict_image(name: str, request: Request) -> PredictResponse:
    model = _get_model_or_404(request, name)
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if content_type not in _IMAGE_TYPES:
        raise HTTPException(415, "Content-Type must be image/jpeg or image/png")
    handler = getattr(model, "predict_image", None)
    if handler is None:
        raise HTTPException(400, f"model {name!r} does not accept images")
    data = await _read_body_capped(request, request.app.state.settings.max_body_bytes)
    if not data:
        raise HTTPException(400, "empty body")
    # Query-string parameters are passed through as strings; the model must validate them.
    options = {k: v for k, v in list(request.query_params.items())[:16] if len(k) <= 64 and len(v) <= 64}
    return await _run_prediction(request, name, model.info.version,
                                 lambda: handler(data, content_type, options), None)


# --------------------------------------------------------------------------- #
# App factory
# --------------------------------------------------------------------------- #

def create_app(runtime: Optional[ModelRuntime] = None,
               settings: Optional[ApiSettings] = None) -> FastAPI:
    settings = settings or ApiSettings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.started_at = time.time()
        if settings.auth_disabled:
            log.warning("API authentication is DISABLED (API_AUTH_DISABLED). Do not use this in production.")
        elif not settings.api_keys:
            log.warning("API_KEYS is empty: protected endpoints will answer 503 until it is set.")
        log.info("inference API started")
        yield
        log.info("inference API stopped")

    docs = settings.docs_enabled
    app = FastAPI(title="Prahari inference service", version="1.0.0", lifespan=lifespan,
                  docs_url="/docs" if docs else None, redoc_url="/redoc" if docs else None,
                  openapi_url="/openapi.json" if docs else None)
    app.state.settings = settings
    app.state.runtime = runtime or EmptyRuntime()
    app.state.predict_slots = None

    if settings.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=list(settings.cors_origins),
                           allow_methods=["GET", "POST"], allow_credentials=False,
                           allow_headers=["X-API-Key", "Content-Type", "X-Request-ID"])

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = clean_request_id(request.headers.get("x-request-id")) or uuid.uuid4().hex
        started = time.perf_counter()

        declared = request.headers.get("content-length")
        response: Optional[Response] = None
        if declared is not None:
            try:
                size = int(declared)
                if size < 0:
                    raise ValueError
            except ValueError:
                response = _error_response(request, 400, "invalid Content-Length header")
            else:
                if size > settings.max_body_bytes:
                    response = _error_response(request, 413, f"body larger than {settings.max_body_bytes} bytes")
        if response is None:
            try:
                response = await call_next(request)
            except Exception:
                # The 500 reply is produced by on_unexpected_error; here we only make sure the
                # failure shows up in the HTTP metrics before it propagates.
                if METRICS_AVAILABLE:
                    HTTP_REQUESTS.labels(request.method, "unhandled", "500").inc()
                raise

        elapsed_ms = (time.perf_counter() - started) * 1000
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["X-Process-Time-Ms"] = f"{elapsed_ms:.1f}"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"

        route = request.scope.get("route")
        route_label = getattr(route, "path", "unmatched")        # template, not the raw path: bounded labels
        if METRICS_AVAILABLE:
            HTTP_REQUESTS.labels(request.method, route_label, str(response.status_code)).inc()
        if route_label not in ("/healthz", "/readyz", "/startupz", "/metrics"):
            log.info("%s %s -> %d in %.1fms [%s]", request.method, request.url.path,
                     response.status_code, elapsed_ms, request.state.request_id)
        return response

    # Uniform error body: {"error": {"code", "message", "request_id", "details"?}}
    @app.exception_handler(StarletteHTTPException)
    async def on_http_error(request: Request, exc: StarletteHTTPException):
        return _error_response(request, exc.status_code, str(exc.detail), headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def on_validation_error(request: Request, exc: RequestValidationError):
        return _error_response(request, 422, "request validation failed",
                               details=format_validation_errors(exc.errors()))

    @app.exception_handler(ModelInputError)
    async def on_model_input_error(request: Request, exc: ModelInputError):
        return _error_response(request, 422, str(exc) or "the model rejected the input")

    @app.exception_handler(Exception)
    async def on_unexpected_error(request: Request, exc: Exception):
        log.exception("unhandled error [%s]", getattr(request.state, "request_id", "?"))
        return _error_response(request, 500, "internal server error")   # details stay in the logs

    app.include_router(health_router)
    app.include_router(models)
    app.include_router(predict)

    if METRICS_AVAILABLE:
        deps = [] if settings.metrics_public else [Depends(require_api_key)]

        @app.get("/metrics", include_in_schema=False, dependencies=deps)
        async def metrics() -> Response:
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app