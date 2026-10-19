import asyncio
import dataclasses
import logging
import math
import time
from datetime import date, datetime
from typing import Any, Callable, Optional
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from inference.api.deps import clean_request_id, get_model_or_404, require_api_key
from inference.api.schemas import ModelInputError, PredictRequest, PredictResponse
from inference.monitoring.metrics import count_prediction_failure, observe_prediction

log = logging.getLogger("prahari.inference.predict")

IMAGE_TYPES = {"image/jpeg", "image/png"}
_MAX_JSON_DEPTH = 32

router = APIRouter(prefix="/v1/predict", tags=["predict"], dependencies=[Depends(require_api_key)])


# --------------------------------------------------------------------------- #
# Plain helpers (no FastAPI involved, easy to unit-test)
# --------------------------------------------------------------------------- #

def sniff_image_type(data: bytes) -> Optional[str]:
    """Identify JPEG/PNG from the file signature. None if it is neither."""
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    return None


def to_jsonable(obj: Any, _depth: int = 0) -> Any:
    """Convert a model's output into plain JSON types. Raises TypeError for anything it cannot
    represent (bytes, arbitrary objects) rather than guessing."""
    if _depth > _MAX_JSON_DEPTH:
        raise TypeError("output is nested too deeply")
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None             # JSON has no NaN or Infinity
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v, _depth + 1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_jsonable(v, _depth + 1) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "tolist"):                                  # numpy arrays and scalars
        return to_jsonable(obj.tolist(), _depth + 1)
    if hasattr(obj, "model_dump"):                              # pydantic v2 models
        return to_jsonable(obj.model_dump(), _depth + 1)
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return to_jsonable(dataclasses.asdict(obj), _depth + 1)
    raise TypeError(f"output of type {type(obj).__name__} is not JSON-serialisable")


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


async def _run_prediction(request: Request, name: str, version: str,
                          make_call: Callable[[], Any], caller_request_id: Optional[str]) -> PredictResponse:
    settings = request.app.state.settings
    slots = _slots(request.app)
    try:
        await asyncio.wait_for(slots.acquire(), timeout=settings.queue_timeout_s)
    except asyncio.TimeoutError:
        count_prediction_failure(name, "busy")
        raise HTTPException(429, "all prediction slots are busy, retry shortly",
                            headers={"Retry-After": "1"})

    started = time.perf_counter()
    try:
        outputs = await asyncio.wait_for(make_call(), timeout=settings.predict_timeout_s)
    except asyncio.TimeoutError:
        count_prediction_failure(name, "timeout")
        raise HTTPException(504, f"prediction exceeded {settings.predict_timeout_s:.0f}s")
    except ModelInputError:
        count_prediction_failure(name, "bad_input")
        raise
    except Exception:
        count_prediction_failure(name, "error")
        raise
    finally:
        slots.release()

    elapsed = time.perf_counter() - started
    try:
        outputs = to_jsonable(outputs)
    except TypeError as exc:
        count_prediction_failure(name, "error")
        log.error("model %s returned an unusable output: %s", name, exc)
        raise HTTPException(500, "the model returned an output that cannot be serialised")

    observe_prediction(name, elapsed)
    return PredictResponse(
        model=name, version=version,
        request_id=caller_request_id or getattr(request.state, "request_id", ""),
        latency_ms=round(elapsed * 1000, 2), outputs=outputs,
    )


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #

@router.get("/limits")
async def limits(request: Request) -> dict:
    s = request.app.state.settings
    return {
        "max_batch_items": s.max_batch_items,
        "max_body_bytes": s.max_body_bytes,
        "max_concurrent_predictions": s.max_concurrent_predictions,
        "predict_timeout_s": s.predict_timeout_s,
        "image_types": sorted(IMAGE_TYPES),
    }


@router.post("/{name}", response_model=PredictResponse)
async def predict_json(name: str, body: PredictRequest, request: Request) -> PredictResponse:
    model = get_model_or_404(request, name)
    max_items = request.app.state.settings.max_batch_items
    if isinstance(body.inputs, list) and len(body.inputs) > max_items:
        raise HTTPException(413, f"at most {max_items} items per request")
    return await _run_prediction(request, name, model.info.version,
                                 lambda: model.predict(body.inputs, body.options),
                                 clean_request_id(body.request_id))


@router.post("/{name}/image", response_model=PredictResponse)
async def predict_image(name: str, request: Request) -> PredictResponse:
    model = get_model_or_404(request, name)

    declared = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if declared not in IMAGE_TYPES:
        raise HTTPException(415, "Content-Type must be image/jpeg or image/png")
    handler = getattr(model, "predict_image", None)
    if handler is None:                                     # check before reading a big upload
        raise HTTPException(400, f"model {name!r} does not accept images")

    data = await _read_body_capped(request, request.app.state.settings.max_body_bytes)
    if not data:
        raise HTTPException(400, "empty body")
    if sniff_image_type(data) != declared:
        raise HTTPException(415, f"the body is not a valid {declared} image")

    # Query-string parameters reach the model as strings (bounded); the model must validate them.
    options = {k: v for k, v in list(request.query_params.items())[:16] if len(k) <= 64 and len(v) <= 64}
    return await _run_prediction(request, name, model.info.version,
                                 lambda: handler(data, declared, options), None)