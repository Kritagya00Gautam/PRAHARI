""

import asyncio
import logging
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request

from inference.api.deps import get_runtime, require_api_key, valid_model_name
from inference.api.schemas import ModelInfo

log = logging.getLogger("prahari.inference.models")

DEFAULT_RELOAD_TIMEOUT_S = 120.0

router = APIRouter(prefix="/v1/models", tags=["models"], dependencies=[Depends(require_api_key)])


# --------------------------------------------------------------------------- #
# Plain helpers (no FastAPI involved, easy to unit-test)
# --------------------------------------------------------------------------- #

def filter_models(models: list, status: Optional[str]) -> list:
    return list(models) if status is None else [m for m in models if m.status == status]


def find_info(models: list, name: str) -> Optional[ModelInfo]:
    return next((m for m in models if m.name == name), None)


def _loaded_version(runtime, name: str) -> Optional[str]:
    model = runtime.get_model(name)
    return model.info.version if model is not None else None


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #

@router.get("", response_model=list[ModelInfo])
async def list_models(
    request: Request,
    status: Optional[Literal["loading", "loaded", "failed"]] = None,
) -> list:
    return filter_models(get_runtime(request).list_models(), status)


@router.get("/{name}", response_model=ModelInfo)
async def get_model_info(name: str, request: Request) -> ModelInfo:
    # Look in the full listing rather than at loaded models only, so a model that failed to load
    # or is still loading can be inspected too.
    info = find_info(get_runtime(request).list_models(), name) if valid_model_name(name) else None
    if info is None:
        raise HTTPException(404, f"model {name!r} not found")
    return info


@router.post("/{name}/reload", response_model=ModelInfo)
async def reload_model(name: str, request: Request) -> ModelInfo:
    if not valid_model_name(name):
        raise HTTPException(404, "model not found")

    state = request.app.state
    reloading = getattr(state, "reloading", None)
    if reloading is None:
        reloading = state.reloading = set()
    if name in reloading:       # single event loop: no await between this check and the add below
        raise HTTPException(409, "a reload of this model is already in progress",
                            headers={"Retry-After": "5"})

    runtime = get_runtime(request)
    timeout = getattr(state.settings, "reload_timeout_s", DEFAULT_RELOAD_TIMEOUT_S)
    request_id = getattr(request.state, "request_id", "?")
    previous = _loaded_version(runtime, name)

    reloading.add(name)
    try:
        info = await asyncio.wait_for(runtime.reload_model(name), timeout=timeout)
    except KeyError:
        raise HTTPException(404, f"model {name!r} is not known to the registry")
    except asyncio.TimeoutError:
        log.error("reload of %s timed out after %.0fs [%s]", name, timeout, request_id)
        raise HTTPException(504, f"reload exceeded {timeout:.0f}s; any previously loaded version keeps serving")
    except Exception:
        log.exception("reload of %s failed [%s]", name, request_id)
        raise HTTPException(500, "reload failed; any previously loaded version keeps serving (see the service logs)")
    finally:
        reloading.discard(name)

    log.info("model %s reloaded: %s -> %s [%s]", name, previous or "none", info.version, request_id)
    return info