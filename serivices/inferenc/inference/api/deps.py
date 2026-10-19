import hmac
import re
from typing import Optional
from fastapi import Header, HTTPException, Request
from inference.api.schemas import LoadedModel, ModelRuntime

MODEL_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def is_authorized(provided: Optional[str], keys: tuple) -> bool:
    """Constant-time comparison against every configured key (no early exit on a match)."""
    if not provided:
        return False
    matches = [hmac.compare_digest(provided.encode("utf-8"), k.encode("utf-8")) for k in keys]
    return any(matches)


def require_api_key(request: Request, x_api_key: Optional[str] = Header(default=None)) -> None:
    settings = request.app.state.settings
    if settings.auth_disabled:
        return
    if not settings.api_keys:                                   # fail closed, never "open by accident"
        raise HTTPException(503, "authentication is not configured (set API_KEYS)")
    if not is_authorized(x_api_key, settings.api_keys):
        raise HTTPException(401, "invalid or missing API key", headers={"WWW-Authenticate": "ApiKey"})


def get_runtime(request: Request) -> ModelRuntime:
    return request.app.state.runtime


def valid_model_name(name: str) -> bool:
    """Reject anything that is not a plain identifier before it reaches the runtime or the logs."""
    return bool(MODEL_NAME_RE.match(name))


def get_model_or_404(request: Request, name: str) -> LoadedModel:
    if not valid_model_name(name):
        raise HTTPException(404, "model not found")
    model = get_runtime(request).get_model(name)
    if model is None:
        raise HTTPException(404, f"model {name!r} is not loaded")
    return model