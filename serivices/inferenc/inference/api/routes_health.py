import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("prahari.inference.health")

router = APIRouter(tags=["health"])


# --------------------------------------------------------------------------- #
# Readiness logic (plain Python, so it can be unit-tested without FastAPI)
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ReadinessReport:
    ready: bool
    runtime_ready: bool
    nats_connected: bool
    models_loaded: int
    models_loading: int
    models_failed: int
    failed_checks: tuple = ()       # names of checks that raised; never the error text

    def to_dict(self) -> dict:
        body = {
            "ready": self.ready,
            "runtime_ready": self.runtime_ready,
            "nats": self.nats_connected,
            "models_loaded": self.models_loaded,
            "models_loading": self.models_loading,
            "models_failed": self.models_failed,
        }
        if self.failed_checks:
            body["failed_checks"] = list(self.failed_checks)
        return body


def evaluate_readiness(runtime: Any) -> ReadinessReport:
    """Ask the runtime three questions. Any of them may raise; none of that escapes."""
    failed: list = []

    def ask(name: str, fn: Callable, default: Any) -> Any:
        try:
            return fn()
        except Exception:
            log.exception("readiness check %r raised", name)
            failed.append(name)
            return default

    models = ask("list_models", runtime.list_models, [])
    runtime_ready = bool(ask("is_ready", runtime.is_ready, False))
    nats = bool(ask("nats_connected", runtime.nats_connected, False))

    statuses = [getattr(m, "status", None) for m in models]
    loaded = statuses.count("loaded")
    return ReadinessReport(
        ready=runtime_ready and nats and loaded > 0,
        runtime_ready=runtime_ready,
        nats_connected=nats,
        models_loaded=loaded,
        models_loading=statuses.count("loading"),
        models_failed=statuses.count("failed"),
        failed_checks=tuple(failed),
    )


def _check(request: Request) -> ReadinessReport:
    report = evaluate_readiness(request.app.state.runtime)
    if report.ready:
        request.app.state.startup_complete = True       # latch for /startupz
    return report


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #

@router.get("/healthz")
async def healthz(request: Request) -> dict:
    body: dict = {"status": "ok"}
    started = getattr(request.app.state, "started_at", None)
    if started:
        body["uptime_s"] = round(time.time() - started)
    return body


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    report = _check(request)
    return JSONResponse(status_code=200 if report.ready else 503, content=report.to_dict())


@router.get("/startupz")
async def startupz(request: Request) -> JSONResponse:
    if getattr(request.app.state, "startup_complete", False):
        return JSONResponse(status_code=200, content={"started": True})
    report = _check(request)
    body = report.to_dict()
    body["started"] = report.ready
    return JSONResponse(status_code=200 if report.ready else 503, content=body)