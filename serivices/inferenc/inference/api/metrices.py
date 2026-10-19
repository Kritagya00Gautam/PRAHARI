
from typing import Tuple

_PROMETHEUS_IMPORT_ERROR = None

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

    METRICS_AVAILABLE = True
    _HTTP_REQUESTS = Counter("inference_http_requests_total", "HTTP requests",
                             ["method", "route", "status"])
    _PREDICT_SECONDS = Histogram(
        "inference_predict_seconds", "Prediction latency in seconds", ["model"],
        buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
    _PREDICT_FAILURES = Counter("inference_predict_failures_total", "Failed predictions",
                                ["model", "kind"])
except ImportError as exc:  # pragma: no cover
    METRICS_AVAILABLE = False
    _PROMETHEUS_IMPORT_ERROR = exc


def count_http(method: str, route: str, status: int) -> None:
    if METRICS_AVAILABLE:
        _HTTP_REQUESTS.labels(method, route, str(status)).inc()


def observe_prediction(model: str, seconds: float) -> None:
    if METRICS_AVAILABLE:
        _PREDICT_SECONDS.labels(model=model).observe(seconds)


def count_prediction_failure(model: str, kind: str) -> None:
    if METRICS_AVAILABLE:
        _PREDICT_FAILURES.labels(model=model, kind=kind).inc()


def render_metrics() -> Tuple[bytes, str]:
    """(body, content type) for the /metrics endpoint."""
    if not METRICS_AVAILABLE:
        raise RuntimeError(
            "Prometheus metrics are unavailable; install prometheus-client"
        ) from _PROMETHEUS_IMPORT_ERROR
    return generate_latest(), CONTENT_TYPE_LATEST