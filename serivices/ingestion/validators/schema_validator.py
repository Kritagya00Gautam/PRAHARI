from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Any, Generic, Iterable, Optional, Type, TypeVar
from prahari_schemas import TelemetryData  # packages/schemas
from pydantic import BaseModel, ValidationError

log = logging.getLogger("prahari.ingestion.schema_validator")
T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class FieldError:
    """One failed field. Deliberately excludes the offending input value."""
    path: str
    message: str
    type: str


@dataclass(frozen=True)
class ValidationOutcome(Generic[T]):
    data: Optional[T] = None
    errors: list[FieldError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.data is not None


@dataclass(frozen=True)
class Reject:
    """A rejected payload, ready to write to a dead-letter store."""
    source: str
    raw: Any
    errors: list[FieldError]


def validate_payload(
    raw_payload: Any,
    model: Type[T] = TelemetryData,  # type: ignore[assignment]
    source: str = "unknown",
) -> ValidationOutcome[T]:
    """Validate one raw payload against a pydantic model."""
    if not isinstance(raw_payload, dict):
        err = FieldError("<root>", f"expected object, got {type(raw_payload).__name__}", "type_error")
        log.warning("rejected payload from %s: %s", source, err.message)
        return ValidationOutcome(errors=[err])

    try:
        return ValidationOutcome(data=model.model_validate(raw_payload))
    except ValidationError as exc:
        errors = [
            FieldError(
                path=".".join(str(p) for p in e["loc"]) or "<root>",
                message=e["msg"],
                type=e["type"],
            )
            for e in exc.errors(include_url=False, include_input=False)
        ]
        log.warning(
            "rejected payload from %s: %d error(s): %s",
            source, len(errors), "; ".join(f"{e.path}: {e.message}" for e in errors[:5]),
        )
        return ValidationOutcome(errors=errors)


def validate_batch(
    payloads: Iterable[Any],
    model: Type[T] = TelemetryData,  # type: ignore[assignment]
    source: str = "unknown",
) -> tuple[list[T], list[Reject]]:
    """Split a batch into valid records and rejects (for the dead-letter store)."""
    valid: list[T] = []
    rejects: list[Reject] = []
    for raw in payloads:
        outcome = validate_payload(raw, model, source)
        if outcome.ok:
            valid.append(outcome.data)  # type: ignore[arg-type]
        else:
            rejects.append(Reject(source=source, raw=raw, errors=outcome.errors))
    return valid, rejects