from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class TelemetryData(BaseModel):
    """Minimal shared schema used by the ingestion pipeline.

    The project originally expected a separate ``prahari_schemas`` package, but it is
    not present in this workspace. Providing a local compatibility model keeps the
    validators/imports working without changing the calling code.
    """

    model_config = ConfigDict(extra="allow")

    sensor_type: str = ""
    station_id: str = ""
    value: float | int | None = None
    timestamp: datetime | str | None = None
    quality_flags: list[str] = Field(default_factory=list)
    lat: float | None = None
    lon: float | None = None
    elevation_m: float | None = None

    def model_copy(self, *, update: dict[str, Any] | None = None, **kwargs: Any) -> "TelemetryData":
        """Compatibility shim for callers that expect pydantic v2-style copying."""
        data = self.model_dump()
        if update:
            data.update(update)
        return type(self).model_validate(data)


__all__ = ["TelemetryData"]
