from datetime import datetime
from typing import Any, Literal, Optional, Protocol, Union
from pydantic import BaseModel, Field


# --------------------------------------------------------------------------- #
# Models
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
    """What the API asks of the inference runtime (built by main.py from the registry and models)."""

    def list_models(self) -> list: ...                   # list[ModelInfo], cheap and non-blocking
    def get_model(self, name: str) -> Optional[LoadedModel]: ...   # None unless the model is loaded
    def is_ready(self) -> bool: ...
    def nats_connected(self) -> bool: ...

    async def reload_model(self, name: str) -> ModelInfo:
        """Load the newest artifact for `name` and swap it in. Contract: raise KeyError for an unknown
        name, and if anything fails (or the call is cancelled by a timeout) KEEP SERVING the old
        version, so a bad reload can never take a working model offline."""


# --------------------------------------------------------------------------- #
# Predictions
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