from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class HazardType(str, Enum):
    FLOOD = "flood"
    LANDSLIDE = "landslide"
    EARTHQUAKE = "earthquake"
    OUTBREAK = "outbreak"

class SecurityLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class GeoLocation(BaseModel):
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)

class Alert(BaseModel):
    id: str
    hazard_type: HazardType
    security_level: SecurityLevel
    location: GeoLocation
    description: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    affected_areas: Optional[List[GeoLocation]] = None

