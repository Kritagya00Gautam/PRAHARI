import json
import nats
from prahari_schemas import TelemetryData

class IngestionStreamPublisher:
    def __init__(self, nats_url: str):
        self.nats_url = nats_url
        self.nc = None

    async def connect(self):
        self.nc = await nats.connect(self.nats_url)

    async def publish_telemetry(self, telemetry: TelemetryData):
        subject = f"telemetry.{telemetry.sensor_type}.{telemetry.station_id}"
        payload = telemetry.model_dump_json().encode("utf-8")
        await self.nc.publish(subject, payload)