import httpx
from typing import AsyncGenerator, Dict, Any
from connectors.base import BaseConnector

class DHMGaugeConnector(BaseConnector):
    def __init__(self, endpoint_url: str, api_key: str):
        self.endpoint_url = endpoint_url
        self.headers = {"Authorization": f"Bearer {api_key}"}

    async def fetch_latest(self) -> AsyncGenerator[Dict[str, Any], None]:
        async with httpx.AsyncClient() as client:
            response = await client.get(self.endpoint_url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            for item in data.get("stations", []):
                yield {
                    "station_id": item["id"],
                    "water_level_m": item["current_level"],
                    "discharge_m3s": item.get("discharge"),
                    "latitude": item["lat"],
                    "longitude": item["lon"],
                    "recorded_at": item["timestamp"],
                }