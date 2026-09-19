"""
OXYS Real Data Source - Live Global Atmospheric & IoT Telemetry
Uses Open-Meteo Public Weather API (No Auth / API key required).
"""
import requests
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from ingestion.base import DataSource, NormalizedEvent


class WeatherTelemetrySource(DataSource):
    """
    Ingests live planetary atmospheric telemetry across distributed station coordinates.
    Fields:
      - station_id: categorical
      - latitude: float
      - longitude: float
      - temperature_c: float
      - relative_humidity: float
      - surface_pressure_hpa: float
      - wind_speed_kmh: float
      - weather_code: int
    """
    DEFAULT_STATIONS = [
        {"id": "STATION-LON-01", "city": "London", "lat": 51.5074, "lon": -0.1278},
        {"id": "STATION-NYC-02", "city": "New York", "lat": 40.7128, "lon": -74.0060},
        {"id": "STATION-TYO-03", "city": "Tokyo", "lat": 35.6762, "lon": 139.6503},
        {"id": "STATION-SIN-04", "city": "Singapore", "lat": 1.3521, "lon": 103.8198},
        {"id": "STATION-SYD-05", "city": "Sydney", "lat": -33.8688, "lon": 151.2093},
    ]

    def __init__(self, stations: Optional[List[Dict[str, Any]]] = None):
        super().__init__(
            name="open_meteo_telemetry_stream",
            source_url="https://api.open-meteo.com/v1/forecast"
        )
        self.stations = stations or self.DEFAULT_STATIONS
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "OXYS-Ingestion-Engine/1.0"})

    def fetch(self) -> List[Dict[str, Any]]:
        records = []
        for station in self.stations:
            try:
                params = {
                    "latitude": station["lat"],
                    "longitude": station["lon"],
                    "current": "temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m,weather_code"
                }
                resp = self.session.get(self.source_url, params=params, timeout=4)
                if resp.status_code == 200:
                    data = resp.json()
                    current = data.get("current", {})
                    records.append({
                        "station_id": station["id"],
                        "city": station["city"],
                        "latitude": station["lat"],
                        "longitude": station["lon"],
                        "temperature_2m": current.get("temperature_2m"),
                        "relative_humidity_2m": current.get("relative_humidity_2m"),
                        "surface_pressure": current.get("surface_pressure"),
                        "wind_speed_10m": current.get("wind_speed_10m"),
                        "weather_code": current.get("weather_code"),
                        "time": current.get("time")
                    })
            except Exception:
                continue
        return records

    def normalize(self, raw: Dict[str, Any]) -> NormalizedEvent:
        station_id = raw.get("station_id", "STATION-UNKNOWN")
        
        try:
            temp = float(raw.get("temperature_2m")) if raw.get("temperature_2m") is not None else None
        except (ValueError, TypeError):
            temp = None

        try:
            humidity = float(raw.get("relative_humidity_2m")) if raw.get("relative_humidity_2m") is not None else None
        except (ValueError, TypeError):
            humidity = None

        try:
            pressure = float(raw.get("surface_pressure")) if raw.get("surface_pressure") is not None else None
        except (ValueError, TypeError):
            pressure = None

        try:
            wind = float(raw.get("wind_speed_10m")) if raw.get("wind_speed_10m") is not None else None
        except (ValueError, TypeError):
            wind = None

        try:
            wcode = int(raw.get("weather_code", 0)) if raw.get("weather_code") is not None else None
        except (ValueError, TypeError):
            wcode = None

        time_str = raw.get("time")
        if time_str:
            try:
                event_ts = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc).isoformat()
            except Exception:
                event_ts = datetime.now(timezone.utc).isoformat()
        else:
            event_ts = datetime.now(timezone.utc).isoformat()

        payload = {
            "station_id": station_id,
            "city": raw.get("city", "Unknown"),
            "latitude": float(raw.get("latitude", 0.0)),
            "longitude": float(raw.get("longitude", 0.0)),
            "temperature_c": temp,
            "relative_humidity": humidity,
            "surface_pressure_hpa": pressure,
            "wind_speed_kmh": wind,
            "weather_code": wcode
        }

        event_id = f"weather_{station_id}_{int(time.time()*1000)}"

        return NormalizedEvent(
            source="open_meteo_telemetry_stream",
            payload=payload,
            event_timestamp=event_ts,
            schema_version="v1.0.0",
            event_id=event_id
        )
