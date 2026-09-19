"""
OXYS Real Data Source - USGS Real-Time Earthquake GeoJSON Feed
Official Source: https://earthquake.usgs.gov/earthquakes/feed/
OXYS remains completely domain-agnostic; USGS is a real-world stream.
"""
import os
import time
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from ingestion.base import DataSource, NormalizedEvent

logger = logging.getLogger("oxys.ingestion.usgs")


class USGSEarthquakeSource(DataSource):
    """
    Ingests live global seismic telemetry from the USGS Real-Time GeoJSON Feed.
    Extracts numerical and categorical properties:
      - magnitude: float
      - place: str
      - longitude: float
      - latitude: float
      - depth: float
      - mag_type: str
      - status: str
      - tsunami: int
      - significance: int
    """
    DEFAULT_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    FALLBACK_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    def __init__(
        self,
        feed_url: Optional[str] = None,
        poll_interval_seconds: Optional[int] = None
    ):
        configured_url = os.getenv("USGS_FEED_URL", feed_url or self.DEFAULT_FEED_URL)
        super().__init__(name="usgs", source_url=configured_url)
        
        self.poll_interval_seconds = int(os.getenv("USGS_POLL_INTERVAL_SECONDS", poll_interval_seconds or 60))
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "OXYS-Data-Integrity-Engine/2.0"})
        
        # State-aware deduplication cache: feature_id -> updated_timestamp_ms
        self._seen_events: Dict[str, int] = {}
        self._max_cache_size = 5000

    def fetch(self) -> List[Dict[str, Any]]:
        """
        Polls the USGS GeoJSON feed, parses features, and filters out already-seen
        unchanged events while recognizing updated earthquake records.
        """
        new_or_updated_features: List[Dict[str, Any]] = []
        target_urls = [self.source_url, self.FALLBACK_FEED_URL]

        for url in target_urls:
            try:
                resp = self.session.get(url, timeout=6)
                if resp.status_code == 200:
                    data = resp.json()
                    features = data.get("features", [])
                    
                    for f in features:
                        feature_id = str(f.get("id", "")).strip()
                        if not feature_id:
                            continue
                        
                        props = f.get("properties", {}) or {}
                        updated_ts = int(props.get("updated") or props.get("time") or 0)
                        
                        # Deduplication check: only emit if new or updated
                        last_seen_updated = self._seen_events.get(feature_id)
                        if last_seen_updated is None or updated_ts > last_seen_updated:
                            self._seen_events[feature_id] = updated_ts
                            new_or_updated_features.append(f)
                    
                    # Prevent unbounded memory growth in deduplication cache
                    if len(self._seen_events) > self._max_cache_size:
                        # Keep newest 2500 entries
                        sorted_items = sorted(self._seen_events.items(), key=lambda x: x[1], reverse=True)
                        self._seen_events = dict(sorted_items[:2500])

                    if features:
                        break
            except Exception as e:
                logger.warning(f"Error fetching USGS feed from {url}: {e}")
                continue

        return new_or_updated_features

    def normalize(self, raw_feature: Dict[str, Any]) -> NormalizedEvent:
        """
        Normalizes raw USGS GeoJSON Feature into the domain-agnostic OXYS event envelope.
        """
        feature_id = str(raw_feature.get("id", f"usgs_{int(time.time()*1000)}")).strip()
        props = raw_feature.get("properties", {}) or {}
        geom = raw_feature.get("geometry", {}) or {}
        coords = geom.get("coordinates", [None, None, None])

        # Parse numeric magnitude safely
        raw_mag = props.get("mag")
        magnitude: Optional[float] = None
        if raw_mag is not None:
            try:
                magnitude = float(raw_mag)
            except (ValueError, TypeError):
                magnitude = None

        # Coordinates parsing: [longitude, latitude, depth]
        longitude: Optional[float] = None
        latitude: Optional[float] = None
        depth: Optional[float] = None

        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            try:
                longitude = float(coords[0]) if coords[0] is not None else None
            except (ValueError, TypeError):
                longitude = None

            try:
                latitude = float(coords[1]) if coords[1] is not None else None
            except (ValueError, TypeError):
                latitude = None

            if len(coords) >= 3:
                try:
                    depth = float(coords[2]) if coords[2] is not None else None
                except (ValueError, TypeError):
                    depth = None

        # Categorical and status metadata
        place = props.get("place")
        mag_type = props.get("magType")
        status = props.get("status")
        
        try:
            tsunami = int(props.get("tsunami", 0)) if props.get("tsunami") is not None else 0
        except (ValueError, TypeError):
            tsunami = 0

        try:
            significance = int(props.get("sig", 0)) if props.get("sig") is not None else 0
        except (ValueError, TypeError):
            significance = 0

        # Event timestamp
        time_ms = props.get("time")
        if time_ms:
            try:
                event_ts = datetime.fromtimestamp(time_ms / 1000.0, tz=timezone.utc).isoformat()
            except Exception:
                event_ts = datetime.now(timezone.utc).isoformat()
        else:
            event_ts = datetime.now(timezone.utc).isoformat()

        payload = {
            "magnitude": magnitude,
            "place": place,
            "longitude": longitude,
            "latitude": latitude,
            "depth": depth,
            "mag_type": str(mag_type) if mag_type else None,
            "status": str(status) if status else None,
            "tsunami": tsunami,
            "significance": significance
        }

        return NormalizedEvent(
            source="usgs",
            payload=payload,
            event_timestamp=event_ts,
            schema_version="v1.0.0",
            event_id=f"usgs_{feature_id}"
        )
