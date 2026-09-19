"""
OXYS Real-Time Data Ingestion Abstraction
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import uuid
import hashlib
import json
from datetime import datetime, timezone


class NormalizedEvent:
    def __init__(
        self,
        source: str,
        payload: Dict[str, Any],
        event_timestamp: Optional[str] = None,
        schema_version: str = "v1.0.0",
        event_id: Optional[str] = None,
        ingestion_timestamp: Optional[str] = None
    ):
        self.source = source
        self.schema_version = schema_version
        self.payload = payload
        self.event_timestamp = event_timestamp or datetime.now(timezone.utc).isoformat()
        self.ingestion_timestamp = ingestion_timestamp or datetime.now(timezone.utc).isoformat()
        
        if event_id:
            self.event_id = event_id
        else:
            # Deterministic hash generation from source + event_timestamp + sorted payload keys
            serialized_core = f"{source}|{self.event_timestamp}|{json.dumps(payload, sort_keys=True)}"
            self.event_id = hashlib.sha256(serialized_core.encode("utf-8")).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source": self.source,
            "event_timestamp": self.event_timestamp,
            "ingestion_timestamp": self.ingestion_timestamp,
            "schema_version": self.schema_version,
            "payload": self.payload
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NormalizedEvent":
        return cls(
            source=data.get("source", "unknown"),
            payload=data.get("payload", {}),
            event_timestamp=data.get("event_timestamp"),
            schema_version=data.get("schema_version", "v1.0.0"),
            event_id=data.get("event_id"),
            ingestion_timestamp=data.get("ingestion_timestamp")
        )


class DataSource(ABC):
    """
    Abstract base class for all real streaming data sources in OXYS.
    """
    def __init__(self, name: str, source_url: Optional[str] = None):
        self.name = name
        self.source_url = source_url

    @abstractmethod
    def fetch(self) -> List[Dict[str, Any]]:
        """
        Poll or receive raw data records from external real data source.
        """
        pass

    @abstractmethod
    def normalize(self, raw_record: Dict[str, Any]) -> NormalizedEvent:
        """
        Normalize raw external record into standard OXYS event envelope.
        """
        pass

    def fetch_and_normalize(self) -> List[NormalizedEvent]:
        raw_items = self.fetch()
        events = []
        for raw in raw_items:
            try:
                norm = self.normalize(raw)
                events.append(norm)
            except Exception as e:
                # Flag as quarantine/error event envelope
                err_event = NormalizedEvent(
                    source=self.name,
                    payload={"_raw_malformed": str(raw), "_normalization_error": str(e)},
                    schema_version="v0.0.0-error"
                )
                events.append(err_event)
        return events
