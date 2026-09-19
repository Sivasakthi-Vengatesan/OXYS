"""
OXYS Kafka Ingestion Producer & Dispatcher
Publishes normalized events to Kafka topics:
  - oxys.raw
  - oxys.normalized
  - oxys.quarantine
"""
import os
import json
import logging
import time
from typing import Optional, List, Dict, Any
from ingestion.base import NormalizedEvent

logger = logging.getLogger("oxys.ingestion.producer")


class OxysKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        raw_topic: str = "oxys.raw",
        normalized_topic: str = "oxys.normalized",
        quarantine_topic: str = "oxys.quarantine"
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.raw_topic = raw_topic
        self.normalized_topic = normalized_topic
        self.quarantine_topic = quarantine_topic
        self._producer = None
        self._connected = False
        self._in_memory_buffer: List[Dict[str, Any]] = []

    def connect(self) -> bool:
        if bool(os.getenv("VERCEL") or os.getenv("AWS_LAMBDA_FUNCTION_NAME") or os.getenv("SERVERLESS")):
            self._connected = False
            return False
        try:
            import socket
            host, port = self.bootstrap_servers.split(",")[0].split(":")
            with socket.create_connection((host, int(port)), timeout=0.2):
                pass
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=1,
                request_timeout_ms=1000
            )
            self._connected = True
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.info(f"Kafka broker at {self.bootstrap_servers} not active. Operating in resilient direct memory mode.")
            self._connected = False
            return False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def publish_normalized(self, event: NormalizedEvent) -> Dict[str, Any]:
        """
        Publishes a normalized event to oxys.normalized.
        If malformed (schema error), routes to oxys.quarantine.
        """
        payload = event.to_dict()
        key = event.event_id

        # Malformed check
        if event.schema_version.endswith("-error") or "_raw_malformed" in event.payload:
            return self.publish_quarantine(
                event=event,
                reason="MALFORMED_INGESTION_PAYLOAD",
                details=str(event.payload.get("_normalization_error", "Schema error"))
            )

        if self._connected and self._producer:
            try:
                future = self._producer.send(self.normalized_topic, key=key, value=payload)
                self._producer.flush(timeout=2)
                record_meta = future.get(timeout=2)
                logger.info(f"[PRODUCER -> KAFKA] event_id={event.event_id} topic={self.normalized_topic} partition={record_meta.partition} offset={record_meta.offset}")
                return {
                    "status": "SENT_KAFKA",
                    "topic": self.normalized_topic,
                    "partition": record_meta.partition,
                    "offset": record_meta.offset,
                    "event_id": event.event_id
                }
            except Exception as e:
                logger.error(f"Kafka send failed: {e}. Storing in memory queue.")
                self._connected = False

        # Resilient in-memory fallback
        self._in_memory_buffer.append(payload)
        if len(self._in_memory_buffer) > 1000:
            self._in_memory_buffer.pop(0)
        logger.info(f"[PRODUCER -> DIRECT] event_id={event.event_id} source={event.source}")
        return {
            "status": "QUEUED_MEMORY",
            "topic": self.normalized_topic,
            "event_id": event.event_id
        }

    def publish_quarantine(self, event: NormalizedEvent, reason: str, details: str) -> Dict[str, Any]:
        """
        Publishes an anomaly or malformed payload to oxys.quarantine topic.
        """
        quarantine_payload = event.to_dict()
        quarantine_payload["_quarantine_meta"] = {
            "reason": reason,
            "details": details,
            "quarantined_at": time.time()
        }
        key = event.event_id

        if self._connected and self._producer:
            try:
                future = self._producer.send(self.quarantine_topic, key=key, value=quarantine_payload)
                self._producer.flush(timeout=2)
                record_meta = future.get(timeout=2)
                logger.warning(f"[PRODUCER -> QUARANTINE_KAFKA] event_id={event.event_id} reason={reason}")
                return {
                    "status": "QUARANTINED_KAFKA",
                    "topic": self.quarantine_topic,
                    "partition": record_meta.partition,
                    "offset": record_meta.offset,
                    "event_id": event.event_id
                }
            except Exception as e:
                logger.error(f"Kafka quarantine send failed: {e}")
                self._connected = False

        self._in_memory_buffer.append(quarantine_payload)
        return {
            "status": "QUARANTINED_MEMORY",
            "topic": self.quarantine_topic,
            "event_id": event.event_id
        }

    def get_buffered_events(self) -> List[Dict[str, Any]]:
        return list(self._in_memory_buffer)

    def close(self):
        if self._producer:
            try:
                self._producer.close(timeout=2)
            except Exception:
                pass
            self._connected = False
