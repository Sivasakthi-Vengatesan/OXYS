"""
OXYS Real-Time Detection Engine
Implements 5 core streaming integrity & anomaly detectors:
  1. Schema Drift Guard
  2. Null-Rate Drift Guard
  3. Cardinality / Entropy Guard
  4. Distribution / Z-Score Guard
  5. Duplicate / Integrity Guard
"""
import math
import time
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import deque, Counter
import numpy as np


class DetectorResult:
    def __init__(
        self,
        detector_id: str,
        name: str,
        is_breached: bool,
        current_value: float,
        formatted_metric: str,
        threshold_limit: str,
        severity: str = "INFO",  # INFO, WARNING, CRITICAL
        reason: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.detector_id = detector_id
        self.name = name
        self.is_breached = is_breached
        self.current_value = current_value
        self.formatted_metric = formatted_metric
        self.threshold_limit = threshold_limit
        self.severity = severity
        self.reason = reason
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "detector_id": self.detector_id,
            "name": self.name,
            "is_breached": self.is_breached,
            "current_value": self.current_value,
            "metric": self.formatted_metric,
            "limit": self.threshold_limit,
            "severity": self.severity,
            "reason": self.reason,
            "details": self.details
        }


class SchemaDriftDetector:
    """
    Guards structural integrity: enforces expected field types, detects unexpected
    field additions or type mutations against the schema baseline.
    """
    DEFAULT_SCHEMAS: Dict[str, Dict[str, Any]] = {
        "usgs": {
            "magnitude": (int, float),
            "place": str,
            "longitude": (int, float),
            "latitude": (int, float),
            "depth": (int, float),
            "mag_type": str,
            "status": str,
            "tsunami": (int,),
            "significance": (int,)
        },
        "crypto": {
            "symbol": str,
            "last_price": (int, float),
            "volume": (int, float),
            "price_change_percent": (int, float),
            "trade_count": (int, float)
        },
        "weather": {
            "station_id": str,
            "latitude": (int, float),
            "longitude": (int, float),
            "temperature_c": (int, float),
            "relative_humidity_pct": (int, float),
            "wind_speed_kmh": (int, float)
        }
    }

    def __init__(self, expected_schema: Optional[Dict[str, type]] = None):
        self.registered_schemas: Dict[str, Dict[str, Any]] = dict(self.DEFAULT_SCHEMAS)
        if expected_schema:
            self.registered_schemas["custom"] = expected_schema
        self.breach_count = 0

    def register_baseline_schema(self, source: str, schema: Dict[str, Any]):
        """Registers or locks a schema baseline for a given stream source."""
        self.registered_schemas[source] = schema

    def _resolve_schema_for_payload(self, payload: Dict[str, Any], source: Optional[str] = None) -> Dict[str, Any]:
        if source and source in self.registered_schemas:
            return self.registered_schemas[source]
        # Match by key signatures if source not directly matched
        if "magnitude" in payload or "mag_type" in payload or "depth" in payload:
            return self.registered_schemas["usgs"]
        if "symbol" in payload or "last_price" in payload:
            return self.registered_schemas["crypto"]
        if "station_id" in payload or "temperature_c" in payload:
            return self.registered_schemas["weather"]
        return self.registered_schemas.get("custom", self.registered_schemas["crypto"])

    def evaluate(self, payload: Dict[str, Any], source: Optional[str] = None) -> DetectorResult:
        drift_errors = []
        schema = self._resolve_schema_for_payload(payload, source)
        
        for field, expected_type in schema.items():
            if field in payload and payload[field] is not None:
                val = payload[field]
                if not isinstance(val, expected_type):
                    drift_errors.append(f"Type mismatch on '{field}': expected {expected_type}, got {type(val).__name__}")
        
        # Check for unexpected fields with corrupted signatures
        if "_raw_malformed" in payload or "_normalization_error" in payload:
            drift_errors.append("Malformed raw record signature")

        is_breached = len(drift_errors) > 0
        if is_breached:
            self.breach_count += 1
            severity = "CRITICAL"
            reason = f"SCHEMA_DRIFT: {'; '.join(drift_errors)}"
            metric = f"{len(drift_errors)} drift(s)"
        else:
            severity = "INFO"
            reason = None
            metric = "0 drift"

        return DetectorResult(
            detector_id="schema",
            name="SCHEMA INTEGRITY",
            is_breached=is_breached,
            current_value=float(len(drift_errors)),
            formatted_metric=metric,
            threshold_limit="0 allowed",
            severity=severity,
            reason=reason,
            details={"errors": drift_errors}
        )


class NullRateDetector:
    """
    Guards quality: monitors rolling window of field null frequencies against
    spike threshold (e.g. 15.00%).
    """
    def __init__(self, window_size: int = 100, threshold_pct: float = 15.00):
        self.window_size = window_size
        self.threshold_pct = threshold_pct
        self.history: deque = deque(maxlen=window_size)
        self.breach_count = 0

    def evaluate(self, payload: Dict[str, Any]) -> DetectorResult:
        if not payload:
            null_count = 1
            total_fields = 1
        else:
            total_fields = len(payload)
            null_count = sum(1 for v in payload.values() if v is None)
        
        event_null_rate = (null_count / max(1, total_fields)) * 100.0
        self.history.append(event_null_rate)
        
        # Rolling average null rate
        rolling_null_rate = sum(self.history) / len(self.history)
        is_breached = rolling_null_rate > self.threshold_pct

        if is_breached:
            self.breach_count += 1
            severity = "CRITICAL"
            reason = f"NULL_RATE_BREACH: Rolling null rate {rolling_null_rate:.2f}% exceeds threshold {self.threshold_pct:.2f}%"
        else:
            severity = "INFO"
            reason = None

        return DetectorResult(
            detector_id="null_rate",
            name="NULL RATE",
            is_breached=is_breached,
            current_value=round(rolling_null_rate, 2),
            formatted_metric=f"{rolling_null_rate:.2f}%",
            threshold_limit=f"{self.threshold_pct:.2f}%",
            severity=severity,
            reason=reason,
            details={"null_fields_sample": [k for k, v in payload.items() if v is None]}
        )


class CardinalityDetector:
    """
    Guards entropy: monitors Shannon entropy and distinct key diversity on primary partition keys.
    """
    def __init__(self, window_size: int = 50, min_entropy_threshold: float = 0.40, key_field: str = "symbol"):
        self.window_size = window_size
        self.min_entropy_threshold = min_entropy_threshold
        self.key_field = key_field
        self.history: deque = deque(maxlen=window_size)
        self.breach_count = 0

    def evaluate(self, payload: Dict[str, Any]) -> DetectorResult:
        key_val = str(
            payload.get(self.key_field) or
            payload.get("place") or
            payload.get("mag_type") or
            payload.get("symbol") or
            payload.get("station_id") or
            "default"
        )
        self.history.append(key_val)

        # Calculate Shannon entropy: - sum(p * log2(p))
        counts = Counter(self.history)
        total = len(self.history)
        entropy = 0.0
        for count in counts.values():
            p = count / total
            if p > 0:
                entropy -= p * math.log2(p)

        # Normalize entropy between 0 and 1 relative to max possible diversity in window
        max_possible = math.log2(max(1, len(counts)))
        normalized_entropy = (entropy / max_possible) if max_possible > 0 else 1.0
        
        # In small initial windows, ensure healthy baseline
        if total < 5:
            normalized_entropy = 0.88

        is_breached = normalized_entropy < self.min_entropy_threshold
        if is_breached:
            self.breach_count += 1
            severity = "WARNING"
            reason = f"CARDINALITY_COLLAPSE: Key entropy {normalized_entropy:.2f} dropped below limit {self.min_entropy_threshold:.2f}"
        else:
            severity = "INFO"
            reason = None

        return DetectorResult(
            detector_id="cardinality",
            name="CARDINALITY",
            is_breached=is_breached,
            current_value=round(normalized_entropy, 2),
            formatted_metric=f"{normalized_entropy:.2f} entropy",
            threshold_limit=f"> {self.min_entropy_threshold:.2f}",
            severity=severity,
            reason=reason,
            details={"distinct_keys_in_window": len(counts), "window_size": total}
        )


class DistributionDetector:
    """
    Guards statistical distribution: monitors rolling Z-score & standard deviations on numerical metrics.
    """
    def __init__(self, window_size: int = 100, z_score_threshold: float = 3.50, num_field: str = "last_price"):
        self.window_size = window_size
        self.z_score_threshold = z_score_threshold
        self.num_field = num_field
        self.history: deque = deque(maxlen=window_size)
        self.breach_count = 0

    def evaluate(self, payload: Dict[str, Any]) -> DetectorResult:
        val = payload.get(self.num_field)
        if val is None or not isinstance(val, (int, float)):
            val = (
                payload.get("magnitude") if isinstance(payload.get("magnitude"), (int, float)) else
                payload.get("depth") if isinstance(payload.get("depth"), (int, float)) else
                payload.get("last_price") if isinstance(payload.get("last_price"), (int, float)) else
                payload.get("price_change_percent") if isinstance(payload.get("price_change_percent"), (int, float)) else
                payload.get("temperature_c") if isinstance(payload.get("temperature_c"), (int, float)) else
                None
            )

        if val is None or not isinstance(val, (int, float)):
            z_score = 0.0
        else:
            if len(self.history) >= 5:
                arr = np.array(self.history)
                mean = float(np.mean(arr))
                std = float(np.std(arr))
                if std > 1e-6:
                    z_score = abs((float(val) - mean) / std)
                else:
                    z_score = 0.0
            else:
                z_score = 1.12
            self.history.append(float(val))

        is_breached = z_score > self.z_score_threshold
        if is_breached:
            self.breach_count += 1
            severity = "WARNING"
            reason = f"DISTRIBUTION_OUTLIER: Z-score {z_score:.2f} exceeds statistical threshold {self.z_score_threshold:.2f}"
        else:
            severity = "INFO"
            reason = None

        return DetectorResult(
            detector_id="distribution",
            name="DISTRIBUTION",
            is_breached=is_breached,
            current_value=round(z_score, 2),
            formatted_metric=f"{z_score:.2f} Z-Score",
            threshold_limit=f"< {self.z_score_threshold:.2f}",
            severity=severity,
            reason=reason,
            details={"current_value": val}
        )


class DuplicateIntegrityDetector:
    """
    Guards stream integrity: tracks sliding deduplication window to detect replay storms or duplicate event_ids.
    """
    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.seen_ids: Set[str] = set()
        self.order: deque = deque()
        self.duplicate_count = 0

    def evaluate(self, event_id: str) -> DetectorResult:
        is_duplicate = event_id in self.seen_ids
        if is_duplicate:
            self.duplicate_count += 1
            severity = "CRITICAL"
            reason = f"DUPLICATE_EVENT_DETECTED: Replay storm or duplicate event ID {event_id}"
        else:
            self.seen_ids.add(event_id)
            self.order.append(event_id)
            if len(self.order) > self.window_size:
                oldest = self.order.popleft()
                self.seen_ids.discard(oldest)
            severity = "INFO"
            reason = None

        return DetectorResult(
            detector_id="integrity",
            name="EVENT INTEGRITY",
            is_breached=is_duplicate,
            current_value=float(self.duplicate_count),
            formatted_metric=f"{self.duplicate_count} dupes",
            threshold_limit="0 dupes",
            severity=severity,
            reason=reason,
            details={"event_id": event_id}
        )
