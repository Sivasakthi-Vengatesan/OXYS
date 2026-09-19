"""
OXYS Kinetic Circuit Breaker & Finite-State Machine
FSM States:
  CLOSED -> BREACH DETECTED -> OPEN -> QUARANTINE -> HALF-OPEN -> RECOVERED -> CLOSED

Decisions:
  - ALLOW: Nominal payload passes to downstream sinks
  - QUARANTINE: Anomaly detected above threshold; isolated to MinIO & quarantine tables
  - BLOCK: Severe corruption / duplicate replay; rejected immediately
"""
import os
import time
from typing import Dict, Any, Optional, Tuple, List
from engine.detectors import DetectorResult


class DecisionType:
    ALLOW = "ALLOW"
    QUARANTINE = "QUARANTINE"
    BLOCK = "BLOCK"


class CircuitBreakerFSM:
    def __init__(
        self,
        null_rate_threshold: float = 15.00,
        cardinality_min_threshold: float = 0.40,
        z_score_threshold: float = 3.50,
        auto_reset_seconds: float = 60.0
    ):
        # Configurable thresholds
        self.null_rate_threshold = float(os.getenv("NULL_RATE_THRESHOLD", null_rate_threshold))
        self.cardinality_min_threshold = float(os.getenv("CARDINALITY_MIN_THRESHOLD", cardinality_min_threshold))
        self.z_score_threshold = float(os.getenv("DISTRIBUTION_Z_THRESHOLD", z_score_threshold))
        self.auto_reset_seconds = float(os.getenv("CIRCUIT_AUTO_RESET_SEC", auto_reset_seconds))

        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN, QUARANTINE, RECOVERED
        self.active_stream = "crypto_market_stream"
        self.last_breach_reason = "NONE"
        self.last_breach_metric = "0.00%"
        self.last_trip_time: Optional[float] = None
        self.canary_count = 0
        self.last_valid_checkpoint = "cp_00482"

    def evaluate_decisions(self, detector_results: List[DetectorResult], stream_name: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Evaluates detector breaches and returns (decision, breach_reason, severity).
        Decision is one of ALLOW, QUARANTINE, BLOCK.
        """
        self.active_stream = stream_name
        breaches = [r for r in detector_results if r.is_breached]

        # Check auto-reset window if OPEN
        if self.state == "OPEN" and self.last_trip_time:
            if time.time() - self.last_trip_time > self.auto_reset_seconds:
                self.state = "HALF-OPEN"
                self.canary_count = 0

        if not breaches:
            if self.state == "HALF-OPEN":
                self.canary_count += 1
                if self.canary_count >= 3:
                    self.state = "RECOVERED"
                    time.sleep(0.1)
                    self.state = "CLOSED"
                    self.last_breach_reason = "NONE"
            return (DecisionType.ALLOW, None, "INFO")

        # Sort breaches by severity
        critical_breaches = [b for b in breaches if b.severity == "CRITICAL"]
        warning_breaches = [b for b in breaches if b.severity == "WARNING"]

        primary_breach = critical_breaches[0] if critical_breaches else warning_breaches[0]
        self.last_breach_reason = primary_breach.reason or primary_breach.name
        self.last_breach_metric = primary_breach.formatted_metric

        if critical_breaches:
            # Trip circuit breaker to OPEN
            self.state = "OPEN"
            self.last_trip_time = time.time()
            
            # If integrity duplicate, BLOCK; if structural/null, QUARANTINE
            if primary_breach.detector_id == "integrity":
                return (DecisionType.BLOCK, primary_breach.reason, "CRITICAL")
            else:
                return (DecisionType.QUARANTINE, primary_breach.reason, "CRITICAL")
        else:
            # Warning level anomaly
            return (DecisionType.QUARANTINE, primary_breach.reason, "WARNING")

    def trip(self, reason: str = "MANUAL_OVERRIDE", metric: str = "MANUAL"):
        self.state = "OPEN"
        self.last_breach_reason = reason
        self.last_breach_metric = metric
        self.last_trip_time = time.time()

    def reset(self):
        self.state = "CLOSED"
        self.last_breach_reason = "NONE"
        self.last_trip_time = None
        self.canary_count = 0

    def half_open(self):
        self.state = "HALF-OPEN"
        self.canary_count = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "state": self.state,
            "stream": self.active_stream,
            "batch_id": f"#{int(time.time() % 10000):05d}",
            "reason": self.last_breach_reason,
            "current_metric": self.last_breach_metric,
            "threshold": f"{self.null_rate_threshold:.2f}%",
            "checkpoint": self.last_valid_checkpoint
        }
