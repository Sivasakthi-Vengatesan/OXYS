"""
OXYS Failure Injection & Controlled Chaos Test Suite
Verifies pipeline defense mechanisms under realistic corruption conditions:
  1. Malformed Event Payload
  2. Null-Rate Spike
  3. Schema Drift & Field Type Mutation
  4. Duplicate Replay Storm
  5. Statistical Distribution Outlier
"""
import time
from ingestion.base import NormalizedEvent
from engine.processor import processor
from engine.circuit_breaker import DecisionType
from server.db import db


def test_failure_malformed_event_quarantine():
    """Verify malformed raw record is flagged and routed to quarantine."""
    malformed_event = NormalizedEvent(
        source="crypto_market_stream",
        payload={
            "_raw_malformed": "CORRUPTED_RAW_UNPARSABLE_BINARY_DATA",
            "_normalization_error": "JSONDecodeError"
        },
        schema_version="v0.0.0-error"
    )
    result = processor.process_event(malformed_event)
    assert result["decision"] == DecisionType.QUARANTINE
    assert processor.circuit_breaker.state == "OPEN"
    processor.heal_system()


def test_failure_null_rate_spike():
    """Verify sudden null rate spike triggers circuit trip and quarantine isolation."""
    processor.heal_system()
    
    # Inject 5 poisoned null payloads
    for i in range(5):
        null_spike_event = NormalizedEvent(
            source="crypto_market_stream",
            payload={
                "symbol": "SOLUSDT",
                "last_price": None,
                "volume": None,
                "price_change_percent": None,
                "trade_count": None
            },
            schema_version="v1.0.0"
        )
        res = processor.process_event(null_spike_event)
    
    assert res["decision"] == DecisionType.QUARANTINE
    assert processor.circuit_breaker.state == "OPEN"
    assert "NULL_RATE" in processor.circuit_breaker.last_breach_reason
    processor.heal_system()


def test_failure_schema_drift_type_mismatch():
    """Verify type mutation (string where float is expected) trips schema guard."""
    processor.heal_system()
    
    type_corrupt_event = NormalizedEvent(
        source="crypto_market_stream",
        payload={
            "symbol": "BTCUSDT",
            "last_price": "INVALID_PRICE_AS_STRING_INJECTION",
            "volume": 500.0,
            "price_change_percent": 2.1
        },
        schema_version="v1.0.0"
    )
    res = processor.process_event(type_corrupt_event)
    assert res["decision"] == DecisionType.QUARANTINE
    assert processor.circuit_breaker.state == "OPEN"
    assert "SCHEMA_DRIFT" in processor.circuit_breaker.last_breach_reason
    processor.heal_system()


def test_failure_duplicate_event_replayed():
    """Verify replayed event IDs are detected and BLOCKED to protect sink integrity."""
    processor.heal_system()
    fixed_id = f"test_duplicate_replay_{int(time.time()*1000)}"
    
    event1 = NormalizedEvent(
        source="crypto_market_stream",
        payload={"symbol": "ETHUSDT", "last_price": 3250.0, "volume": 150.0, "price_change_percent": -1.2, "trade_count": 800},
        schema_version="v1.0.0",
        event_id=fixed_id
    )
    # First pass: ALLOW
    res1 = processor.process_event(event1)
    assert res1["decision"] == DecisionType.ALLOW

    # Second pass: Replay detected -> BLOCK
    res2 = processor.process_event(event1)
    assert res2["decision"] == DecisionType.BLOCK


def test_failure_statistical_distribution_outlier():
    """Verify extreme value outlier triggers distribution anomaly detection."""
    processor.heal_system()
    
    # Establish baseline
    for p in [100.0, 101.5, 99.2, 100.8, 101.0, 99.8]:
        evt = NormalizedEvent(
            source="crypto_market_stream",
            payload={"symbol": "TEST", "last_price": p, "volume": 10.0, "price_change_percent": 0.1, "trade_count": 50}
        )
        processor.process_event(evt)
    
    # Extreme price surge outlier (100,000x baseline)
    outlier_evt = NormalizedEvent(
        source="crypto_market_stream",
        payload={"symbol": "TEST", "last_price": 999999.0, "volume": 10.0, "price_change_percent": 0.1, "trade_count": 50}
    )
    res = processor.process_event(outlier_evt)
    assert res["decision"] in [DecisionType.QUARANTINE, DecisionType.ALLOW]
    assert any(b["detector_id"] == "distribution" for b in res["breaches"])
    processor.heal_system()


# --- USGS REAL SOURCE ANOMALY & CHAOS TESTS ---

def test_usgs_clean_real_event_allowed():
    """Verify valid normalized USGS earthquake record passes all detectors and is ALLOWED."""
    processor.heal_system()
    clean_event = NormalizedEvent(
        source="usgs",
        payload={
            "magnitude": 3.42,
            "place": "15km ESE of Anza, CA",
            "longitude": -116.51,
            "latitude": 33.51,
            "depth": 11.2,
            "mag_type": "ml",
            "status": "reviewed",
            "tsunami": 0,
            "significance": 180
        },
        schema_version="v1.0.0",
        event_id=f"usgs_clean_test_{int(time.time()*1000)}"
    )
    res = processor.process_event(clean_event)
    assert res["decision"] == DecisionType.ALLOW
    assert res["circuit_state"] == "CLOSED"


def test_usgs_failure_schema_drift_type_mismatch():
    """Verify type mutation on USGS stream (magnitude: '4.2', depth: 'UNKNOWN') trips schema guard."""
    processor.heal_system()
    corrupt_usgs_event = NormalizedEvent(
        source="usgs",
        payload={
            "magnitude": "4.2",  # String instead of float
            "place": "Central Alaska",
            "longitude": -149.2,
            "latitude": 64.8,
            "depth": "UNKNOWN_DEPTH",  # String instead of float
            "mag_type": "ml",
            "status": "reviewed",
            "tsunami": 0,
            "significance": 100
        },
        schema_version="v1.0.0",
        event_id=f"usgs_drift_test_{int(time.time()*1000)}"
    )
    res = processor.process_event(corrupt_usgs_event)
    assert res["decision"] == DecisionType.QUARANTINE
    assert processor.circuit_breaker.state == "OPEN"
    assert "SCHEMA_DRIFT" in processor.circuit_breaker.last_breach_reason
    processor.heal_system()


def test_usgs_failure_null_rate_spike():
    """Verify sudden null rate spike across USGS stream triggers containment."""
    processor.heal_system()
    for _ in range(5):
        null_usgs_event = NormalizedEvent(
            source="usgs",
            payload={
                "magnitude": None,
                "place": None,
                "longitude": None,
                "latitude": None,
                "depth": None,
                "mag_type": None,
                "status": None,
                "tsunami": 0,
                "significance": 0
            },
            schema_version="v1.0.0",
            event_id=f"usgs_null_test_{int(time.time()*1000)}"
        )
        res = processor.process_event(null_usgs_event)
    
    assert res["decision"] == DecisionType.QUARANTINE
    assert processor.circuit_breaker.state == "OPEN"
    assert "NULL_RATE" in processor.circuit_breaker.last_breach_reason
    processor.heal_system()


def test_usgs_failure_duplicate_event_id():
    """Verify repeated USGS event ID triggers duplicate integrity detection and is BLOCKED."""
    processor.heal_system()
    fixed_usgs_id = f"usgs_ci40192837_{int(time.time())}"
    
    usgs_event = NormalizedEvent(
        source="usgs",
        payload={
            "magnitude": 2.1,
            "place": "Southern California",
            "longitude": -117.2,
            "latitude": 34.1,
            "depth": 5.4,
            "mag_type": "ml",
            "status": "reviewed",
            "tsunami": 0,
            "significance": 68
        },
        schema_version="v1.0.0",
        event_id=fixed_usgs_id
    )
    
    # 1. First event -> ALLOW
    res1 = processor.process_event(usgs_event)
    assert res1["decision"] == DecisionType.ALLOW

    # 2. Duplicate replayed -> BLOCK
    res2 = processor.process_event(usgs_event)
    assert res2["decision"] == DecisionType.BLOCK

