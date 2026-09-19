"""
OXYS Comprehensive Test Suite
Tests:
  1. External Data Source Connectivity & Normalization
  2. Kafka Producer & Dispatch Logic
  3. 5 Anomaly Detectors (Schema, Null, Cardinality, Distribution, Duplicate)
  4. Kinetic Circuit Breaker Finite-State Machine
  5. Database Persistence (PostgreSQL / SQLite)
  6. MinIO Storage Manager
  7. FastAPI Control Plane Endpoints
"""
import pytest
import time
from fastapi.testclient import TestClient

from ingestion.base import NormalizedEvent
from ingestion.usgs_source import USGSEarthquakeSource
from ingestion.crypto_source import CryptoMarketSource
from ingestion.weather_source import WeatherTelemetrySource
from ingestion.producer import OxysKafkaProducer
from engine.detectors import (
    SchemaDriftDetector, NullRateDetector, CardinalityDetector,
    DistributionDetector, DuplicateIntegrityDetector
)
from engine.circuit_breaker import CircuitBreakerFSM, DecisionType
from engine.processor import processor
from server.db import db, EventRecord, AnomalyRecord, QuarantinedEventRecord
from server.storage import storage
from server.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


# --- 1. DATA SOURCE & NORMALIZATION TESTS ---

def test_usgs_source_fetch_and_normalize():
    source = USGSEarthquakeSource()
    events = source.fetch_and_normalize()
    assert len(events) > 0, "Expected at least 1 real earthquake event from live USGS feed"
    
    evt = events[0]
    assert evt.source == "usgs"
    assert evt.schema_version == "v1.0.0"
    assert evt.event_id.startswith("usgs_")
    
    payload = evt.payload
    assert "magnitude" in payload
    assert "place" in payload
    assert "longitude" in payload
    assert "latitude" in payload
    assert "depth" in payload
    assert "mag_type" in payload
    assert "status" in payload
    assert "tsunami" in payload
    assert "significance" in payload


def test_usgs_source_deduplication():
    source = USGSEarthquakeSource()
    # First fetch populates seen_events
    events1 = source.fetch_and_normalize()
    assert len(events1) > 0
    
    # Second immediate fetch without upstream changes must yield 0 duplicates
    events2 = source.fetch()
    assert len(events2) == 0, "Second fetch without timestamp updates must be deduplicated"

    # Simulate updated event with higher timestamp
    simulated_raw = {
        "id": "mock_quake_101",
        "properties": {"mag": 3.8, "place": "Near Ridgecrest, CA", "time": 1700000000000, "updated": 1700000001000},
        "geometry": {"coordinates": [-117.5, 35.8, 8.2]}
    }
    source._seen_events["mock_quake_101"] = 1700000001000
    
    # Feature with newer updated timestamp is accepted
    simulated_updated = {
        "id": "mock_quake_101",
        "properties": {"mag": 3.9, "place": "Near Ridgecrest, CA", "time": 1700000000000, "updated": 1700000005000},
        "geometry": {"coordinates": [-117.5, 35.8, 8.2]}
    }
    norm_evt = source.normalize(simulated_updated)
    assert norm_evt.payload["magnitude"] == 3.9
    assert norm_evt.payload["place"] == "Near Ridgecrest, CA"


def test_crypto_source_fetch_and_normalize():
    source = CryptoMarketSource()
    events = source.fetch_and_normalize()
    assert len(events) > 0, "Expected at least 1 crypto event from public market endpoint"
    
    evt = events[0]
    assert evt.source == "binance_crypto_stream"
    assert evt.schema_version == "v1.0.0"
    assert evt.event_id is not None
    assert "symbol" in evt.payload
    assert "last_price" in evt.payload
    assert isinstance(evt.payload["symbol"], str)


def test_weather_source_fetch_and_normalize():
    source = WeatherTelemetrySource()
    events = source.fetch_and_normalize()
    assert len(events) > 0, "Expected at least 1 weather telemetry event"
    
    evt = events[0]
    assert evt.source == "open_meteo_telemetry_stream"
    assert "station_id" in evt.payload
    assert "temperature_c" in evt.payload


def test_deterministic_event_id():
    payload = {"symbol": "BTCUSDT", "last_price": 64250.0}
    ts = "2026-09-19T12:00:00.000Z"
    evt1 = NormalizedEvent(source="crypto", payload=payload, event_timestamp=ts)
    evt2 = NormalizedEvent(source="crypto", payload=payload, event_timestamp=ts)
    assert evt1.event_id == evt2.event_id, "Event IDs with same source, payload, and timestamp must match"


# --- 2. KAFKA PRODUCER TESTS ---

def test_producer_dispatch():
    prod = OxysKafkaProducer()
    evt = NormalizedEvent(source="test_stream", payload={"symbol": "ETHUSDT", "last_price": 3200.0})
    res = prod.publish_normalized(evt)
    assert res["status"] in ["SENT_KAFKA", "QUEUED_MEMORY"]
    assert res["event_id"] == evt.event_id


def test_producer_quarantine_routing_on_malformed():
    prod = OxysKafkaProducer()
    err_evt = NormalizedEvent(
        source="test_stream",
        payload={"_raw_malformed": "{bad json}", "_normalization_error": "SyntaxError"},
        schema_version="v0.0.0-error"
    )
    res = prod.publish_normalized(err_evt)
    assert "QUARANTINE" in res["status"]
    assert res["topic"] == "oxys.quarantine"


# --- 3. ANOMALY DETECTORS TESTS ---

def test_schema_drift_detector():
    detector = SchemaDriftDetector()
    
    # Healthy record
    clean_res = detector.evaluate({"symbol": "BTCUSDT", "last_price": 50000.0, "volume": 100.0})
    assert not clean_res.is_breached
    assert clean_res.current_value == 0.0

    # Corrupt type record (string price instead of float)
    corrupt_res = detector.evaluate({"symbol": "BTCUSDT", "last_price": "STRING_CORRUPT_PRICE"})
    assert corrupt_res.is_breached
    assert corrupt_res.severity == "CRITICAL"


def test_null_rate_detector():
    detector = NullRateDetector(window_size=10, threshold_pct=15.00)
    
    # Healthy records (0% nulls)
    clean_payload = {"symbol": "BTCUSDT", "last_price": 50000.0, "volume": 100.0, "trade_count": 500}
    detector.evaluate(clean_payload)
    
    # Poison record (100% nulls)
    poison_payload = {"symbol": None, "last_price": None, "volume": None, "trade_count": None}
    for _ in range(5):
        res = detector.evaluate(poison_payload)
    
    assert res.is_breached
    assert res.current_value > 15.00


def test_cardinality_detector():
    detector = CardinalityDetector(window_size=20, min_entropy_threshold=0.40)
    # Healthy diversity
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT"]:
        res = detector.evaluate({"symbol": sym})
    assert not res.is_breached


def test_distribution_detector():
    detector = DistributionDetector(window_size=30, z_score_threshold=3.50)
    # Feed normal baseline
    for p in [100.0, 101.0, 99.0, 102.0, 100.5, 99.5, 101.2, 100.1]:
        detector.evaluate({"last_price": p})
    
    # Extreme outlier (e.g. 5000.0)
    outlier_res = detector.evaluate({"last_price": 5000.0})
    assert outlier_res.is_breached
    assert outlier_res.current_value > 3.50


def test_duplicate_integrity_detector():
    detector = DuplicateIntegrityDetector()
    first_res = detector.evaluate("evt_unique_101")
    assert not first_res.is_breached
    
    duplicate_res = detector.evaluate("evt_unique_101")
    assert duplicate_res.is_breached
    assert duplicate_res.severity == "CRITICAL"


# --- 4. CIRCUIT BREAKER & PROCESSOR TESTS ---

def test_circuit_breaker_fsm():
    cb = CircuitBreakerFSM(null_rate_threshold=15.00)
    assert cb.state == "CLOSED"
    
    cb.trip("NULL_RATE_BREACH", "27.41%")
    assert cb.state == "OPEN"
    
    cb.reset()
    assert cb.state == "CLOSED"


def test_end_to_end_processor_allow():
    processor.heal_system()
    evt = NormalizedEvent(
        source="crypto_market_stream",
        payload={"symbol": "BTCUSDT", "last_price": 65000.0, "volume": 120.0, "price_change_percent": 1.5, "trade_count": 4000},
        schema_version="v1.0.0"
    )
    res = processor.process_event(evt)
    assert res["decision"] == DecisionType.ALLOW
    assert res["circuit_state"] == "CLOSED"


def test_end_to_end_processor_quarantine():
    processor.circuit_breaker.reset()
    corrupt_evt = NormalizedEvent(
        source="crypto_market_stream",
        payload={"symbol": "ETHUSDT", "last_price": None, "volume": None, "price_change_percent": None, "trade_count": None},
        schema_version="v1.0.0"
    )
    # Process multiple times to spike rolling null rate
    for _ in range(5):
        res = processor.process_event(corrupt_evt)
    
    assert res["decision"] == DecisionType.QUARANTINE
    assert processor.circuit_breaker.state == "OPEN"
    processor.circuit_breaker.reset()


# --- 5. DATABASE & STORAGE PERSISTENCE TESTS ---

def test_database_persistence():
    event_id = f"test_db_evt_{int(time.time()*1000)}"
    rec = db.record_event(
        event_id=event_id,
        source="unit_test_source",
        payload={"test_metric": 42},
        decision="ALLOW"
    )
    assert rec.event_id == event_id
    
    summary = db.get_metrics_summary()
    assert summary["events_processed"] > 0


def test_storage_quarantine_save():
    batch_id = f"test_{int(time.time()*1000)}"
    path = storage.save_quarantine_batch("test_stream", batch_id, {"reason": "TEST_ISOLATION"})
    assert "oxys-quarantine" in path
    assert batch_id in path


# --- 6. FASTAPI ENDPOINTS TESTS ---

def test_fastapi_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "database" in data
    assert data["database"] == "CONNECTED"


def test_fastapi_metrics_endpoint(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "events_processed" in data
    assert "events_allowed" in data
    assert "null_rate_pct" in data
    assert "circuit_state" in data


def test_fastapi_events_endpoint(client):
    response = client.get("/events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)


def test_fastapi_anomalies_endpoint(client):
    response = client.get("/anomalies")
    assert response.status_code == 200
    data = response.json()
    assert "total_anomalies" in data
    assert "recent_anomalies" in data


def test_fastapi_schema_endpoint(client):
    response = client.get("/schema")
    assert response.status_code == 200
    data = response.json()
    assert data["active_schema_version"] == "v1.0.0"
    assert "expected_fields" in data


def test_fastapi_pipeline_status_endpoint(client):
    response = client.get("/pipeline/status")
    assert response.status_code == 200
    data = response.json()
    assert data["pipeline_name"] == "oxys-real-time-integrity-engine"
    assert "kafka_topics" in data
    assert "minio_buckets" in data


def test_fastapi_circuit_action_endpoint(client):
    # Test circuit trip
    res_trip = client.post("/api/circuit/trip")
    assert res_trip.status_code == 200
    assert res_trip.json()["state"] == "OPEN"
    
    # Test circuit reset
    res_reset = client.post("/api/circuit/reset")
    assert res_reset.status_code == 200
    assert res_reset.json()["state"] == "CLOSED"
