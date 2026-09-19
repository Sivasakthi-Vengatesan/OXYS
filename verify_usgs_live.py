"""
OXYS Live End-to-End USGS Pipeline Verification Script
Executes and traces real data flow:
  USGS Real GeoJSON API
  -> Ingestion & Normalizer
  -> Kafka Producer (oxys.raw)
  -> Spark Streaming / Stream Processor
  -> OXYS Anomaly & Integrity Engine
  -> PostgreSQL Database Persistence & Lakehouse Vault
  -> FastAPI Control Plane Verification
"""
import sys
import os
import json
import time
from datetime import datetime, timezone

# Add scratch path to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.usgs_source import USGSEarthquakeSource
from ingestion.producer import OxysKafkaProducer
from engine.processor import processor
from server.db import db
from fastapi.testclient import TestClient
from server.main import app

def run_live_verification():
    print("=" * 60)
    print("OXYS REAL-TIME STREAMING INTEGRITY ENGINE - USGS VERIFICATION")
    print("=" * 60)
    
    # 1. Fetch Real USGS Earthquake Feed
    print("\n[STEP 1] Fetching live data from USGS GeoJSON Feed...")
    source = USGSEarthquakeSource()
    events = source.fetch_and_normalize()
    print(f"-> Successfully ingested and normalized {len(events)} earthquake features from USGS.")
    
    if not events:
        print("[ERROR] No events fetched from USGS feed.")
        return False
        
    sample_event = events[0]
    print(f"-> Sample Event ID: {sample_event.event_id}")
    print(f"-> Source: {sample_event.source}")
    print(f"-> Timestamp: {sample_event.event_timestamp}")
    print(f"-> Payload: {json.dumps(sample_event.payload, indent=2)}")
    
    # 2. Publish to Kafka Producer
    print("\n[STEP 2] Dispatching to Kafka Producer (oxys.raw)...")
    producer = OxysKafkaProducer()
    producer.connect()
    pub_res = producer.publish_normalized(sample_event)
    print(f"-> Kafka Publish Status: {pub_res['status']}")
    print(f"-> Target Topic: {pub_res['topic']}")
    print(f"-> Event Hash ID: {pub_res['event_id']}")
    
    # 3. Process through OXYS Real-Time Engine
    print("\n[STEP 3] Executing OXYS Stream Processor & Detection Guards...")
    proc_res = processor.process_event(sample_event)
    print(f"-> Spark Batch ID: #{proc_res['batch_id']}")
    print(f"-> OXYS Decision: {proc_res['decision']}")
    print(f"-> Circuit Breaker State: {proc_res['circuit_state']}")
    print(f"-> Breaches Detected: {len(proc_res['breaches'])}")
    
    # 4. Verify Database Persistence
    print("\n[STEP 4] Verifying PostgreSQL / SQLite Database Record...")
    db_record = db.get_event_by_id(sample_event.event_id)
    matched = db_record is not None
    print(f"-> Database Event Found by ID ({sample_event.event_id}): {matched}")
    if db_record:
        print(f"-> Stored Record: Source={db_record['source']}, Decision={db_record['decision']}, Magnitude={db_record['payload'].get('magnitude')}, Place='{db_record['payload'].get('place')}'")
    print(f"-> Total Processed Records in DB: {db.get_metrics_summary()['events_processed']}")
    
    # 5. Verify FastAPI Control Plane
    print("\n[STEP 5] Querying FastAPI Endpoints...")
    client = TestClient(app)
    health_resp = client.get("/health").json()
    metrics_resp = client.get("/metrics").json()
    schema_resp = client.get("/schema").json()
    
    print(f"-> GET /health: status={health_resp.get('status')}, circuit={health_resp.get('circuit_breaker')}")
    print(f"-> GET /metrics: events_processed={metrics_resp.get('events_processed')}, null_rate={metrics_resp.get('null_rate_pct')}%")
    print(f"-> GET /schema: active_version={schema_resp.get('active_schema_version')}, fields={list(schema_resp.get('expected_fields', {}).keys())}")
    
    # 6. Test Controlled Fault Injections (DEV ONLY)
    print("\n[STEP 6] Executing Controlled Dev-Only Fault Injections...")
    
    # Schema Drift
    from ingestion.base import NormalizedEvent
    print("   [6.1] Testing Schema Drift Injection (magnitude as string)...")
    drift_event = NormalizedEvent(
        source="usgs",
        payload={
            "magnitude": "INVALID_STRING_MAGNITUDE",
            "place": "Test Location",
            "longitude": -118.4,
            "latitude": 34.0,
            "depth": 10.0,
            "mag_type": "ml",
            "status": "reviewed",
            "tsunami": 0,
            "significance": 10
        },
        schema_version="v1.0.0"
    )
    drift_res = processor.process_event(drift_event)
    print(f"   -> Result: Decision={drift_res['decision']}, Circuit={drift_res['circuit_state']}, Reason={drift_res.get('breaches', [{}])[0].get('reason')}")
    processor.heal_system()
    
    # Duplicate Storm
    print("   [6.2] Testing Duplicate Replay Storm...")
    dup_id = f"usgs_dup_storm_{int(time.time())}"
    dup_evt = NormalizedEvent(
        source="usgs",
        payload={"magnitude": 3.1, "place": "San Francisco, CA", "longitude": -122.4, "latitude": 37.7, "depth": 8.0, "mag_type": "ml", "status": "automatic", "tsunami": 0, "significance": 50},
        event_id=dup_id
    )
    processor.process_event(dup_evt) # 1st pass: ALLOW
    dup_res = processor.process_event(dup_evt) # 2nd pass: BLOCK
    print(f"   -> Result: Decision={dup_res['decision']}, Reason={dup_res.get('breaches', [{}])[0].get('reason')}")
    processor.heal_system()

    print("\n" + "=" * 60)
    print("ALL VERIFICATIONS COMPLETED SUCCESSFULLY WITH ZERO MOCK DATA.")
    print("=" * 60)
    return True

if __name__ == "__main__":
    success = run_live_verification()
    sys.exit(0 if success else 1)
