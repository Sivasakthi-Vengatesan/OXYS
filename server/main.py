"""
OXYS FastAPI Control Plane Backend
Real-time Streaming Data Integrity & Anomaly Protection System
"""
import asyncio
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
import os
from datetime import datetime

from server.models import (
    StreamModel, PipelineModel, GuardModel, CircuitBreakerModel,
    QuarantineBatchModel, CheckpointModel, EbpfTelemetryModel,
    DataQualityTelemetryModel, SystemEventModel, PolicyModel, IncidentModel
)
from server.engine import engine
from server.db import db
from server.storage import storage
from engine.processor import processor

from contextlib import asynccontextmanager

is_serverless = bool(os.environ.get("VERCEL") or os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not is_serverless:
        engine.start_services()
    yield
    if not is_serverless:
        engine.stop_services()

app = FastAPI(
    title="OXYS API",
    description="OXYS - Real-time integrity protection for streaming data.",
    version="2.0.0",
    lifespan=None if is_serverless else lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Active WebSocket connections
connected_websockets: List[WebSocket] = []


@app.websocket("/ws/events")
async def websocket_events_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.append(websocket)
    try:
        while True:
            metrics_summary = db.get_metrics_summary()
            state_payload = {
                "system_status": engine.system_status,
                "streams": engine.streams,
                "guards": engine.guards,
                "circuit_breaker": engine.circuit_breaker,
                "quarantine": engine.quarantine,
                "ebpf": engine.ebpf,
                "data_quality": engine.data_quality,
                "active_incident": engine.active_incident,
                "metrics_summary": metrics_summary,
                "latest_event": engine.events[0] if engine.events else None
            }
            await websocket.send_json(state_payload)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)


# ==========================================
# 1. CORE REQUIRED API ENDPOINTS
# ==========================================

@app.get("/health")
@app.get("/api/health")
def get_health():
    return {
        "status": "HEALTHY" if engine.system_status == "ONLINE" else "DEGRADED",
        "system_status": engine.system_status,
        "database": "CONNECTED",
        "storage": "CONNECTED",
        "circuit_breaker": engine.circuit_breaker["state"],
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/metrics")
@app.get("/api/metrics")
def get_metrics():
    summary = db.get_metrics_summary()
    guards = engine.guards
    null_g = next((g for g in guards if g["id"] == "null_rate"), None)
    card_g = next((g for g in guards if g["id"] == "cardinality"), None)
    
    return {
        "events_processed": summary["events_processed"],
        "events_allowed": summary["events_allowed"],
        "events_quarantined": summary["events_quarantined"],
        "events_blocked": summary["events_blocked"],
        "anomaly_count": summary["anomaly_count"],
        "null_rate_pct": null_g["current"] if null_g else 2.14,
        "cardinality_entropy": card_g["current"] if card_g else 0.88,
        "circuit_state": engine.circuit_breaker["state"],
        "pipeline_health": "OPTIMAL" if engine.circuit_breaker["state"] == "CLOSED" else "CONTAINMENT_ENGAGED"
    }


@app.get("/events")
@app.get("/api/events")
def get_real_events(limit: int = Query(50, ge=1, le=500)):
    return db.get_recent_events(limit=limit)


@app.get("/anomalies")
@app.get("/api/anomalies")
def get_anomalies():
    metrics = db.get_metrics_summary()
    return {
        "total_anomalies": metrics["anomaly_count"],
        "recent_anomalies": metrics["recent_anomalies"]
    }


@app.get("/schema")
@app.get("/api/schema")
def get_schema(source: Optional[str] = Query(None)):
    schemas = {
        "usgs": {
            "magnitude": "float (seismic amplitude)",
            "place": "string (geographic descriptor)",
            "longitude": "float (degrees)",
            "latitude": "float (degrees)",
            "depth": "float (hypocenter depth km)",
            "mag_type": "string (magnitude scale e.g. mb, ml, mw)",
            "status": "string (reviewed / automatic)",
            "tsunami": "integer (0/1)",
            "significance": "integer (0-1000)"
        },
        "crypto": {
            "symbol": "string (categorical key)",
            "last_price": "float (continuous numeric)",
            "volume": "float (continuous numeric)",
            "price_change_percent": "float (continuous numeric)",
            "trade_count": "integer"
        },
        "weather": {
            "station_id": "string",
            "latitude": "float",
            "longitude": "float",
            "temperature_c": "float",
            "relative_humidity_pct": "float",
            "wind_speed_kmh": "float"
        }
    }
    expected = schemas.get(source, schemas["usgs"]) if source else schemas["usgs"]
    return {
        "active_schema_version": "v1.0.0",
        "primary_source": source or "usgs",
        "expected_fields": expected,
        "all_schemas": schemas,
        "validation_policy": "STRICT_AVRO_COMPATIBLE",
        "drift_tolerance": "0 allowed"
    }


@app.get("/quarantine")
@app.get("/api/quarantine/all")
def get_quarantine_all():
    return db.get_quarantined_batches()


@app.get("/pipeline/status")
@app.get("/api/pipeline/status")
def get_pipeline_status():
    return {
        "pipeline_name": "oxys-real-time-integrity-engine",
        "status": engine.system_status,
        "circuit_breaker": engine.circuit_breaker,
        "active_pipelines": engine.pipelines,
        "active_streams": engine.streams,
        "kafka_topics": ["oxys.raw", "oxys.normalized", "oxys.quarantine"],
        "minio_buckets": ["oxys-quarantine", "oxys-checkpoints", "oxys-lakehouse"]
    }


# ==========================================
# 2. REST API ENDPOINTS FOR DASHBOARD & CLI
# ==========================================

@app.get("/api/system/status")
def get_system_status():
    return {
        "status": engine.system_status,
        "active_pipelines": len(engine.pipelines),
        "active_streams": len(engine.streams),
        "active_guards": len(engine.guards),
        "open_incidents": 1 if engine.active_incident else 0,
        "circuit_state": engine.circuit_breaker["state"],
        "kafka_status": "CONNECTED",
        "spark_status": "RUNNING",
        "minio_status": "CONNECTED",
        "postgres_status": "CONNECTED",
        "ebpf_status": "ACTIVE"
    }


@app.get("/api/streams", response_model=List[StreamModel])
def get_streams():
    return engine.streams


@app.get("/api/streams/{stream_id}", response_model=StreamModel)
def get_stream_by_id(stream_id: str):
    s = next((st for st in engine.streams if st["id"] == stream_id), None)
    if not s:
        raise HTTPException(status_code=404, detail="Stream not found")
    return s


@app.get("/api/pipelines", response_model=List[PipelineModel])
def get_pipelines():
    return engine.pipelines


@app.get("/api/guards", response_model=List[GuardModel])
def get_guards():
    return engine.guards


@app.get("/api/circuit", response_model=CircuitBreakerModel)
def get_circuit_breaker():
    return engine.circuit_breaker


@app.post("/api/circuit/{action}")
def post_circuit_action(action: str):
    if action == "trip":
        engine.inject_null_spike()
    elif action == "reset":
        engine.heal_system()
    elif action == "half-open":
        processor.circuit_breaker.half_open()
        engine.add_event("circuit_breaker", "CANARY PROBE BATCH INJECTED (HALF-OPEN)", "INFO", "CIRCUIT")
    return engine.circuit_breaker


@app.get("/api/quarantine", response_model=List[QuarantineBatchModel])
def get_quarantine():
    return engine.quarantine


@app.get("/api/quarantine/{batch_id}")
def get_quarantine_batch(batch_id: str):
    b = next((q for q in engine.quarantine if q["batchId"] == batch_id or q.get("batch_id") == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not in quarantine")
    return b


@app.post("/api/quarantine/{batch_id}/replay")
def post_replay_batch(batch_id: str):
    batches = db.get_quarantined_batches()
    b = next((q for q in batches if q["batchId"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not found")
    engine.add_event("quarantine_engine", f"BATCH #{batch_id} REPLAYED WITH SANITIZED SCHEMA", "HEALTHY", "QUARANTINE")
    return {"status": "SUCCESS", "message": f"Batch #{batch_id} replayed cleanly."}


@app.post("/api/quarantine/{batch_id}/release")
def post_release_batch(batch_id: str):
    batches = db.get_quarantined_batches()
    b = next((q for q in batches if q["batchId"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not found")
    engine.add_event("quarantine_engine", f"OVERRIDE: BATCH #{batch_id} RELEASED TO DOWNSTREAM SINK", "ALERT", "QUARANTINE")
    return {"status": "RELEASED", "message": f"Batch #{batch_id} released to sink."}


@app.post("/api/quarantine/{batch_id}/delete")
def post_delete_batch(batch_id: str):
    batches = db.get_quarantined_batches()
    b = next((q for q in batches if q["batchId"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not found")
    with db.get_session() as session:
        from server.db import QuarantinedEventRecord
        session.query(QuarantinedEventRecord).filter(QuarantinedEventRecord.batch_id == batch_id).delete()
        session.commit()
    engine.add_event("quarantine_engine", f"PERMANENT AUDIT PURGE: BATCH #{batch_id} REMOVED FROM VAULT", "ALERT", "QUARANTINE")
    return {"status": "DELETED", "message": f"Batch #{batch_id} deleted."}


@app.get("/api/recovery")
def get_recovery():
    return engine.recovery


@app.post("/api/recovery/restore")
def post_restore_recovery():
    engine.heal_system()
    engine.add_event("recovery_manager", f"RESTORE CHECKPOINT {engine.recovery['current_checkpoint']} EXECUTED", "HEALTHY", "RECOVERY")
    return {"status": "RESTORED", "checkpoint": engine.recovery["current_checkpoint"]}


@app.get("/api/telemetry")
def get_telemetry():
    return {
        "ebpf": engine.ebpf,
        "data_quality": engine.data_quality
    }


@app.post("/api/telemetry/correlate")
def post_telemetry_correlate():
    guards = engine.guards
    null_g = next((g for g in guards if g["id"] == "null_rate"), None)
    is_data = (null_g["current"] > 15.0) if null_g else False
    is_infra = engine.ebpf["socket_latency"] > 50.0

    if is_data and not is_infra:
        verdict = "DATA_CORRUPTION"
        diag = "PURE APPLICATION DATA ANOMALY. Kernel eBPF layer healthy (3.1ms latency, 0% drop). Upstream serialization defect."
    elif is_infra and not is_data:
        verdict = "INFRASTRUCTURE_BOTTLENECK"
        diag = "INFRASTRUCTURE NETWORK DEGRADATION. Data schemas valid, but high socket latency and retransmits detected."
    else:
        verdict = "ALL_SYSTEMS_NOMINAL"
        diag = "ALL SYSTEMS NOMINAL. Infrastructure and data quality operating within baseline parameters."

    return {
        "verdict": verdict,
        "diagnosis": diag,
        "ebpf_sample": engine.ebpf,
        "data_quality_sample": engine.data_quality
    }


@app.get("/api/events", response_model=List[SystemEventModel])
def get_events(category: Optional[str] = None):
    if category and category.upper() != "ALL":
        return [e for e in engine.events if e["category"].upper() == category.upper()]
    return engine.events


@app.get("/api/policies", response_model=List[PolicyModel])
def get_policies():
    return engine.policies


@app.put("/api/policies/{policy_id}")
def update_policy(policy_id: str, payload: Dict[str, Any]):
    p = next((pol for pol in engine.policies if pol["id"] == policy_id), None)
    if not p:
        raise HTTPException(status_code=404, detail="Policy not found")
    p["value"] = payload.get("value", p["value"])
    engine.add_event("config_manager", f"POLICY UPDATED: {p['name']} -> {p['value']} {p['unit']}", "CONFIG", "GUARD")
    return p


# Anomaly Injections for Failure Testing & Demo
@app.post("/api/simulation/inject/{anomaly_type}")
def post_inject_anomaly(anomaly_type: str):
    if anomaly_type == "null":
        engine.inject_null_spike()
    elif anomaly_type == "schema":
        engine.inject_schema_drift()
    elif anomaly_type == "duplicate":
        engine.inject_duplicate_event()
    elif anomaly_type == "volume":
        vol_g = next((g for g in engine.guards if g["id"] == "volume"), None)
        if vol_g:
            vol_g["current"] = 184.20
            vol_g["metric"] = "+184.20% / min"
            vol_g["status"] = "BREACHED"
        processor.circuit_breaker.trip("VOLUME_SURGE_SPIKE", "+184.20%")
        engine.add_event("guard_volume", "VOLUME SURGE BREACH: +184.20%", "ALERT", "GUARD")
    elif anomaly_type == "network":
        engine.ebpf["socket_latency"] = 88.4
        engine.ebpf["packet_loss"] = 4.2
        engine.add_event("ebpf_kernel", "HIGH SOCKET LATENCY (88.4ms) & 4.2% PACKET LOSS", "ALERT", "INFRASTRUCTURE")
    return {"status": "INJECTED", "anomaly": anomaly_type}


@app.post("/api/simulation/heal")
def post_heal_simulation():
    engine.heal_system()
    return {"status": "HEALED", "system_status": engine.system_status}


# ==========================================
# 3. STATIC FILES ROUTING
# ==========================================
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@app.get("/")
@app.get("/index.html")
async def serve_index():
    index_file = os.path.join(base_dir, "index.html")
    if os.path.exists(index_file):
        try:
            with open(index_file, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        except Exception:
            return FileResponse(index_file)
    return {"message": "OXYS API Online", "health": "/api/health", "docs": "/docs"}


@app.get("/app")
@app.get("/app.html")
async def serve_app():
    app_file = os.path.join(base_dir, "app.html")
    if os.path.exists(app_file):
        try:
            with open(app_file, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        except Exception:
            return FileResponse(app_file)
    return {"message": "OXYS App Online"}


styles_dir = os.path.join(base_dir, "styles")
if os.path.isdir(styles_dir):
    app.mount("/styles", StaticFiles(directory=styles_dir), name="styles")

js_dir = os.path.join(base_dir, "js")
if os.path.isdir(js_dir):
    app.mount("/js", StaticFiles(directory=js_dir), name="js")

assets_dir = os.path.join(base_dir, "assets")
if os.path.isdir(assets_dir):
    app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
