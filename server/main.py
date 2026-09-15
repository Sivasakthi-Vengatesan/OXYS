"""
StreamPulse FastAPI Control Plane Backend
"""
import asyncio
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

from server.models import (
    StreamModel, PipelineModel, GuardModel, CircuitBreakerModel,
    QuarantineBatchModel, CheckpointModel, EbpfTelemetryModel,
    DataQualityTelemetryModel, SystemEventModel, PolicyModel, IncidentModel
)
from server.engine import engine

app = FastAPI(
    title="StreamPulse API",
    description="Real-time Data Reliability & Containment Control Plane",
    version="1.0.0"
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
            state_payload = {
                "system_status": engine.system_status,
                "streams": engine.streams,
                "guards": engine.guards,
                "circuit_breaker": engine.circuit_breaker,
                "quarantine": engine.quarantine,
                "ebpf": engine.ebpf,
                "data_quality": engine.data_quality,
                "active_incident": engine.active_incident,
                "latest_event": engine.events[0] if engine.events else None
            }
            await websocket.send_json(state_payload)
            await asyncio.sleep(1.5)
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)

# --- REST ENDPOINTS ---

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
        engine.circuit_breaker["state"] = "HALF-OPEN"
        engine.add_event("circuit_breaker", "CANARY PROBE BATCH INJECTED (HALF-OPEN)", "INFO", "CIRCUIT")
    return engine.circuit_breaker

@app.get("/api/quarantine", response_model=List[QuarantineBatchModel])
def get_quarantine():
    return engine.quarantine

@app.get("/api/quarantine/{batch_id}", response_model=QuarantineBatchModel)
def get_quarantine_batch(batch_id: str):
    b = next((q for q in engine.quarantine if q["batch_id"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not in quarantine")
    return b

@app.post("/api/quarantine/{batch_id}/replay")
def post_replay_batch(batch_id: str):
    b = next((q for q in engine.quarantine if q["batch_id"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not found")
    b["status"] = "REPLAYED"
    engine.add_event("quarantine_engine", f"BATCH #{batch_id} REPLAYED WITH SANITIZED SCHEMA", "HEALTHY", "QUARANTINE")
    return {"status": "SUCCESS", "message": f"Batch #{batch_id} replayed cleanly."}

@app.post("/api/quarantine/{batch_id}/release")
def post_release_batch(batch_id: str):
    b = next((q for q in engine.quarantine if q["batch_id"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not found")
    b["status"] = "RELEASED"
    engine.add_event("quarantine_engine", f"OVERRIDE: BATCH #{batch_id} RELEASED TO DOWNSTREAM SINK", "ALERT", "QUARANTINE")
    return {"status": "RELEASED", "message": f"Batch #{batch_id} released to sink."}

@app.post("/api/quarantine/{batch_id}/delete")
def post_delete_batch(batch_id: str):
    b = next((q for q in engine.quarantine if q["batch_id"] == batch_id), None)
    if not b:
        raise HTTPException(status_code=404, detail="Batch not found")
    engine.quarantine = [q for q in engine.quarantine if q["batch_id"] != batch_id]
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
    is_data = engine.guards[1]["current"] > 15.0
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

# Simulation Endpoints
@app.post("/api/simulation/inject/{anomaly_type}")
def post_inject_anomaly(anomaly_type: str):
    if anomaly_type == "null":
        engine.inject_null_spike()
    elif anomaly_type == "schema":
        schema_g = next((g for g in engine.guards if g["id"] == "schema"), None)
        if schema_g:
            schema_g["current"] = 1.00
            schema_g["metric"] = "FIELD TYPE MISMATCH (amount_cents: String != Int64)"
            schema_g["status"] = "BREACHED"
        engine.circuit_breaker["state"] = "OPEN"
        engine.circuit_breaker["reason"] = "SCHEMA_DRIFT"
        engine.add_event("guard_schema", "CRITICAL: SCHEMA DRIFT on payments", "ALERT", "GUARD")
    elif anomaly_type == "volume":
        vol_g = next((g for g in engine.guards if g["id"] == "volume"), None)
        if vol_g:
            vol_g["current"] = 184.20
            vol_g["metric"] = "+184.20% / min"
            vol_g["status"] = "BREACHED"
        engine.circuit_breaker["state"] = "OPEN"
        engine.circuit_breaker["reason"] = "VOLUME_BREACH"
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

# --- STATIC FILES ROUTING ---
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

@app.get("/")
@app.get("/index.html")
async def serve_index():
    return FileResponse(os.path.join(base_dir, "index.html"))

@app.get("/app")
@app.get("/app.html")
async def serve_app():
    return FileResponse(os.path.join(base_dir, "app.html"))

app.mount("/styles", StaticFiles(directory=os.path.join(base_dir, "styles")), name="styles")
app.mount("/js", StaticFiles(directory=os.path.join(base_dir, "js")), name="js")

