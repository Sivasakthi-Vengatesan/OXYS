"""
StreamPulse Pydantic Models & Data Contracts
"""
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class StreamModel(BaseModel):
    id: str
    name: str
    topic: str
    consumer_group: str
    partitions: int
    rate: int
    lag: int
    offset: int
    batch_id: int
    schema_ver: str
    circuit: str
    status: str
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

class PipelineModel(BaseModel):
    id: str
    name: str
    source_topic: str
    processor_job: str
    guard_id: str
    sink_dest: str
    state: str
    last_execution: str

class GuardModel(BaseModel):
    id: str
    name: str
    type: str
    current: float
    threshold: float
    breaches: int
    status: str
    metric: str
    limit: str
    last_evaluated: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

class CircuitBreakerModel(BaseModel):
    state: str  # CLOSED, BREACH DETECTED, OPEN, QUARANTINE, HALF-OPEN, RECOVERED
    stream: str
    batch_id: str
    reason: str
    current_metric: str
    threshold: str
    last_transition: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

class QuarantineBatchModel(BaseModel):
    batch_id: str
    stream: str
    reason: str
    current_val: str
    threshold_val: str
    checkpoint: str
    status: str  # ISOLATED, REPLAYED, RELEASED, DELETED
    timestamp: str
    storage: str
    records_total: int
    poison_records: int
    sample_poison: Dict[str, Any]

class CheckpointModel(BaseModel):
    checkpoint_id: str
    stream: str
    last_valid_batch: str
    failed_batch: str
    recovery_status: str
    storage_path: str

class EbpfTelemetryModel(BaseModel):
    cpu: float
    memory: float
    network_rx: float
    network_tx: float
    packet_loss: float
    retransmits: float
    socket_latency: float

class DataQualityTelemetryModel(BaseModel):
    null_rate: float
    schema_status: str
    volume_delta: str
    cardinality_entropy: float
    z_score_drift: float
    dead_letter_count: int

class SystemEventModel(BaseModel):
    id: str
    time: str
    source: str
    message: str
    tag: str  # HEALTHY, ALERT, INFO, RECOVERY, CONFIG
    category: str  # STREAM, GUARD, CIRCUIT, QUARANTINE, RECOVERY, INFRASTRUCTURE

class PolicyModel(BaseModel):
    id: str
    name: str
    value: str
    unit: str
    description: str
    current_state: str

class IncidentModel(BaseModel):
    id: str
    time: str
    stream: str
    batch_id: str
    trigger: str
    current_value: str
    threshold: str
    circuit_state: str
    action: str
    checkpoint: str
    recovery_status: str
