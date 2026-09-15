"""
StreamPulse Backend Engine & State Controller
"""
import random
from datetime import datetime
from typing import List, Dict, Any, Optional

class StreamPulseEngine:
    def __init__(self):
        self.system_status = "ONLINE"
        self.active_incident = None

        # Streams
        self.streams = [
            {
                "id": "orders",
                "name": "orders",
                "topic": "prod.cdc.orders.v2",
                "consumer_group": "spark-streampulse-orders-cg",
                "partitions": 8,
                "rate": 18421,
                "lag": 2381,
                "offset": 4892180,
                "batch_id": 483,
                "schema_ver": "v2.4.0 (Avro)",
                "circuit": "CLOSED",
                "status": "RUNNING"
            },
            {
                "id": "payments",
                "name": "payments",
                "topic": "prod.events.payments.v1",
                "consumer_group": "spark-streampulse-payments-cg",
                "partitions": 12,
                "rate": 12083,
                "lag": 841,
                "offset": 9140221,
                "batch_id": 194,
                "schema_ver": "v1.1.2 (Protobuf)",
                "circuit": "CLOSED",
                "status": "RUNNING"
            },
            {
                "id": "telemetry",
                "name": "telemetry",
                "topic": "infra.ebpf.telemetry",
                "consumer_group": "spark-streampulse-telemetry-cg",
                "partitions": 16,
                "rate": 8291,
                "lag": 1102,
                "offset": 18491024,
                "batch_id": 841,
                "schema_ver": "v3.0.1 (FlatBuffers)",
                "circuit": "CLOSED",
                "status": "RUNNING"
            },
            {
                "id": "inventory",
                "name": "inventory",
                "topic": "prod.wms.inventory.v1",
                "consumer_group": "spark-streampulse-inventory-cg",
                "partitions": 6,
                "rate": 5412,
                "lag": 310,
                "offset": 2314901,
                "batch_id": 312,
                "schema_ver": "v1.0.0 (Avro)",
                "circuit": "CLOSED",
                "status": "RUNNING"
            }
        ]

        # Pipelines
        self.pipelines = [
            {
                "id": "orders-production",
                "name": "orders-production",
                "source_topic": "prod.cdc.orders.v2",
                "processor_job": "orders-cleaner (Spark 3.5)",
                "guard_id": "orders-guard-inline",
                "sink_dest": "MinIO Lakehouse + PG meta",
                "state": "RUNNING",
                "last_execution": "04:32:18 UTC"
            },
            {
                "id": "payments-production",
                "name": "payments-production",
                "source_topic": "prod.events.payments.v1",
                "processor_job": "payments-stream-agg (Spark 3.5)",
                "guard_id": "payments-guard-inline",
                "sink_dest": "MinIO Parquet Lakehouse",
                "state": "RUNNING",
                "last_execution": "04:32:15 UTC"
            },
            {
                "id": "telemetry-production",
                "name": "telemetry-production",
                "source_topic": "infra.ebpf.telemetry",
                "processor_job": "telemetry-stream-collector",
                "guard_id": "infra-guard-inline",
                "sink_dest": "Prometheus / ClickHouse",
                "state": "RUNNING",
                "last_execution": "04:32:10 UTC"
            }
        ]

        # Guards
        self.guards = [
            {
                "id": "schema",
                "name": "SCHEMA INTEGRITY",
                "type": "STRUCTURAL",
                "current": 0.00,
                "threshold": 0.00,
                "breaches": 0,
                "status": "HEALTHY",
                "metric": "0 drift",
                "limit": "0 allowed"
            },
            {
                "id": "null_rate",
                "name": "NULL RATE",
                "type": "QUALITY",
                "current": 2.14,
                "threshold": 15.00,
                "breaches": 0,
                "status": "HEALTHY",
                "metric": "2.14%",
                "limit": "15.00%"
            },
            {
                "id": "cardinality",
                "name": "CARDINALITY",
                "type": "ENTROPY",
                "current": 0.88,
                "threshold": 0.40,
                "breaches": 0,
                "status": "HEALTHY",
                "metric": "0.88 entropy",
                "limit": "> 0.40"
            },
            {
                "id": "distribution",
                "name": "DISTRIBUTION",
                "type": "STATISTICAL",
                "current": 1.12,
                "threshold": 3.50,
                "breaches": 0,
                "status": "HEALTHY",
                "metric": "1.12 Z-Score",
                "limit": "< 3.50"
            },
            {
                "id": "volume",
                "name": "VOLUME",
                "type": "THROUGHPUT",
                "current": 3.40,
                "threshold": 50.00,
                "breaches": 0,
                "status": "HEALTHY",
                "metric": "+3.40% / min",
                "limit": "< 50.00%"
            }
        ]

        # Circuit Breaker
        self.circuit_breaker = {
            "state": "CLOSED",
            "stream": "orders",
            "batch_id": "#00483",
            "reason": "NONE",
            "current_metric": "2.14%",
            "threshold": "15.00%"
        }

        # Quarantined Batches
        self.quarantine = [
            {
                "batch_id": "00483",
                "stream": "orders",
                "reason": "NULL_RATE_BREACH",
                "current_val": "27.41%",
                "threshold_val": "15.00%",
                "checkpoint": "cp_00482",
                "status": "ISOLATED",
                "timestamp": "04:32:18 UTC",
                "storage": "s3://streampulse-quarantine/orders/batch_00483.parquet",
                "records_total": 18420,
                "poison_records": 5048,
                "sample_poison": {
                    "order_id": "ord_994821a",
                    "customer_id": None,
                    "total_amount": 142.50,
                    "currency": None,
                    "tax_code": None,
                    "created_at": "2026-09-15T04:32:18.112Z",
                    "_quarantine_meta": {
                        "flagged_fields": ["customer_id", "currency", "tax_code"],
                        "rule": "NON_NULLABLE_CONSTRAINT_VIOLATION",
                        "guard_id": "GUARD_NULL_RATE"
                    }
                }
            },
            {
                "batch_id": "00471",
                "stream": "payments",
                "reason": "SCHEMA_DRIFT",
                "current_val": "FIELD_TYPE_MISMATCH",
                "threshold_val": "STRICT_COMPATIBILITY",
                "checkpoint": "cp_00470",
                "status": "ISOLATED",
                "timestamp": "03:14:02 UTC",
                "storage": "s3://streampulse-quarantine/payments/batch_00471.parquet",
                "records_total": 12050,
                "poison_records": 12050,
                "sample_poison": {
                    "payment_id": "pay_882901",
                    "amount_cents": "14250",
                    "merchant_id": "m_alpha_7",
                    "_quarantine_meta": {
                        "flagged_fields": ["amount_cents"],
                        "rule": "SCHEMA_PARSER_TYPE_ERROR (Expected INT64, got STRING)",
                        "guard_id": "GUARD_SCHEMA_INTEGRITY"
                    }
                }
            },
            {
                "batch_id": "00452",
                "stream": "orders",
                "reason": "VOLUME_BREACH",
                "current_val": "+184.20%",
                "threshold_val": "+50.00%",
                "checkpoint": "cp_00451",
                "status": "ISOLATED",
                "timestamp": "01:52:45 UTC",
                "storage": "s3://streampulse-quarantine/orders/batch_00452.parquet",
                "records_total": 45200,
                "poison_records": 45200,
                "sample_poison": {
                    "burst_trigger": "RETRY_STORM_DETECTED",
                    "rate": "45200/s",
                    "_quarantine_meta": {
                        "flagged_fields": ["rate"],
                        "rule": "RATE_BREACH_CONTAINMENT",
                        "guard_id": "GUARD_VOLUME"
                    }
                }
            }
        ]

        # Recovery
        self.recovery = {
            "current_checkpoint": "cp_00482",
            "last_valid_batch": "00482",
            "failed_batch": "00483",
            "recovery_status": "READY",
            "storage_path": "s3://streampulse-checkpoints/prod/orders/cp_00482.chk",
            "history": [
                {
                    "time": "04:32:18",
                    "stream": "orders",
                    "checkpoint": "cp_00482",
                    "action": "BREACH",
                    "result": "FAILED"
                },
                {
                    "time": "04:32:19",
                    "stream": "orders",
                    "checkpoint": "cp_00482",
                    "action": "QUARANTINE",
                    "result": "SUCCESS"
                },
                {
                    "time": "04:32:24",
                    "stream": "orders",
                    "checkpoint": "cp_00482",
                    "action": "RESUME",
                    "result": "SUCCESS"
                }
            ]
        }

        # Telemetry
        self.ebpf = {
            "cpu": 42.4,
            "memory": 61.8,
            "network_rx": 82.4,
            "network_tx": 45.1,
            "packet_loss": 0.02,
            "retransmits": 0.01,
            "socket_latency": 3.1
        }
        self.data_quality = {
            "null_rate": 2.14,
            "schema_status": "VERIFIED",
            "volume_delta": "+4.2%",
            "cardinality_entropy": 0.88,
            "z_score_drift": 1.12,
            "dead_letter_count": 3
        }

        # Events
        self.events = [
            {"id": "evt-1", "time": "04:31:02", "source": "batch_00481", "message": "SPARK MICRO-BATCH PROCESSED (18,410 records)", "tag": "HEALTHY", "category": "STREAM"},
            {"id": "evt-2", "time": "04:32:03", "source": "batch_00482", "message": "SPARK MICRO-BATCH PROCESSED (18,421 records)", "tag": "HEALTHY", "category": "STREAM"},
            {"id": "evt-3", "time": "04:32:18", "source": "guard_null_rate", "message": "NULL_RATE_BREACH (27.41% > 15.00% threshold)", "tag": "ALERT", "category": "GUARD"},
            {"id": "evt-4", "time": "04:32:18", "source": "circuit_breaker", "message": "CIRCUIT STATE TRANSITION -> OPEN [orders]", "tag": "ALERT", "category": "CIRCUIT"},
            {"id": "evt-5", "time": "04:32:19", "source": "batch_00483", "message": "POISON BATCH ISOLATED TO MinIO QUARANTINE VAULT", "tag": "ALERT", "category": "QUARANTINE"},
            {"id": "evt-6", "time": "04:32:21", "source": "recovery_manager", "message": "RECOVERY CHECKPOINT IDENTIFIED -> cp_00482", "tag": "INFO", "category": "RECOVERY"},
            {"id": "evt-7", "time": "04:32:24", "source": "stream_engine", "message": "STREAM RESUMED CLEANLY FROM cp_00482", "tag": "HEALTHY", "category": "STREAM"}
        ]

        # Policies
        self.policies = [
            {"id": "pol-1", "name": "NULL RATE THRESHOLD", "value": "15.00", "unit": "%", "description": "Maximum tolerated null fields per micro-batch before containment trigger.", "current_state": "ACTIVE"},
            {"id": "pol-2", "name": "CARDINALITY MINIMUM", "value": "0.40", "unit": "entropy", "description": "Minimum distinct value diversity across primary partition keys.", "current_state": "ACTIVE"},
            {"id": "pol-3", "name": "VOLUME SPIKE LIMIT", "value": "50.00", "unit": "% / min", "description": "Maximum sudden rate increase over baseline rolling window.", "current_state": "ACTIVE"},
            {"id": "pol-4", "name": "SCHEMA VALIDATION MODE", "value": "STRICT_AVRO", "unit": "mode", "description": "Reject unannounced field additions or type coercions instantly.", "current_state": "ACTIVE"},
            {"id": "pol-5", "name": "CIRCUIT BREAKER AUTO-RESET", "value": "60", "unit": "sec", "description": "Cooldown window before injecting canary half-open micro-batch.", "current_state": "ACTIVE"},
            {"id": "pol-6", "name": "QUARANTINE RETENTION", "value": "30", "unit": "days", "description": "Immutable dead-letter storage retention in MinIO bucket.", "current_state": "ACTIVE"}
        ]

    def add_event(self, source: str, message: str, tag: str = "INFO", category: str = "STREAM"):
        t = datetime.utcnow().strftime("%H:%M:%S")
        evt = {
            "id": f"evt-{len(self.events) + 1}",
            "time": t,
            "source": source,
            "message": message,
            "tag": tag,
            "category": category
        }
        self.events.insert(0, evt)
        if len(self.events) > 100:
            self.events.pop()
        return evt

    def inject_null_spike(self):
        null_guard = next((g for g in self.guards if g["id"] == "null_rate"), None)
        if null_guard:
            null_guard["current"] = 27.41
            null_guard["metric"] = "27.41%"
            null_guard["breaches"] += 1
            null_guard["status"] = "BREACHED"

        self.circuit_breaker["state"] = "OPEN"
        self.circuit_breaker["stream"] = "orders"
        self.circuit_breaker["batch_id"] = "#00483"
        self.circuit_breaker["reason"] = "NULL_RATE_BREACH"
        self.circuit_breaker["current_metric"] = "27.41%"
        self.system_status = "INCIDENT"
        self.active_incident = {
            "id": "INC-00483",
            "time": datetime.utcnow().strftime("%H:%M:%S"),
            "stream": "orders",
            "batch_id": "00483",
            "trigger": "NULL_RATE_BREACH",
            "current_value": "27.41%",
            "threshold": "15.00%",
            "circuit_state": "OPEN",
            "action": "QUARANTINE BATCH",
            "checkpoint": "cp_00482",
            "recovery_status": "READY"
        }

        self.add_event("guard_null_rate", "CRITICAL: NULL RATE BREACH (27.41% > 15.00%) on orders", "ALERT", "GUARD")
        self.add_event("circuit_breaker", "STATE TRANSITION -> OPEN [orders]", "ALERT", "CIRCUIT")
        self.add_event("containment_engine", "BATCH #00483 ISOLATED & QUARANTINED TO MinIO", "ALERT", "QUARANTINE")

    def heal_system(self):
        for g in self.guards:
            g["status"] = "HEALTHY"
            if g["id"] == "null_rate":
                g["current"] = 2.14
                g["metric"] = "2.14%"
            elif g["id"] == "schema":
                g["current"] = 0.00
                g["metric"] = "0 drift"
            elif g["id"] == "volume":
                g["current"] = 3.40
                g["metric"] = "+3.40% / min"

        self.ebpf["socket_latency"] = 3.1
        self.ebpf["packet_loss"] = 0.02
        self.ebpf["retransmits"] = 0.01

        self.circuit_breaker["state"] = "CLOSED"
        self.circuit_breaker["reason"] = "NONE"
        self.system_status = "ONLINE"
        self.active_incident = None

        self.add_event("circuit_breaker", "STATE TRANSITION -> RECOVERED -> CLOSED", "HEALTHY", "CIRCUIT")
        self.add_event("stream_engine", "ALL STREAMS OPERATING NORMALLY. PIPELINE GREEN.", "HEALTHY", "STREAM")

engine = StreamPulseEngine()
