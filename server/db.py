"""
OXYS Real Database Layer (PostgreSQL / SQLite fallback)
Persists:
  - events
  - anomalies
  - schema_versions
  - quarantined_events
  - pipeline_metrics
  - decisions
"""
import os
import time
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from sqlalchemy import (
    create_engine, Column, String, Integer, Float, DateTime, Text, Boolean, MetaData, Table, inspect
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session

logger = logging.getLogger("oxys.server.db")

Base = declarative_base()


class EventRecord(Base):
    __tablename__ = "events"

    event_id = Column(String(64), primary_key=True, index=True)
    source = Column(String(64), index=True)
    schema_version = Column(String(32))
    event_timestamp = Column(String(64))
    ingestion_timestamp = Column(String(64))
    payload_json = Column(Text)
    decision = Column(String(32), default="ALLOW")  # ALLOW, QUARANTINE, BLOCK
    created_at = Column(DateTime, default=datetime.utcnow)


class AnomalyRecord(Base):
    __tablename__ = "anomalies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(64), index=True)
    stream = Column(String(64), index=True)
    detector_id = Column(String(64))
    detector_name = Column(String(128))
    metric_value = Column(Float)
    metric_display = Column(String(64))
    threshold_limit = Column(String(64))
    severity = Column(String(32))  # INFO, WARNING, CRITICAL
    reason = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class SchemaVersionRecord(Base):
    __tablename__ = "schema_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(32), unique=True, index=True)
    source = Column(String(64))
    fields_spec = Column(Text)
    status = Column(String(32), default="ACTIVE")
    registered_at = Column(DateTime, default=datetime.utcnow)


class QuarantinedEventRecord(Base):
    __tablename__ = "quarantined_events"

    batch_id = Column(String(64), primary_key=True, index=True)
    event_id = Column(String(64), index=True)
    stream = Column(String(64), index=True)
    reason = Column(String(128))
    current_val = Column(String(64))
    threshold_val = Column(String(64))
    checkpoint = Column(String(64))
    status = Column(String(32), default="ISOLATED")  # ISOLATED, REPLAYED, RELEASED, PURGED
    storage_path = Column(String(255))
    records_total = Column(Integer, default=1)
    poison_records = Column(Integer, default=1)
    payload_json = Column(Text)
    quarantine_metadata_json = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class DecisionRecord(Base):
    __tablename__ = "decisions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(64), index=True)
    stream = Column(String(64))
    decision = Column(String(32))  # ALLOW, QUARANTINE, BLOCK
    reason = Column(Text)
    circuit_state = Column(String(32))
    latency_ms = Column(Float, default=0.0)
    created_at = Column(DateTime, default=datetime.utcnow)


class PipelineMetricRecord(Base):
    __tablename__ = "pipeline_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    events_processed = Column(Integer, default=0)
    events_allowed = Column(Integer, default=0)
    events_quarantined = Column(Integer, default=0)
    events_blocked = Column(Integer, default=0)
    anomalies_detected = Column(Integer, default=0)
    current_null_rate = Column(Float, default=0.0)
    current_cardinality = Column(Float, default=0.88)
    avg_latency_ms = Column(Float, default=0.0)


class DatabaseManager:
    def __init__(self, db_url: Optional[str] = None):
        import tempfile
        is_serverless = bool(
            os.getenv("VERCEL") or 
            os.getenv("AWS_LAMBDA_FUNCTION_NAME") or 
            os.getenv("SERVERLESS") or
            not os.access(os.path.dirname(__file__), os.W_OK)
        )

        default_url = "sqlite:///./oxys.db"
        if is_serverless:
            tmp_db_path = os.path.join(tempfile.gettempdir(), "oxys.db")
            local_seed = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "oxys.db"))
            if os.path.exists(local_seed) and not os.path.exists(tmp_db_path):
                try:
                    import shutil
                    shutil.copyfile(local_seed, tmp_db_path)
                except Exception:
                    pass
            default_url = f"sqlite:///{tmp_db_path}"

        raw_url = db_url or os.getenv("DATABASE_URL", os.getenv("POSTGRES_URL", default_url))
        
        # Clean postgres scheme for SQLAlchemy
        if raw_url.startswith("postgres://"):
            raw_url = raw_url.replace("postgres://", "postgresql+psycopg2://", 1)
        elif raw_url.startswith("postgresql://") and "+psycopg2" not in raw_url:
            raw_url = raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)

        self.db_url = raw_url
        connect_args = {"check_same_thread": False} if self.db_url.startswith("sqlite") else {}
        
        try:
            self.engine = create_engine(self.db_url, connect_args=connect_args, pool_pre_ping=True)
            Base.metadata.create_all(bind=self.engine)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.info(f"Database initialized with URL: {self.db_url.split('@')[-1] if '@' in self.db_url else self.db_url}")
        except Exception as e:
            logger.warning(f"Could not connect to configured DB ({e}). Falling back to temp / in-memory SQLite database.")
            try:
                self.db_url = f"sqlite:///{os.path.join(tempfile.gettempdir(), 'oxys.db')}" if is_serverless else "sqlite:///./oxys.db"
                self.engine = create_engine(self.db_url, connect_args={"check_same_thread": False})
                Base.metadata.create_all(bind=self.engine)
                self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            except Exception as e2:
                logger.warning(f"File SQLite failed ({e2}). Using in-memory database.")
                from sqlalchemy.pool import StaticPool
                self.db_url = "sqlite:///:memory:"
                self.engine = create_engine(self.db_url, connect_args={"check_same_thread": False}, poolclass=StaticPool)
                Base.metadata.create_all(bind=self.engine)
                self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self) -> Session:
        return self.SessionLocal()

    def record_event(
        self,
        event_id: str,
        source: str,
        payload: Dict[str, Any],
        decision: str,
        schema_version: str = "v1.0.0",
        event_timestamp: Optional[str] = None,
        ingestion_timestamp: Optional[str] = None
    ) -> EventRecord:
        with self.get_session() as session:
            now_dt = datetime.now(timezone.utc)
            rec = EventRecord(
                event_id=event_id,
                source=source,
                schema_version=schema_version,
                event_timestamp=event_timestamp or now_dt.isoformat(),
                ingestion_timestamp=ingestion_timestamp or now_dt.isoformat(),
                payload_json=json.dumps(payload),
                decision=decision,
                created_at=now_dt
            )
            session.merge(rec)
            session.commit()
            return rec

    def get_event_by_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self.get_session() as session:
            r = session.query(EventRecord).filter(EventRecord.event_id == event_id).first()
            if not r:
                return None
            try:
                payload = json.loads(r.payload_json) if r.payload_json else {}
            except Exception:
                payload = {}
            return {
                "event_id": r.event_id,
                "source": r.source,
                "schema_version": r.schema_version,
                "event_timestamp": r.event_timestamp,
                "ingestion_timestamp": r.ingestion_timestamp,
                "decision": r.decision,
                "payload": payload,
                "created_at": r.created_at.isoformat() if r.created_at else None
            }

    def record_anomaly(
        self,
        event_id: str,
        stream: str,
        detector_id: str,
        detector_name: str,
        metric_value: float,
        metric_display: str,
        threshold_limit: str,
        severity: str,
        reason: Optional[str]
    ) -> AnomalyRecord:
        with self.get_session() as session:
            rec = AnomalyRecord(
                event_id=event_id,
                stream=stream,
                detector_id=detector_id,
                detector_name=detector_name,
                metric_value=metric_value,
                metric_display=metric_display,
                threshold_limit=threshold_limit,
                severity=severity,
                reason=reason or ""
            )
            session.add(rec)
            session.commit()
            return rec

    def record_quarantine(
        self,
        batch_id: str,
        event_id: str,
        stream: str,
        reason: str,
        current_val: str,
        threshold_val: str,
        checkpoint: str,
        storage_path: str,
        payload: Dict[str, Any],
        quarantine_meta: Dict[str, Any],
        records_total: int = 1,
        poison_records: int = 1
    ) -> QuarantinedEventRecord:
        with self.get_session() as session:
            rec = QuarantinedEventRecord(
                batch_id=batch_id,
                event_id=event_id,
                stream=stream,
                reason=reason,
                current_val=current_val,
                threshold_val=threshold_val,
                checkpoint=checkpoint,
                status="ISOLATED",
                storage_path=storage_path,
                records_total=records_total,
                poison_records=poison_records,
                payload_json=json.dumps(payload),
                quarantine_metadata_json=json.dumps(quarantine_meta)
            )
            session.merge(rec)
            session.commit()
            return rec

    def record_decision(
        self,
        event_id: str,
        stream: str,
        decision: str,
        reason: Optional[str],
        circuit_state: str,
        latency_ms: float
    ) -> DecisionRecord:
        with self.get_session() as session:
            rec = DecisionRecord(
                event_id=event_id,
                stream=stream,
                decision=decision,
                reason=reason or "",
                circuit_state=circuit_state,
                latency_ms=latency_ms
            )
            session.add(rec)
            session.commit()
            return rec

    def get_metrics_summary(self) -> Dict[str, Any]:
        with self.get_session() as session:
            total_events = session.query(EventRecord).count()
            allowed = session.query(EventRecord).filter(EventRecord.decision == "ALLOW").count()
            quarantined = session.query(QuarantinedEventRecord).count()
            blocked = session.query(EventRecord).filter(EventRecord.decision == "BLOCK").count()
            anomalies = session.query(AnomalyRecord).count()

            recent_anomalies = session.query(AnomalyRecord).order_by(AnomalyRecord.created_at.desc()).limit(10).all()

            return {
                "events_processed": total_events,
                "events_allowed": allowed,
                "events_quarantined": quarantined,
                "events_blocked": blocked,
                "anomaly_count": anomalies,
                "recent_anomalies": [
                    {
                        "id": a.id,
                        "event_id": a.event_id,
                        "stream": a.stream,
                        "detector": a.detector_name,
                        "metric": a.metric_display,
                        "limit": a.threshold_limit,
                        "severity": a.severity,
                        "reason": a.reason,
                        "time": a.created_at.strftime("%H:%M:%S") if a.created_at else ""
                    }
                    for a in recent_anomalies
                ]
            }

    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            records = session.query(EventRecord).order_by(EventRecord.created_at.desc()).limit(limit).all()
            result = []
            for r in records:
                try:
                    payload = json.loads(r.payload_json) if r.payload_json else {}
                except Exception:
                    payload = {}
                result.append({
                    "event_id": r.event_id,
                    "source": r.source,
                    "schema_version": r.schema_version,
                    "event_timestamp": r.event_timestamp,
                    "ingestion_timestamp": r.ingestion_timestamp,
                    "decision": r.decision,
                    "payload": payload
                })
            return result

    def get_quarantined_batches(self) -> List[Dict[str, Any]]:
        with self.get_session() as session:
            records = session.query(QuarantinedEventRecord).order_by(QuarantinedEventRecord.created_at.desc()).all()
            batches = []
            for q in records:
                try:
                    payload = json.loads(q.payload_json) if q.payload_json else {}
                except Exception:
                    payload = {}
                try:
                    meta = json.loads(q.quarantine_metadata_json) if q.quarantine_metadata_json else {}
                except Exception:
                    meta = {}
                
                # Combine payload with metadata for inspector
                payload["_quarantine_meta"] = meta

                batches.append({
                    "batch_id": q.batch_id,
                    "stream": q.stream,
                    "reason": q.reason,
                    "current_val": q.current_val,
                    "threshold_val": q.threshold_val,
                    "checkpoint": q.checkpoint,
                    "status": q.status,
                    "timestamp": q.created_at.strftime("%H:%M:%S UTC") if q.created_at else "",
                    "storage": q.storage_path,
                    "records_total": q.records_total,
                    "poison_records": q.poison_records,
                    "sample_poison": payload,
                    # Frontend UI camelCase compatibility:
                    "batchId": q.batch_id,
                    "currentVal": q.current_val,
                    "thresholdVal": q.threshold_val,
                    "recordsTotal": q.records_total,
                    "poisonRecords": q.poison_records,
                    "samplePoison": payload
                })
            return batches


# Global database instance
db = DatabaseManager()
