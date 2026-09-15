# STREAMPULSE // Real-Time Data Reliability & Containment Console

> **DETECT THE BREACH. ISOLATE THE POISON. RESUME THE STREAM.**

StreamPulse is a **reactive real-time data reliability and containment engine** for modern streaming data pipelines (Apache Kafka, Apache Spark Structured Streaming, MinIO / S3, PostgreSQL, and eBPF infrastructure telemetry).

The interface is styled with a **soft-brutalist 80s pastel terminal aesthetic** (`#e8d5f2` lavender, `#ffd89b` peach, `#a8e6cf` mint, `#ffb8a1` coral, `#4a3b52` deep purple, `Press Start 2P`, `VT323`, 0px border-radius, hard stacked shadows, CRT scanlines) coupled with a distributed data reliability control room.

---

## 1. System Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        STREAMPULSE CORE PLANE                          │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│   KAFKA  ──►  SPARK STREAMING  ──►  STREAMPULSE GUARD  ──►  DECISION   │
│                                                               │        │
│                                                ┌──────────────┴────┐   │
│                                                ▼                   ▼   │
│                                            CLEAN SINK          QUARANTINE
│                                                │                   │   │
│                                         MINIO / POSTGRES       MINIO DLQ
│                                                                    │   │
│                                                           CHECKPOINT RECOVERY
│                                                             (cp_00482) │
└────────────────────────────────────────────────────────────────────────┘
```

Separately:

```
eBPF KERNEL PROBES  ──►  INFRASTRUCTURE TELEMETRY  ──►  CORRELATION ENGINE
                                                              │
                                            DATA QUALITY VS INFRASTRUCTURE
```

---

## 2. Core Subsystems

1. **`[01] CONTROL`**: Live streaming DAG with clickable node diagnostics, summary metric cards, anomaly injector panel, and reactive incident warning banner.
2. **`[02] STREAMS`**: Kafka stream monitor table (`orders`, `payments`, `telemetry`, `inventory`), partition leader distribution drawer, consumer lag meters.
3. **`[03] PIPELINES`**: Configured Spark micro-batch pipelines and sink destinations.
4. **`[04] SCHEMA GUARD`**: Side-by-side expected vs received schema diff highlighting missing fields (`status -> missing`) and unexpected additions (`state -> unexpected`).
5. **`[05] NULL RATE GUARD`**: Historical progression chart and breach threshold tracker (`15.00%`).
6. **`[06] CARDINALITY GUARD`**: Partition key diversity Shannon entropy meter.
7. **`[07] DISTRIBUTION GUARD`**: Numerical Z-score drift histogram analysis.
8. **`[08] VOLUME GUARD`**: Real-time surge / retry-storm containment.
9. **`[09] CIRCUIT BREAKER`**: 6-step kinetic Finite State Machine (`CLOSED` $\rightarrow$ `BREACH DETECTED` $\rightarrow$ `OPEN` $\rightarrow$ `QUARANTINE` $\rightarrow$ `HALF-OPEN` $\rightarrow$ `RECOVERED` $\rightarrow$ `CLOSED`).
10. **`[10] QUARANTINE`**: Quarantined batch vault with raw JSON diff inspection, sanitized replay, and permanent audit purge with confirmation modal.
11. **`[12] TELEMETRY`**: eBPF Infrastructure Telemetry (CPU, memory, packet loss, retransmits, socket RTT) correlated side-by-side with Application Data Health.
12. **`[13] EVENTS`**: Filterable live system logs (`ALL`, `STREAM`, `GUARD`, `CIRCUIT`, `QUARANTINE`, `RECOVERY`, `INFRASTRUCTURE`) with Incident Drilldown modal.
13. **`[14] POLICIES`**: Retro terminal configuration utility with live save actions.
14. **`[15] SETTINGS`**: Broker configurations, storage paths, and probe intervals.

---

## 3. Quickstart & Local Setup

### Prerequisites
- Python 3.10+
- `pip install fastapi uvicorn pydantic`

### Running the Application

1. **Start the FastAPI Control Plane Backend**:
   ```bash
   python -m uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **Open the Console in your Browser**:
   ```
   http://localhost:8000
   ```

3. **Interact via the Terminal REPL**:
   - `help` — List all available commands
   - `status` — Print system health & active streams
   - `inject null` — Simulate a critical null-rate spike and observe automated quarantine
   - `heal` — Restore clean checkpoints and close the circuit breaker

---

## 4. API Specification

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/system/status` | System health, connected brokers, and open incidents |
| `GET` | `/api/streams` | List of active Kafka streams, rates, lags, and schema versions |
| `GET` | `/api/guards` | 5 active data-quality assertion guards and limits |
| `GET` | `/api/circuit` | Circuit breaker state machine status |
| `POST` | `/api/circuit/{action}` | Trip or reset circuit breaker (`trip`, `reset`, `half-open`) |
| `GET` | `/api/quarantine` | List of isolated batches in MinIO vault |
| `POST` | `/api/quarantine/{id}/replay` | Replay poisoned batch with sanitized schema filter |
| `POST` | `/api/quarantine/{id}/delete` | Permanent audit purge from vault |
| `GET` | `/api/recovery` | Checkpoint rollback history and recovery targets |
| `GET` | `/api/telemetry` | Side-by-side eBPF kernel probes vs data health |
| `POST` | `/api/telemetry/correlate` | Root-cause correlation diagnostic engine |
| `GET` | `/api/events` | Filterable event stream logs |
| `WS` | `/ws/events` | Real-time WebSocket event & telemetry broadcast |

---

## 5. Design System

- **Palette**: Lavender (`#e8d5f2`), Peach (`#ffd89b`), Mint (`#a8e6cf`), Coral (`#ffb8a1`), Deep Purple (`#4a3b52`).
- **Typography**: Google Fonts `Press Start 2P` (headings, badges, labels) and `VT323` (metrics, logs, data).
- **Geometry**: Strict `0px` border-radius with hard 3D stacked shadows.

---

## 6. License
MIT License. Built for distributed data reliability.
