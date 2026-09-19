/* ==========================================================================
   OXYS - PIPELINE DAG INTERACTION & NODE INSPECTOR
   ========================================================================== */

const DAG_NODE_DETAILS = {
  kafka: {
    title: 'NODE // KAFKA DISTRIBUTED TOPIC LOG',
    type: 'INGESTION BROKER',
    status: 'HEALTHY',
    metrics: [
      { label: 'TOPICS SUBSCRIBED', value: '03 TOPICS (oxys.raw, oxys.normalized, oxys.quarantine)' },
      { label: 'TOTAL PARTITIONS', value: '14 PARTITIONS (Replication Factor: 1+)' },
      { label: 'CONSUMER GROUP', value: 'oxys-spark-cg' },
      { label: 'INGESTION SOURCES', value: 'Binance Crypto Stream + Open-Meteo Weather' },
      { label: 'DELIVERY GUARANTEE', value: 'At-Least-Once with Deterministic Event IDs' }
    ],
    description: 'Persistent distributed commit log acting as upstream buffer between real public data sources and the OXYS streaming processor.'
  },
  spark: {
    title: 'NODE // SPARK STRUCTURED STREAMING',
    type: 'MICRO-BATCH RUNTIME',
    status: 'HEALTHY',
    metrics: [
      { label: 'MICRO-BATCH INTERVAL', value: '500ms (Continuous Trigger)' },
      { label: 'CURRENT BATCH ID', value: 'BATCH #00483' },
      { label: 'WATERMARK DELAY', value: '1.2 SECONDS' },
      { label: 'ACTIVE EXECUTORS', value: 'DISTRIBUTED SPARK WORKERS' },
      { label: 'STATE STORE BACKEND', value: 'State Store Checkpoint Manager' },
      { label: 'CHECKPOINT DIR', value: 's3://oxys-checkpoints/prod/crypto/' }
    ],
    description: 'Continuous incremental computation engine applying stateful micro-batching, parsing, and real-time schema validation.'
  },
  guard: {
    title: 'NODE // OXYS INTEGRITY GUARD ENGINE',
    type: 'INLINE ASSERTION ENGINE',
    status: 'ACTIVE',
    metrics: [
      { label: 'ACTIVE GUARDS', value: '05 DETECTORS ENABLED' },
      { label: 'SCHEMA INTEGRITY', value: 'Strict Type & Field Evolution Enforcement' },
      { label: 'NULL RATE THRESHOLD', value: '15.00% MAX TOLERANCE' },
      { label: 'CARDINALITY MIN', value: '0.40 ENTROPY THRESHOLD' },
      { label: 'INLINE LATENCY OVERHEAD', value: '1.2ms AVG / BATCH' },
      { label: 'CIRCUIT BREAKER HOOK', value: 'ENABLED (ALLOW / QUARANTINE / BLOCK)' }
    ],
    description: 'Sub-millisecond inline data-quality assertion engine evaluating schema drift, null rates, cardinality diversity, Z-score distributions, and duplicate event IDs.'
  },
  clean: {
    title: 'NODE // ALLOW / CLEAN SINK BRANCH',
    type: 'VALIDATED DATA PIPELINE',
    status: 'PASS',
    metrics: [
      { label: 'RECORD HEALTH', value: '100% INVARIANT PASS' },
      { label: 'DESTINATION SINK', value: 'MinIO Lakehouse + PostgreSQL Events' },
      { label: 'TRANSACTION SEMANTICS', value: 'Atomic DB / Object Commit' },
      { label: 'ROUTING DECISION', value: 'ALLOW' }
    ],
    description: 'Clean batch partition verified by all 5 active guards, committed directly to primary analytics lakehouse and PostgreSQL OLTP metadata tables.'
  },
  minio: {
    title: 'NODE // MINIO & POSTGRESQL STORAGE',
    type: 'STORAGE & METADATA SINK',
    status: 'PERSISTED',
    metrics: [
      { label: 'LAKEHOUSE BUCKET', value: 's3://oxys-lakehouse/' },
      { label: 'QUARANTINE BUCKET', value: 's3://oxys-quarantine/' },
      { label: 'POSTGRESQL DB', value: 'oxys database (events, anomalies, metrics)' },
      { label: 'STORAGE STATUS', value: 'ONLINE & SYNCHRONIZED' }
    ],
    description: 'Object storage for raw and quarantined JSON/Parquet batches coupled with PostgreSQL tables tracking events, anomalies, and pipeline metrics.'
  },
  poison: {
    title: 'NODE // QUARANTINE / ANOMALY BRANCH',
    type: 'CONTAINMENT PIPELINE',
    status: 'ISOLATED',
    metrics: [
      { label: 'TRIGGER CAUSE', value: 'GUARD BREACH DETECTED' },
      { label: 'CIRCUIT BREAKER STATE', value: 'QUARANTINE / OPEN' },
      { label: 'CONTAINMENT ACTION', value: 'ISOLATED TO MinIO DEAD-LETTER VAULT' },
      { label: 'DOWNSTREAM TAINT', value: '0.00% (CONTAINED)' }
    ],
    description: 'Dedicated isolated routing lane for poisoned or anomalous batches, preventing corrupted records from entering production storage sinks.'
  },
  quarantine: {
    title: 'NODE // QUARANTINE DEAD-LETTER VAULT',
    type: 'CONTAINMENT VAULT',
    status: 'ALERT',
    metrics: [
      { label: 'VAULT STORAGE', value: 's3://oxys-quarantine/batches/' },
      { label: 'RETENTION PERIOD', value: '30 DAYS (Immutable Audit Log)' },
      { label: 'CHECKPOINT ROLLBACK', value: 'cp_00482 Restored' },
      { label: 'REPLAY CAPABILITY', value: 'Ready for Manual / Auto Schema Remediation' }
    ],
    description: 'Secure containment storage keeping poisoned records segregated with complete metadata, column violation diffs, and replay triggers.'
  }
};

class PipelineDagController {
  constructor() {
    this.modalEl = document.getElementById('dag-inspector-modal');
  }

  inspectNode(nodeKey) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const info = DAG_NODE_DETAILS[nodeKey];
    if (!info) return;

    const modalTitle = document.getElementById('modal-node-title');
    const modalContent = document.getElementById('modal-node-content');
    if (!modalTitle || !modalContent) return;

    modalTitle.textContent = info.title;

    let html = `
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; border-bottom:2px dashed var(--deep-purple); padding-bottom:8px;">
        <span style="font-family:var(--font-heading); font-size:10px;">TYPE: ${info.type}</span>
        <span class="status-badge ${info.status === 'HEALTHY' || info.status === 'PASS' ? 'badge-mint' : 'badge-coral'}">${info.status}</span>
      </div>
      
      <div style="margin-bottom:16px; font-size:19px; line-height:1.4;">
        ${info.description}
      </div>

      <div style="background:var(--panel-peach); border:2px solid var(--deep-purple); padding:12px; margin-bottom:16px;">
        <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:8px; border-bottom:1px solid var(--deep-purple); padding-bottom:4px;">DIAGNOSTIC METRICS</div>
        <div style="display:flex; flex-direction:column; gap:6px;">
          ${info.metrics.map(m => `
            <div style="display:flex; justify-content:space-between; border-bottom:1px dashed rgba(74,59,82,0.3); padding-bottom:2px;">
              <span style="font-family:var(--font-heading); font-size:9px;">${m.label}:</span>
              <span style="font-weight:bold;">${m.value}</span>
            </div>
          `).join('')}
        </div>
      </div>

      <div style="display:flex; justify-content:flex-end; gap:10px;">
        <button class="btn-brutalist btn-sm btn-dark" onclick="window.DAG.closeModal()">[ CLOSE INSPECTOR ]</button>
      </div>
    `;

    modalContent.innerHTML = html;
    this.modalEl.classList.remove('hidden');
  }

  closeModal() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    if (this.modalEl) this.modalEl.classList.add('hidden');
  }
}

window.DAG = new PipelineDagController();
