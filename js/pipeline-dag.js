/* ==========================================================================
   STREAMPULSE - PIPELINE DAG INTERACTION & NODE INSPECTOR
   ========================================================================== */

const DAG_NODE_DETAILS = {
  kafka: {
    title: 'NODE // KAFKA DISTRIBUTED LOG',
    type: 'INGESTION ENGINE',
    status: 'HEALTHY',
    metrics: [
      { label: 'TOPICS SUBSCRIBED', value: '04 TOPICS (orders, payments, telemetry, inventory)' },
      { label: 'TOTAL PARTITIONS', value: '42 PARTITIONS (Replication Factor: 3)' },
      { label: 'CONSUMER GROUP', value: 'spark-streampulse-orders-cg' },
      { label: 'INGESTION RATE', value: '44,207 MSGS/SEC' },
      { label: 'LEADER EPOCH', value: 'EPOCH #142' },
      { label: 'MIN INSYNC REPLICAS', value: '2 (ACK = ALL)' }
    ],
    description: 'Persistent distributed commit log acting as upstream buffer. Zero data loss guarantee with strict ordering per partition key.'
  },
  spark: {
    title: 'NODE // SPARK STRUCTURED STREAMING',
    type: 'MICRO-BATCH RUNTIME',
    status: 'HEALTHY',
    metrics: [
      { label: 'MICRO-BATCH INTERVAL', value: '500ms (ProcessingTime Trigger)' },
      { label: 'CURRENT BATCH ID', value: 'BATCH #00483' },
      { label: 'WATERMARK DELAY', value: '2.4 SECONDS' },
      { label: 'ACTIVE EXECUTORS', value: '8 NODES (64 CORES / 256GB RAM)' },
      { label: 'STATE STORE BACKEND', value: 'RocksDB State Store Provider' },
      { label: 'CHECKPOINT DIR', value: 's3://streampulse-checkpoints/prod/orders/' }
    ],
    description: 'Continuous incremental computation engine applying stateful windowing, deduplication, and schema validation per micro-batch.'
  },
  guard: {
    title: 'NODE // STREAMPULSE GUARD ENGINE',
    type: 'INLINE RELIABILITY GUARD',
    status: 'ACTIVE',
    metrics: [
      { label: 'ACTIVE GUARDS', value: '05 GUARDS ENABLED' },
      { label: 'SCHEMA INTEGRITY', value: 'Avro 1.11.1 / Strict Drift Containment' },
      { label: 'NULL RATE THRESHOLD', value: '15.00% MAX ALLOWED' },
      { label: 'CARDINALITY MIN', value: '0.40 ENTROPY THRESHOLD' },
      { label: 'INLINE LATENCY OVERHEAD', value: '1.4ms AVG / BATCH' },
      { label: 'CIRCUIT BREAKER HOOK', value: 'ENABLED (Automated Trip & Quarantine)' }
    ],
    description: 'Sub-millisecond inline data-quality assertion engine evaluating column nullability, statistical distributions, and schema evolution before sink commit.'
  },
  clean: {
    title: 'NODE // CLEAN STREAM BRANCH',
    type: 'VALIDATED DATA PIPELINE',
    status: 'PASS',
    metrics: [
      { label: 'RECORD HEALTH', value: '100% ASSERTION PASS' },
      { label: 'DESTINATION SINK', value: 'MinIO Parquet Lake + PG Metadata Store' },
      { label: 'TRANSACTION SEMANTICS', value: 'Exactly-Once with 2PC' },
      { label: 'COMPACTION CYCLE', value: 'Auto-Compacted every 15 mins' }
    ],
    description: 'Clean batch partition verified by all 5 active guards, committed directly to primary analytics lakehouse and OLTP metadata tables.'
  },
  minio: {
    title: 'NODE // MINIO & POSTGRESQL STORAGE',
    type: 'STORAGE & METADATA SINK',
    status: 'PERSISTED',
    metrics: [
      { label: 'BUCKET', value: 's3://streampulse-lakehouse-prod/' },
      { label: 'PARTITION SCHEME', value: 'year=2026/month=09/day=15/hour=04/' },
      { label: 'POSTGRESQL DB', value: 'streampulse_prod_meta (Port 5432)' },
      { label: 'WRITE THROUGHPUT', value: '78.4 MB/S' }
    ],
    description: 'Object storage for columnar Parquet datasets coupled with ACID metadata table logs tracking offset checkpoints.'
  },
  poison: {
    title: 'NODE // POISON DATA BRANCH',
    type: 'CONTAINMENT PIPELINE',
    status: 'ISOLATED',
    metrics: [
      { label: 'TRIGGER CAUSE', value: 'GUARD BREACH DETECTED' },
      { label: 'CIRCUIT BREAKER STATE', value: 'AUTOMATED TRIP' },
      { label: 'CORRUPTED RECORDS', value: 'ISOLATED WITHOUT IMPACTING CLEAN COMMITS' },
      { label: 'DOWNSTREAM TAINT', value: '0.00% (CONTAINED)' }
    ],
    description: 'Dedicated isolated routing lane for poisoned batches, preventing corrupted records from entering production storage sinks.'
  },
  quarantine: {
    title: 'NODE // QUARANTINE DEAD-LETTER VAULT',
    type: 'CONTAINMENT VAULT',
    status: 'ALERT',
    metrics: [
      { label: 'VAULT STORAGE', value: 's3://streampulse-quarantine/batches/' },
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
