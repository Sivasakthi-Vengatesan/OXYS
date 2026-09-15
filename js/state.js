/* ==========================================================================
   STREAMPULSE - CENTRAL STATE STORE & REACTIVE SUBSCRIPTIONS (V2 PRODUCTION)
   ========================================================================== */

class StreamPulseStore {
  constructor() {
    this.state = {
      activeSubsystem: 'control',
      systemStatus: 'ONLINE',
      soundEnabled: true,
      activeIncident: null,
      selectedFilterCategory: 'ALL',
      
      // Live Data Streams
      streams: [
        { id: 'orders', name: 'orders', topic: 'prod.cdc.orders.v2', partitions: 8, rate: 18421, lag: 2381, offset: 4892180, batchId: 483, schemaVer: 'v2.4.0 (Avro)', circuit: 'CLOSED', status: 'RUNNING' },
        { id: 'payments', name: 'payments', topic: 'prod.events.payments.v1', partitions: 12, rate: 12083, lag: 841, offset: 9140221, batchId: 194, schemaVer: 'v1.1.2 (Protobuf)', circuit: 'CLOSED', status: 'RUNNING' },
        { id: 'telemetry', name: 'telemetry', topic: 'infra.ebpf.telemetry', partitions: 16, rate: 8291, lag: 1102, offset: 18491024, batchId: 841, schemaVer: 'v3.0.1 (FlatBuffers)', circuit: 'CLOSED', status: 'RUNNING' },
        { id: 'inventory', name: 'inventory', topic: 'prod.wms.inventory.v1', partitions: 6, rate: 5412, lag: 310, offset: 2314901, batchId: 312, schemaVer: 'v1.0.0 (Avro)', circuit: 'CLOSED', status: 'RUNNING' }
      ],
      selectedStreamId: 'orders',

      // Pipelines
      pipelines: [
        { id: 'orders-production', name: 'orders-production', sourceTopic: 'prod.cdc.orders.v2', processorJob: 'orders-cleaner (Spark 3.5)', guardId: 'orders-guard-inline', sinkDest: 'MinIO Lakehouse + PG meta', state: 'RUNNING', lastExecution: '04:32:18 UTC' },
        { id: 'payments-production', name: 'payments-production', sourceTopic: 'prod.events.payments.v1', processorJob: 'payments-stream-agg (Spark 3.5)', guardId: 'payments-guard-inline', sinkDest: 'MinIO Parquet Lakehouse', state: 'RUNNING', lastExecution: '04:32:15 UTC' },
        { id: 'telemetry-production', name: 'telemetry-production', sourceTopic: 'infra.ebpf.telemetry', processorJob: 'telemetry-stream-collector', guardId: 'infra-guard-inline', sinkDest: 'Prometheus / ClickHouse', state: 'RUNNING', lastExecution: '04:32:10 UTC' }
      ],

      // Data Quality Guards
      guards: [
        { id: 'schema', name: 'SCHEMA INTEGRITY', type: 'STRUCTURAL', current: 0.00, threshold: 0.00, breaches: 0, status: 'HEALTHY', metric: '0 drift', limit: '0 allowed' },
        { id: 'null_rate', name: 'NULL RATE', type: 'QUALITY', current: 2.14, threshold: 15.00, breaches: 0, status: 'HEALTHY', metric: '2.14%', limit: '15.00%' },
        { id: 'cardinality', name: 'CARDINALITY', type: 'ENTROPY', current: 0.88, threshold: 0.40, breaches: 0, status: 'HEALTHY', metric: '0.88 entropy', limit: '> 0.40' },
        { id: 'distribution', name: 'DISTRIBUTION', type: 'STATISTICAL', current: 1.12, threshold: 3.50, breaches: 0, status: 'HEALTHY', metric: '1.12 Z-Score', limit: '< 3.50' },
        { id: 'volume', name: 'VOLUME', type: 'THROUGHPUT', current: 3.40, threshold: 50.00, breaches: 0, status: 'HEALTHY', metric: '+3.40% / min', limit: '< 50.00%' }
      ],

      // Circuit Breaker State Machine
      circuitBreaker: {
        state: 'CLOSED', // CLOSED -> BREACH DETECTED -> OPEN -> QUARANTINE -> HALF-OPEN -> RECOVERED -> CLOSED
        stream: 'orders',
        batchId: '#00483',
        reason: 'NONE',
        currentMetric: '2.14%',
        threshold: '15.00%'
      },

      // Quarantine Isolated Batches
      quarantine: [
        {
          batchId: '00483',
          stream: 'orders',
          reason: 'NULL_RATE_BREACH',
          currentVal: '27.41%',
          thresholdVal: '15.00%',
          checkpoint: 'cp_00482',
          status: 'ISOLATED',
          timestamp: '04:32:18 UTC',
          storage: 's3://streampulse-quarantine/orders/batch_00483.parquet',
          recordsTotal: 18420,
          poisonRecords: 5048,
          samplePoison: {
            order_id: "ord_994821a",
            customer_id: null,
            total_amount: 142.50,
            currency: null,
            tax_code: null,
            created_at: "2026-09-15T04:32:18.112Z",
            _quarantine_meta: {
              flagged_fields: ["customer_id", "currency", "tax_code"],
              rule: "NON_NULLABLE_CONSTRAINT_VIOLATION",
              guard_id: "GUARD_NULL_RATE"
            }
          }
        },
        {
          batchId: '00471',
          stream: 'payments',
          reason: 'SCHEMA_DRIFT',
          currentVal: 'FIELD_TYPE_MISMATCH',
          thresholdVal: 'STRICT_COMPATIBILITY',
          checkpoint: 'cp_00470',
          status: 'ISOLATED',
          timestamp: '03:14:02 UTC',
          storage: 's3://streampulse-quarantine/payments/batch_00471.parquet',
          recordsTotal: 12050,
          poisonRecords: 12050,
          samplePoison: {
            payment_id: "pay_882901",
            amount_cents: "14250",
            merchant_id: "m_alpha_7",
            _quarantine_meta: {
              flagged_fields: ["amount_cents"],
              rule: "SCHEMA_PARSER_TYPE_ERROR (Expected INT64, got STRING)",
              guard_id: "GUARD_SCHEMA_INTEGRITY"
            }
          }
        },
        {
          batchId: '00452',
          stream: 'orders',
          reason: 'VOLUME_BREACH',
          currentVal: '+184.20%',
          thresholdVal: '+50.00%',
          checkpoint: 'cp_00451',
          status: 'ISOLATED',
          timestamp: '01:52:45 UTC',
          storage: 's3://streampulse-quarantine/orders/batch_00452.parquet',
          recordsTotal: 45200,
          poisonRecords: 45200,
          samplePoison: {
            burst_trigger: "RETRY_STORM_DETECTED",
            rate: "45200/s",
            _quarantine_meta: {
              flagged_fields: ["rate"],
              rule: "RATE_BREACH_CONTAINMENT",
              guard_id: "GUARD_VOLUME"
            }
          }
        }
      ],
      inspectingBatchId: null,
      deletingBatchId: null,

      // Recovery State
      recovery: {
        currentCheckpoint: 'cp_00482',
        lastValidBatch: '00482',
        failedBatch: '00483',
        recoveryStatus: 'READY',
        storagePath: 's3://streampulse-checkpoints/prod/orders/cp_00482.chk',
        history: [
          { time: '04:32:18', stream: 'orders', checkpoint: 'cp_00482', action: 'BREACH', result: 'FAILED' },
          { time: '04:32:19', stream: 'orders', checkpoint: 'cp_00482', action: 'QUARANTINE', result: 'SUCCESS' },
          { time: '04:32:24', stream: 'orders', checkpoint: 'cp_00482', action: 'RESUME', result: 'SUCCESS' }
        ]
      },

      // Infrastructure Telemetry
      telemetry: {
        ebpf: {
          cpu: 42.4,
          memory: 61.8,
          networkRx: 82.4,
          networkTx: 45.1,
          packetLoss: 0.02,
          retransmits: 0.01,
          socketLatency: 3.1
        },
        dataQuality: {
          nullRate: 2.14,
          schemaStatus: 'VERIFIED',
          volumeDelta: '+4.2%',
          cardinalityEntropy: 0.88,
          zScoreDrift: 1.12,
          deadLetterCount: 3
        }
      },

      // System Event Stream Logs
      eventLogs: [
        { id: 'evt-1', time: '04:31:02', source: 'batch_00481', message: 'SPARK MICRO-BATCH PROCESSED (18,410 records)', tag: 'HEALTHY', category: 'STREAM' },
        { id: 'evt-2', time: '04:32:03', source: 'batch_00482', message: 'SPARK MICRO-BATCH PROCESSED (18,421 records)', tag: 'HEALTHY', category: 'STREAM' },
        { id: 'evt-3', time: '04:32:18', source: 'guard_null_rate', message: 'NULL_RATE_BREACH (27.41% > 15.00% threshold)', tag: 'ALERT', category: 'GUARD' },
        { id: 'evt-4', time: '04:32:18', source: 'circuit_breaker', message: 'STATE TRANSITION -> OPEN [orders]', tag: 'ALERT', category: 'CIRCUIT' },
        { id: 'evt-5', time: '04:32:19', source: 'batch_00483', message: 'POISON BATCH ISOLATED TO MinIO QUARANTINE VAULT', tag: 'ALERT', category: 'QUARANTINE' },
        { id: 'evt-6', time: '04:32:21', source: 'recovery_manager', message: 'RECOVERY CHECKPOINT IDENTIFIED -> cp_00482', tag: 'INFO', category: 'RECOVERY' },
        { id: 'evt-7', time: '04:32:24', source: 'stream_engine', message: 'STREAM RESUMED CLEANLY FROM cp_00482', tag: 'HEALTHY', category: 'STREAM' }
      ],

      // Configuration Policies
      policies: [
        { id: 'pol-1', name: 'NULL RATE THRESHOLD', value: '15.00', unit: '%', description: 'Maximum tolerated null fields per micro-batch before containment trigger.', currentState: 'ACTIVE' },
        { id: 'pol-2', name: 'CARDINALITY MINIMUM', value: '0.40', unit: 'entropy', description: 'Minimum distinct value diversity across primary partition keys.', currentState: 'ACTIVE' },
        { id: 'pol-3', name: 'VOLUME SPIKE LIMIT', value: '50.00', unit: '% / min', description: 'Maximum sudden rate increase over baseline rolling window.', currentState: 'ACTIVE' },
        { id: 'pol-4', name: 'SCHEMA VALIDATION MODE', value: 'STRICT_AVRO', unit: 'mode', description: 'Reject unannounced field additions or type coercions instantly.', currentState: 'ACTIVE' },
        { id: 'pol-5', name: 'CIRCUIT BREAKER AUTO-RESET', value: '60', unit: 'sec', description: 'Cooldown window before injecting canary half-open micro-batch.', currentState: 'ACTIVE' },
        { id: 'pol-6', name: 'QUARANTINE RETENTION', value: '30', unit: 'days', description: 'Immutable dead-letter storage retention in MinIO bucket.', currentState: 'ACTIVE' }
      ]
    };

    this.listeners = [];
  }

  getState() {
    return this.state;
  }

  subscribe(fn) {
    this.listeners.push(fn);
    return () => {
      this.listeners = this.listeners.filter(l => l !== fn);
    };
  }

  notify() {
    for (const fn of this.listeners) {
      fn(this.state);
    }
  }

  setSubsystem(subsystemId) {
    this.state.activeSubsystem = subsystemId;
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.notify();
  }

  setFilterCategory(category) {
    this.state.selectedFilterCategory = category;
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.notify();
  }

  addEventLog(source, message, tag = 'INFO', category = 'STREAM') {
    const d = new Date();
    const time = `${String(d.getUTCHours()).padStart(2,'0')}:${String(d.getUTCMinutes()).padStart(2,'0')}:${String(d.getUTCSeconds()).padStart(2,'0')}`;
    const id = `evt-${Date.now()}`;
    this.state.eventLogs.unshift({ id, time, source, message, tag, category });
    if (this.state.eventLogs.length > 100) this.state.eventLogs.pop();
    this.notify();
  }

  selectStream(streamId) {
    this.state.selectedStreamId = streamId;
    this.notify();
  }

  setCircuitState(newState, reason = null) {
    this.state.circuitBreaker.state = newState;
    if (reason) this.state.circuitBreaker.reason = reason;
    this.notify();
  }

  triggerIncident(incident) {
    this.state.activeIncident = incident;
    this.state.systemStatus = 'INCIDENT';
    if (window.RetroAudio) window.RetroAudio.playAlarm();
    this.notify();
  }

  clearIncident() {
    this.state.activeIncident = null;
    this.state.systemStatus = 'ONLINE';
    this.notify();
  }

  updatePolicyValue(policyId, newValue) {
    const pol = this.state.policies.find(p => p.id === policyId);
    if (pol) {
      pol.value = newValue;
      this.addEventLog('config_manager', `POLICY UPDATED: ${pol.name} -> ${newValue} ${pol.unit}`, 'CONFIG', 'GUARD');
      this.notify();
    }
  }
}

window.Store = new StreamPulseStore();
