/* ==========================================================================
   OXYS - CENTRAL STATE STORE & REACTIVE SUBSCRIPTIONS
   ========================================================================== */

class OxysStore {
  constructor() {
    this.state = {
      activeSubsystem: 'control',
      systemStatus: 'ONLINE',
      soundEnabled: true,
      activeIncident: null,
      selectedFilterCategory: 'ALL',
      
      // Live Data Streams (Crypto & Weather real data feeds)
      streams: [
        { id: 'crypto_stream', name: 'crypto_market_stream', topic: 'oxys.normalized', partitions: 8, rate: 120, lag: 42, offset: 50120, batchId: 483, schemaVer: 'v1.0.0 (JSON-Avro)', circuit: 'CLOSED', status: 'RUNNING' },
        { id: 'weather_stream', name: 'open_meteo_telemetry_stream', topic: 'oxys.normalized', partitions: 6, rate: 50, lag: 15, offset: 20050, batchId: 483, schemaVer: 'v1.0.0 (JSON)', circuit: 'CLOSED', status: 'RUNNING' }
      ],
      selectedStreamId: 'crypto_stream',

      // Pipelines
      pipelines: [
        { id: 'crypto-market-pipeline', name: 'crypto-market-pipeline', sourceTopic: 'oxys.normalized', processorJob: 'oxys-spark-structured-streaming (Spark 3.5)', guardId: 'oxys-integrity-guard-inline', sinkDest: 'MinIO Lakehouse + PostgreSQL', state: 'RUNNING', lastExecution: '04:32:18 UTC' },
        { id: 'weather-telemetry-pipeline', name: 'weather-telemetry-pipeline', sourceTopic: 'oxys.normalized', processorJob: 'oxys-spark-structured-streaming (Spark 3.5)', guardId: 'oxys-integrity-guard-inline', sinkDest: 'MinIO Lakehouse + PostgreSQL', state: 'RUNNING', lastExecution: '04:32:15 UTC' }
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
        stream: 'crypto_market_stream',
        batchId: '#00483',
        reason: 'NONE',
        currentMetric: '2.14%',
        threshold: '15.00%'
      },

      // Quarantine Isolated Batches
      quarantine: [],
      inspectingBatchId: null,
      deletingBatchId: null,

      // Recovery State
      recovery: {
        currentCheckpoint: 'cp_00482',
        lastValidBatch: '00482',
        failedBatch: '00483',
        recoveryStatus: 'SYNCHRONIZED',
        storagePath: 's3://oxys-checkpoints/prod/crypto/cp_00482.chk',
        history: [
          { time: '04:32:18', stream: 'crypto_market_stream', checkpoint: 'cp_00482', action: 'BREACH', result: 'FAILED' },
          { time: '04:32:19', stream: 'crypto_market_stream', checkpoint: 'cp_00482', action: 'QUARANTINE', result: 'SUCCESS' },
          { time: '04:32:24', stream: 'crypto_market_stream', checkpoint: 'cp_00482', action: 'RESUME', result: 'SUCCESS' }
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
          deadLetterCount: 0
        }
      },

      // System Event Stream Logs
      eventLogs: [],

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

window.Store = new OxysStore();
