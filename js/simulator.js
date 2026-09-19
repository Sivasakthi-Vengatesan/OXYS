/* ==========================================================================
   OXYS - CONTROLLED FAILURE INJECTOR & TESTING HARNESS
   ========================================================================== */

class OxysFailureInjector {
  constructor(store) {
    this.store = store;
  }

  async injectNullSpike() {
    try {
      const resp = await fetch('/api/simulation/inject/null', { method: 'POST' });
      if (resp.ok) {
        if (window.RetroAudio) window.RetroAudio.playAlarm();
        return;
      }
    } catch (e) {}

    // Fallback local simulation
    const s = this.store.getState();
    const nullGuard = s.guards.find(g => g.id === 'null_rate');
    if (!nullGuard) return;

    nullGuard.current = 27.41;
    nullGuard.metric = '27.41%';
    nullGuard.breaches += 1;
    nullGuard.status = 'BREACHED';

    s.circuitBreaker.currentMetric = '27.41%';
    s.circuitBreaker.stream = 'crypto_market_stream';
    s.circuitBreaker.batchId = '#00483';
    s.circuitBreaker.reason = 'NULL_RATE_BREACH';

    this.store.setCircuitState('BREACH DETECTED', 'NULL_RATE_BREACH');
    this.store.addEventLog('guard_null_rate', 'CRITICAL BREACH: NULL RATE SPIKE (27.41% > 15.00%)', 'ALERT');
    this.store.addEventLog('circuit_breaker', 'STATE TRANSITION -> BREACH DETECTED -> OPEN', 'ALERT');

    setTimeout(() => {
      this.store.setCircuitState('OPEN', 'NULL_RATE_BREACH');
      this.store.addEventLog('containment_engine', 'BATCH #00483 ISOLATED & QUARANTINED TO MinIO', 'ALERT');
      this.store.triggerIncident({
        id: 'INC-00483',
        stream: 'crypto_market_stream',
        batchId: '00483',
        type: 'NULL_RATE_BREACH',
        current: '27.41%',
        threshold: '15.00%',
        action: 'BATCH QUARANTINED'
      });
    }, 600);
  }

  async injectSchemaDrift() {
    try {
      const resp = await fetch('/api/simulation/inject/schema', { method: 'POST' });
      if (resp.ok) {
        if (window.RetroAudio) window.RetroAudio.playAlarm();
        return;
      }
    } catch (e) {}

    const s = this.store.getState();
    const schemaGuard = s.guards.find(g => g.id === 'schema');
    if (!schemaGuard) return;

    schemaGuard.current = 1.00;
    schemaGuard.metric = 'FIELD TYPE MISMATCH (last_price: String != Float64)';
    schemaGuard.breaches += 1;
    schemaGuard.status = 'BREACHED';

    s.circuitBreaker.currentMetric = 'SCHEMA MISMATCH';
    s.circuitBreaker.stream = 'crypto_market_stream';
    s.circuitBreaker.batchId = '#00471';
    s.circuitBreaker.reason = 'SCHEMA_DRIFT';

    this.store.setCircuitState('OPEN', 'SCHEMA_DRIFT');
    this.store.addEventLog('guard_schema', 'CRITICAL: SCHEMA DRIFT DETECTED on crypto_market_stream', 'ALERT');
    this.store.triggerIncident({
      id: 'INC-00471',
      stream: 'crypto_market_stream',
      batchId: '00471',
      type: 'SCHEMA_DRIFT',
      current: 'INVALID TYPE',
      threshold: 'STRICT',
      action: 'BATCH QUARANTINED'
    });
  }

  async injectDuplicateEvent() {
    try {
      const resp = await fetch('/api/simulation/inject/duplicate', { method: 'POST' });
      if (resp.ok) {
        if (window.RetroAudio) window.RetroAudio.playAlarm();
        return;
      }
    } catch (e) {}

    this.store.addEventLog('integrity_guard', 'INTEGRITY VIOLATION: REPLAYED EVENT ID BLOCKED', 'ALERT', 'GUARD');
    this.store.notify();
  }

  async injectVolumeBreach() {
    try {
      const resp = await fetch('/api/simulation/inject/volume', { method: 'POST' });
      if (resp.ok) {
        if (window.RetroAudio) window.RetroAudio.playAlarm();
        return;
      }
    } catch (e) {}

    const s = this.store.getState();
    const volGuard = s.guards.find(g => g.id === 'volume');
    if (!volGuard) return;

    volGuard.current = 184.20;
    volGuard.metric = '+184.20% / min (SURGE)';
    volGuard.breaches += 1;
    volGuard.status = 'BREACHED';

    s.circuitBreaker.stream = 'crypto_market_stream';
    s.circuitBreaker.batchId = '#00452';
    s.circuitBreaker.reason = 'VOLUME_BREACH';

    this.store.setCircuitState('OPEN', 'VOLUME_BREACH');
    this.store.addEventLog('guard_volume', 'RATE BREACH: +184.20% SURGE (Threshold: +50.00%)', 'ALERT');
    this.store.triggerIncident({
      id: 'INC-00452',
      stream: 'crypto_market_stream',
      batchId: '00452',
      type: 'VOLUME_BREACH',
      current: '+184.20%',
      threshold: '+50.00%',
      action: 'RATE CONTAINED'
    });
  }

  async injectNetworkDegradation() {
    try {
      const resp = await fetch('/api/simulation/inject/network', { method: 'POST' });
      if (resp.ok) return;
    } catch (e) {}

    const s = this.store.getState();
    s.telemetry.ebpf.socketLatency = 88.4;
    s.telemetry.ebpf.packetLoss = 4.20;
    s.telemetry.ebpf.retransmits = 3.15;
    
    this.store.addEventLog('ebpf_kernel', 'eBPF SIGNAL: HIGH SOCKET LATENCY (88.4ms) & 4.2% PACKET LOSS', 'ALERT');
    this.store.notify();
  }

  async healSystem() {
    try {
      const resp = await fetch('/api/simulation/heal', { method: 'POST' });
      if (resp.ok) {
        if (window.RetroAudio) window.RetroAudio.playRecover();
        return;
      }
    } catch (e) {}

    const s = this.store.getState();
    s.guards.forEach(g => {
      g.status = 'HEALTHY';
      if (g.id === 'null_rate') { g.current = 2.14; g.metric = '2.14%'; }
      if (g.id === 'schema') { g.current = 0.00; g.metric = '0 drift'; }
      if (g.id === 'volume') { g.current = 3.40; g.metric = '+3.40% / min'; }
    });

    s.telemetry.ebpf.socketLatency = 3.1;
    s.telemetry.ebpf.packetLoss = 0.02;
    s.telemetry.ebpf.retransmits = 0.01;

    this.store.setCircuitState('HALF-OPEN', 'TESTING_PROBE');
    this.store.addEventLog('circuit_breaker', 'STATE TRANSITION -> HALF-OPEN (Canary Probe Batch #00484)', 'INFO');

    setTimeout(() => {
      this.store.setCircuitState('RECOVERED', 'RELIABILITY_RESTORED');
      this.store.addEventLog('circuit_breaker', 'STATE TRANSITION -> RECOVERED (Probe Verified Clean)', 'HEALTHY');
      
      setTimeout(() => {
        this.store.setCircuitState('CLOSED', 'NONE');
        this.store.clearIncident();
        this.store.addEventLog('stream_engine', 'STREAMS FULLY OPERATIONAL. CIRCUIT CLOSED.', 'HEALTHY');
        if (window.RetroAudio) window.RetroAudio.playRecover();
      }, 700);
    }, 700);
  }

  start() {}
  stop() {}
}

window.Simulator = new OxysFailureInjector(window.Store);
