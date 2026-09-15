/* ==========================================================================
   STREAMPULSE - REAL-TIME STREAMING SIMULATOR & ANOMALY INJECTOR
   ========================================================================== */

class StreamPulseSimulator {
  constructor(store) {
    this.store = store;
    this.timer = null;
    this.isRunning = true;
    this.tickIntervalMs = 1000;
  }

  start() {
    if (this.timer) clearInterval(this.timer);
    this.timer = setInterval(() => this.tick(), this.tickIntervalMs);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  tick() {
    const s = this.store.getState();

    // 1. Advance stream offsets and fluctuate rates realistically
    const updatedStreams = s.streams.map(st => {
      // Natural jitter (+/- 2%)
      const rateJitter = Math.floor(st.rate * (1 + (Math.random() * 0.04 - 0.02)));
      const offsetIncr = Math.floor(rateJitter * (this.tickIntervalMs / 1000));
      const lagJitter = Math.max(50, Math.floor(st.lag + (Math.random() * 80 - 40)));
      
      return {
        ...st,
        rate: rateJitter,
        offset: st.offset + offsetIncr,
        lag: lagJitter
      };
    });

    this.store.state.streams = updatedStreams;

    // 2. Micro-fluctuations in eBPF telemetry
    const ebpf = s.telemetry.ebpf;
    ebpf.cpu = Math.min(99, Math.max(15, Math.round((ebpf.cpu + (Math.random() * 4 - 2)) * 10) / 10));
    ebpf.socketLatency = Math.max(1.2, Math.round((ebpf.socketLatency + (Math.random() * 0.4 - 0.2)) * 10) / 10);
    ebpf.networkRx = Math.max(10, Math.round((ebpf.networkRx + (Math.random() * 2 - 1)) * 10) / 10);

    // 3. Normal guard metrics drift if healthy
    const nullGuard = s.guards.find(g => g.id === 'null_rate');
    if (nullGuard && nullGuard.status === 'HEALTHY') {
      nullGuard.current = Math.max(0.5, Math.min(4.5, Math.round((nullGuard.current + (Math.random() * 0.4 - 0.2)) * 100) / 100));
      nullGuard.metric = `${nullGuard.current.toFixed(2)}%`;
    }

    // 4. Periodically add healthy spark batch processing log
    if (Math.random() < 0.25 && s.circuitBreaker.state === 'CLOSED') {
      const batchNum = Math.floor(484 + Math.random() * 20);
      const str = s.streams[Math.floor(Math.random() * s.streams.length)];
      this.store.addEventLog(
        `batch_#00${batchNum}`,
        `SPARK MICRO-BATCH PROCESSED [${str.name}] (${str.rate.toLocaleString()} records/sec)`,
        'HEALTHY'
      );
    }

    // Notify state updates
    this.store.notify();
  }

  // --- ANOMALY INJECTION SUITE ---

  injectNullSpike() {
    const s = this.store.getState();
    const nullGuard = s.guards.find(g => g.id === 'null_rate');
    if (!nullGuard) return;

    nullGuard.current = 27.41;
    nullGuard.metric = '27.41%';
    nullGuard.breaches += 1;
    nullGuard.status = 'BREACHED';

    s.circuitBreaker.currentMetric = '27.41%';
    s.circuitBreaker.stream = 'orders';
    s.circuitBreaker.batchId = '#00483';
    s.circuitBreaker.reason = 'NULL_RATE_BREACH';

    // Transition circuit breaker
    this.store.setCircuitState('BREACH DETECTED', 'NULL_RATE_BREACH');

    this.store.addEventLog('guard_null_rate', 'CRITICAL BREACH: NULL RATE SPIKE (27.41% > 15.00%)', 'ALERT');
    this.store.addEventLog('circuit_breaker', 'STATE TRANSITION -> BREACH DETECTED -> OPEN', 'ALERT');

    setTimeout(() => {
      this.store.setCircuitState('OPEN', 'NULL_RATE_BREACH');
      this.store.addEventLog('containment_engine', 'BATCH #00483 ISOLATED & QUARANTINED TO MinIO', 'ALERT');
      this.store.triggerIncident({
        id: 'INC-00483',
        stream: 'orders',
        batchId: '00483',
        type: 'NULL_RATE_BREACH',
        current: '27.41%',
        threshold: '15.00%',
        action: 'BATCH QUARANTINED'
      });
    }, 600);
  }

  injectSchemaDrift() {
    const s = this.store.getState();
    const schemaGuard = s.guards.find(g => g.id === 'schema');
    if (!schemaGuard) return;

    schemaGuard.current = 1.00;
    schemaGuard.metric = 'FIELD TYPE MISMATCH (amount_cents: String != Int64)';
    schemaGuard.breaches += 1;
    schemaGuard.status = 'BREACHED';

    s.circuitBreaker.currentMetric = 'SCHEMA MISMATCH';
    s.circuitBreaker.stream = 'payments';
    s.circuitBreaker.batchId = '#00471';
    s.circuitBreaker.reason = 'SCHEMA_DRIFT';

    this.store.setCircuitState('OPEN', 'SCHEMA_DRIFT');
    this.store.addEventLog('guard_schema', 'CRITICAL: SCHEMA DRIFT DETECTED on payments/batch_#00471', 'ALERT');
    this.store.triggerIncident({
      id: 'INC-00471',
      stream: 'payments',
      batchId: '00471',
      type: 'SCHEMA_DRIFT',
      current: 'INVALID TYPE',
      threshold: 'STRICT',
      action: 'BATCH QUARANTINED'
    });
  }

  injectVolumeBreach() {
    const s = this.store.getState();
    const volGuard = s.guards.find(g => g.id === 'volume');
    if (!volGuard) return;

    volGuard.current = 184.20;
    volGuard.metric = '+184.20% / min (SURGE)';
    volGuard.breaches += 1;
    volGuard.status = 'BREACHED';

    s.circuitBreaker.stream = 'orders';
    s.circuitBreaker.batchId = '#00452';
    s.circuitBreaker.reason = 'VOLUME_BREACH';

    this.store.setCircuitState('OPEN', 'VOLUME_BREACH');
    this.store.addEventLog('guard_volume', 'RATE BREACH: +184.20% SURGE (Threshold: +50.00%)', 'ALERT');
    this.store.triggerIncident({
      id: 'INC-00452',
      stream: 'orders',
      batchId: '00452',
      type: 'VOLUME_BREACH',
      current: '+184.20%',
      threshold: '+50.00%',
      action: 'RATE CONTAINED'
    });
  }

  injectNetworkDegradation() {
    const s = this.store.getState();
    s.telemetry.ebpf.socketLatency = 88.4;
    s.telemetry.ebpf.packetLoss = 4.20;
    s.telemetry.ebpf.retransmits = 3.15;
    
    this.store.addEventLog('ebpf_kernel', 'eBPF SIGNAL: HIGH SOCKET LATENCY (88.4ms) & 4.2% PACKET LOSS', 'ALERT');
    this.store.notify();
  }

  healSystem() {
    const s = this.store.getState();

    // Reset guards
    s.guards.forEach(g => {
      g.status = 'HEALTHY';
      if (g.id === 'null_rate') { g.current = 2.14; g.metric = '2.14%'; }
      if (g.id === 'schema') { g.current = 0.00; g.metric = '0 drift'; }
      if (g.id === 'volume') { g.current = 3.40; g.metric = '+3.40% / min'; }
    });

    // Reset eBPF telemetry
    s.telemetry.ebpf.socketLatency = 3.1;
    s.telemetry.ebpf.packetLoss = 0.02;
    s.telemetry.ebpf.retransmits = 0.01;

    // Transition circuit breaker: OPEN -> HALF-OPEN -> RECOVERED -> CLOSED
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
}

window.Simulator = new StreamPulseSimulator(window.Store);
