/* ==========================================================================
   STREAMPULSE - eBPF INFRASTRUCTURE TELEMETRY & ROOT CAUSE CORRELATOR
   ========================================================================== */

class TelemetryController {
  constructor(store) {
    this.store = store;
  }

  runCorrelationAnalysis() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const s = this.store.getState();
    const ebpf = s.telemetry.ebpf;
    const nullGuard = s.guards.find(g => g.id === 'null_rate');

    const resultBox = document.getElementById('correlation-verdict-box');
    if (!resultBox) return;

    let diagnosis = '';
    let category = '';
    let isDataAnomaly = nullGuard && nullGuard.current > 15.00;
    let isInfraAnomaly = ebpf.socketLatency > 50 || ebpf.packetLoss > 1.0;

    if (isDataAnomaly && !isInfraAnomaly) {
      category = 'DATA_CORRUPTION';
      diagnosis = 'VERDICT: PURE APPLICATION DATA ANOMALY. Infrastructure kernel layer (eBPF) is 100% HEALTHY with low latency (3.1ms) and 0% loss. Root cause is upstream producer payload serialization violation.';
    } else if (isInfraAnomaly && !isDataAnomaly) {
      category = 'INFRASTRUCTURE_DEGRADATION';
      diagnosis = 'VERDICT: INFRASTRUCTURE NETWORK BOTTLENECK. Data payloads are well-formed, but kernel socket latency is elevated (88.4ms) with high packet retransmits.';
    } else if (isDataAnomaly && isInfraAnomaly) {
      category = 'COMPOUND_OUTAGE';
      diagnosis = 'VERDICT: COMPOUND SYSTEM INCIDENT. Simultaneous network packet degradation and data schema violations detected.';
    } else {
      category = 'ALL_SYSTEMS_NOMINAL';
      diagnosis = 'VERDICT: NOMINAL STABLE STATE. Both infrastructure eBPF telemetry and data-quality assertion guards are operating well within safety boundaries.';
    }

    resultBox.innerHTML = `
      <div style="background:var(--panel-peach); border:3px solid var(--deep-purple); padding:12px; margin-top:12px; box-shadow:var(--shadow-primary);">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
          <span style="font-family:var(--font-heading); font-size:10px;">DIAGNOSTIC VERDICT:</span>
          <span class="status-badge ${category === 'ALL_SYSTEMS_NOMINAL' ? 'badge-mint' : 'badge-coral'}">${category}</span>
        </div>
        <div style="font-family:var(--font-data); font-size:20px; color:var(--deep-purple); line-height:1.35;">
          ${diagnosis}
        </div>
      </div>
    `;

    this.store.addEventLog('telemetry_correlator', `DIAGNOSIS EXECUTED: ${category}`, category === 'ALL_SYSTEMS_NOMINAL' ? 'INFO' : 'ALERT');
  }

  render(state) {
    const ebpf = state.telemetry.ebpf;
    const dq = state.telemetry.dataQuality;

    const elCpu = document.getElementById('tel-val-cpu');
    const elMem = document.getElementById('tel-val-mem');
    const elNet = document.getElementById('tel-val-net');
    const elLoss = document.getElementById('tel-val-loss');
    const elRetrans = document.getElementById('tel-val-retrans');
    const elLat = document.getElementById('tel-val-lat');

    if (elCpu) elCpu.textContent = `${ebpf.cpu}%`;
    if (elMem) elMem.textContent = `${ebpf.memory}%`;
    if (elNet) elNet.textContent = `${ebpf.networkRx} MB/s`;
    if (elLoss) elLoss.textContent = `${ebpf.packetLoss}%`;
    if (elRetrans) elRetrans.textContent = `${ebpf.retransmits}%`;
    if (elLat) elLat.textContent = `${ebpf.socketLatency} ms`;

    const elDqNull = document.getElementById('tel-val-dq-null');
    const elDqSchema = document.getElementById('tel-val-dq-schema');
    const elDqVol = document.getElementById('tel-val-dq-vol');

    const nullGuard = state.guards.find(g => g.id === 'null_rate');
    if (elDqNull && nullGuard) {
      elDqNull.textContent = `${nullGuard.current.toFixed(2)}%`;
      elDqNull.style.color = nullGuard.current > 15.00 ? 'red' : 'inherit';
    }
    if (elDqSchema) elDqSchema.textContent = dq.schemaStatus;
    if (elDqVol) elDqVol.textContent = dq.volumeDelta;
  }
}

window.Telemetry = new TelemetryController(window.Store);
