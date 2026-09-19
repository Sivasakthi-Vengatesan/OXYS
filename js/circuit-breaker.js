/* ==========================================================================
   OXYS - CIRCUIT BREAKER SUBSYSTEM CONTROLLER
   ========================================================================== */

class CircuitBreakerController {
  constructor(store) {
    this.store = store;
  }

  async manualTrip() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    try {
      await fetch('/api/circuit/trip', { method: 'POST' });
    } catch (e) {}
    window.Simulator.injectNullSpike();
  }

  async manualReset() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    try {
      await fetch('/api/circuit/reset', { method: 'POST' });
    } catch (e) {}
    window.Simulator.healSystem();
  }

  async manualHalfOpen() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    try {
      await fetch('/api/circuit/half-open', { method: 'POST' });
    } catch (e) {}
    this.store.setCircuitState('HALF-OPEN', 'MANUAL_TEST_PROBE');
    this.store.addEventLog('circuit_breaker', 'MANUAL OVERRIDE: HALF-OPEN CANARY PROBE INITIATED', 'INFO');
  }

  render(state) {
    const cb = state.circuitBreaker;
    const hugeStateEl = document.getElementById('circuit-huge-state');
    const stateReasonEl = document.getElementById('circuit-state-reason');
    const stateStreamEl = document.getElementById('circuit-state-stream');
    const stateBatchEl = document.getElementById('circuit-state-batch');
    const stateActionEl = document.getElementById('circuit-state-action');

    if (hugeStateEl) {
      hugeStateEl.textContent = `STATE: ${cb.state}`;
      if (cb.state === 'OPEN' || cb.state === 'BREACH DETECTED') {
        hugeStateEl.className = 'circuit-huge-state state-open';
      } else if (cb.state === 'HALF-OPEN') {
        hugeStateEl.className = 'circuit-huge-state';
        hugeStateEl.style.background = 'var(--panel-peach)';
      } else {
        hugeStateEl.className = 'circuit-huge-state';
        hugeStateEl.style.background = 'var(--healthy-mint)';
      }
    }

    if (stateReasonEl) stateReasonEl.textContent = cb.reason || 'NONE';
    if (stateStreamEl) stateStreamEl.textContent = cb.stream || 'crypto_market_stream';
    if (stateBatchEl) stateBatchEl.textContent = cb.batchId || cb.batch_id || '#00483';
    if (stateActionEl) {
      stateActionEl.textContent = (cb.state === 'OPEN' || cb.state === 'QUARANTINE') ? 'QUARANTINE BATCH' : 'COMMITTING CLEAN SINK (ALLOW)';
    }

    // Update FSM Step highlighting
    const stepClosed = document.getElementById('fsm-step-closed');
    const stepBreach = document.getElementById('fsm-step-breach');
    const stepOpen = document.getElementById('fsm-step-open');
    const stepQuarantine = document.getElementById('fsm-step-quarantine');
    const stepHalfOpen = document.getElementById('fsm-step-halfopen');
    const stepRecovered = document.getElementById('fsm-step-recovered');

    const allSteps = [stepClosed, stepBreach, stepOpen, stepQuarantine, stepHalfOpen, stepRecovered];
    allSteps.forEach(s => {
      if (s) {
        s.classList.remove('active-step', 'step-danger');
      }
    });

    if (cb.state === 'CLOSED' && stepClosed) {
      stepClosed.classList.add('active-step');
    } else if (cb.state === 'BREACH DETECTED' && stepBreach) {
      stepBreach.classList.add('active-step', 'step-danger');
    } else if (cb.state === 'OPEN' && stepOpen) {
      stepOpen.classList.add('active-step', 'step-danger');
    } else if (cb.state === 'QUARANTINE' && stepQuarantine) {
      stepQuarantine.classList.add('active-step', 'step-danger');
    } else if (cb.state === 'HALF-OPEN' && stepHalfOpen) {
      stepHalfOpen.classList.add('active-step');
    } else if (cb.state === 'RECOVERED' && stepRecovered) {
      stepRecovered.classList.add('active-step');
    }
  }
}

window.CircuitBreaker = new CircuitBreakerController(window.Store);
