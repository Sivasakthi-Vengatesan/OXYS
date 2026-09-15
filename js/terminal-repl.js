/* ==========================================================================
   STREAMPULSE - RETRO TERMINAL CLI COMMAND INTERPRETER
   ========================================================================== */

class TerminalReplController {
  constructor(store) {
    this.store = store;
    this.history = [];
    this.historyIndex = -1;
  }

  init() {
    const input = document.getElementById('cli-input');
    if (!input) return;

    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        const cmd = input.value.trim();
        if (cmd) {
          this.execute(cmd);
          this.history.push(cmd);
          this.historyIndex = this.history.length;
          input.value = '';
        }
      } else if (e.key === 'ArrowUp') {
        if (this.historyIndex > 0) {
          this.historyIndex--;
          input.value = this.history[this.historyIndex] || '';
        }
      } else if (e.key === 'ArrowDown') {
        if (this.historyIndex < this.history.length - 1) {
          this.historyIndex++;
          input.value = this.history[this.historyIndex] || '';
        } else {
          this.historyIndex = this.history.length;
          input.value = '';
        }
      }
    });
  }

  execute(cmdText) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const parts = cmdText.trim().split(/\s+/);
    const root = parts[0].toLowerCase();
    const arg1 = parts[1] ? parts[1].toLowerCase() : null;
    const arg2 = parts[2] ? parts[2].toLowerCase() : null;

    this.store.addEventLog('OPERATOR', `> ${cmdText}`, 'INFO');

    switch (root) {
      case 'help':
        this.store.addEventLog('SYS', 'AVAILABLE COMMANDS: status, streams, guards, circuit [trip|reset], quarantine [list|inspect <id>|replay <id>], recover, telemetry, inject [null|drift|volume|network], heal, sound [on|off], clear', 'INFO');
        break;

      case 'status': {
        const s = this.store.getState();
        this.store.addEventLog('SYS', `SYSTEM: ${s.systemStatus} | PIPELINES: 03 ACTIVE | STREAMS: ${s.streams.length} | CIRCUIT: ${s.circuitBreaker.state}`, 'INFO');
        break;
      }

      case 'streams': {
        const s = this.store.getState();
        s.streams.forEach(st => {
          this.store.addEventLog('STREAM', `${st.name.toUpperCase()} -> RATE: ${st.rate}/s | LAG: ${st.lag} | OFFSET: ${st.offset}`, 'INFO');
        });
        break;
      }

      case 'guards': {
        const s = this.store.getState();
        s.guards.forEach(g => {
          this.store.addEventLog('GUARD', `${g.name}: ${g.metric} [Limit: ${g.limit}] -> ${g.status}`, g.status === 'HEALTHY' ? 'HEALTHY' : 'ALERT');
        });
        break;
      }

      case 'circuit':
        if (arg1 === 'trip') {
          window.Simulator.injectNullSpike();
        } else if (arg1 === 'reset') {
          window.Simulator.healSystem();
        } else {
          const cb = this.store.getState().circuitBreaker;
          this.store.addEventLog('CIRCUIT', `STATE: ${cb.state} | STREAM: ${cb.stream} | REASON: ${cb.reason}`, 'INFO');
        }
        break;

      case 'quarantine':
        if (arg1 === 'list') {
          const qList = this.store.getState().quarantine;
          qList.forEach(q => {
            this.store.addEventLog('QUARANTINE', `BATCH #${q.batchId} [${q.stream}] -> ${q.reason} (${q.status})`, 'ALERT');
          });
        } else if (arg1 === 'inspect' && arg2) {
          window.Quarantine.inspectBatch(arg2);
        } else if (arg1 === 'replay' && arg2) {
          window.Quarantine.replayBatch(arg2);
        } else {
          this.store.addEventLog('SYS', 'Usage: quarantine list | quarantine inspect <batch_id> | quarantine replay <batch_id>', 'INFO');
        }
        break;

      case 'inject':
        if (arg1 === 'null') {
          window.Simulator.injectNullSpike();
        } else if (arg1 === 'drift') {
          window.Simulator.injectSchemaDrift();
        } else if (arg1 === 'volume') {
          window.Simulator.injectVolumeBreach();
        } else if (arg1 === 'network') {
          window.Simulator.injectNetworkDegradation();
        } else {
          this.store.addEventLog('SYS', 'Usage: inject [null | drift | volume | network]', 'INFO');
        }
        break;

      case 'heal':
        window.Simulator.healSystem();
        break;

      case 'telemetry':
        window.Telemetry.runCorrelationAnalysis();
        break;

      case 'recover': {
        const rec = this.store.getState().recovery;
        this.store.addEventLog('RECOVERY', `CHECKPOINT RESTORED: ${rec.currentCheckpoint} -> RESUMED STREAMING`, 'HEALTHY');
        window.Simulator.healSystem();
        break;
      }

      case 'sound':
        if (arg1 === 'off') {
          window.RetroAudio.muted = true;
          this.store.addEventLog('AUDIO', 'RETRO AUDIO MUTED', 'INFO');
        } else {
          window.RetroAudio.muted = false;
          this.store.addEventLog('AUDIO', 'RETRO AUDIO ENABLED', 'INFO');
          window.RetroAudio.playBeep();
        }
        break;

      case 'clear':
        this.store.state.eventLogs = [];
        this.store.notify();
        break;

      default:
        this.store.addEventLog('SYS', `UNKNOWN COMMAND: "${cmdText}". Type "help" for valid commands.`, 'ALERT');
        break;
    }
  }

  renderLogs(state) {
    const container = document.getElementById('event-terminal-logs');
    if (!container) return;

    container.innerHTML = state.eventLogs.slice(0, 50).map(l => {
      let tagClass = 'tag-info';
      if (l.tag === 'HEALTHY') tagClass = 'tag-healthy';
      if (l.tag === 'ALERT') tagClass = 'tag-alert';

      return `
        <div class="terminal-log-line">
          <span class="terminal-log-time">[${l.time}]</span>
          <span class="terminal-log-tag ${tagClass}">${l.source}</span>
          <span>${l.message}</span>
        </div>
      `;
    }).join('');
  }
}

window.TerminalREPL = new TerminalReplController(window.Store);
