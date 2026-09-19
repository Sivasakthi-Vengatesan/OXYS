/* ==========================================================================
   OXYS - MAIN APPLICATION ORCHESTRATOR
   ========================================================================== */

document.addEventListener('DOMContentLoaded', () => {
  // 1. Navigation Synchronization (Top Tabs + Left Sidebar)
  const navTabs = document.querySelectorAll('.nav-tab-btn');
  const sidebarNavItems = document.querySelectorAll('.sidebar-nav-item');
  const views = document.querySelectorAll('.subsystem-view');

  function activateSubsystem(subsystemId) {
    // Update Top Tabs
    navTabs.forEach(b => {
      if (b.getAttribute('data-subsystem') === subsystemId) {
        b.classList.add('active');
      } else {
        b.classList.remove('active');
      }
    });

    // Update Sidebar items
    sidebarNavItems.forEach(item => {
      if (item.getAttribute('data-subsystem') === subsystemId) {
        item.classList.add('active');
      } else {
        item.classList.remove('active');
      }
    });

    // Show active view
    views.forEach(v => {
      if (v.id === `view-${subsystemId}`) {
        v.classList.add('active-view');
      } else {
        v.classList.remove('active-view');
      }
    });

    window.Store.setSubsystem(subsystemId);
  }

  navTabs.forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.getAttribute('data-subsystem');
      if (target) activateSubsystem(target);
    });
  });

  sidebarNavItems.forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const target = btn.getAttribute('data-subsystem');
      if (target) activateSubsystem(target);
    });
  });

  // 2. Initialize REPL
  window.TerminalREPL.init();

  // 3. Audio & Terminal Toggle Buttons
  const audioToggleBtn = document.getElementById('btn-toggle-audio');
  if (audioToggleBtn) {
    audioToggleBtn.addEventListener('click', () => {
      const isMuted = window.RetroAudio.toggleMute();
      audioToggleBtn.textContent = isMuted ? '[ AUDIO: OFF ]' : '[ AUDIO: ON ]';
      if (!isMuted) window.RetroAudio.playBeep();
    });
  }

  const terminalToggleBtn = document.getElementById('btn-toggle-terminal-drawer');
  const terminalDock = document.getElementById('event-terminal-dock');
  if (terminalToggleBtn && terminalDock) {
    terminalToggleBtn.addEventListener('click', () => {
      if (window.RetroAudio) window.RetroAudio.playClick();
      terminalDock.style.display = terminalDock.style.display === 'none' ? 'flex' : 'none';
    });
  }

  // 4. Real-time REST Sync from FastAPI & Database
  async function syncFromBackend() {
    try {
      const [statusRes, metricsRes, quarantineRes, guardsRes] = await Promise.all([
        fetch('/api/system/status').catch(() => null),
        fetch('/metrics').catch(() => null),
        fetch('/api/quarantine').catch(() => null),
        fetch('/api/guards').catch(() => null)
      ]);

      if (statusRes && statusRes.ok) {
        const statusData = await statusRes.json();
        if (statusData.status) window.Store.state.systemStatus = statusData.status;
      }

      if (quarantineRes && quarantineRes.ok) {
        const qData = await quarantineRes.json();
        if (Array.isArray(qData)) {
          window.Store.state.quarantine = qData;
        }
      }

      if (guardsRes && guardsRes.ok) {
        const gData = await guardsRes.json();
        if (Array.isArray(gData) && gData.length > 0) {
          window.Store.state.guards = gData;
        }
      }

      window.Store.notify();
    } catch (e) {}
  }

  // Poll backend every 2.5 seconds
  setInterval(syncFromBackend, 2500);
  syncFromBackend();

  // 5. WebSocket Live Synchronization with FastAPI Backend
  function initWebSocket() {
    try {
      const loc = window.location;
      const proto = loc.protocol === 'https:' ? 'wss:' : 'ws:';
      const host = loc.host || 'localhost:8000';
      const wsUrl = `${proto}//${host}/ws/events`;
      const ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        const wsStatusEl = document.getElementById('status-ws-indicator');
        if (wsStatusEl) wsStatusEl.innerHTML = '<span class="status-dot-sm" style="background:var(--healthy-mint);"></span> WS: SYNCED';
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.system_status) window.Store.state.systemStatus = data.system_status;
          if (data.circuit_breaker) window.Store.state.circuitBreaker = data.circuit_breaker;
          if (data.active_incident) window.Store.state.activeIncident = data.active_incident;
          if (data.streams) window.Store.state.streams = data.streams;
          if (data.guards) window.Store.state.guards = data.guards;
          if (data.quarantine) window.Store.state.quarantine = data.quarantine;
          if (data.ebpf) window.Store.state.telemetry.ebpf = data.ebpf;
          if (data.data_quality) window.Store.state.telemetry.dataQuality = data.data_quality;
          if (data.latest_event) {
            const exists = window.Store.state.eventLogs.some(e => e.id === data.latest_event.id);
            if (!exists) window.Store.state.eventLogs.unshift(data.latest_event);
          }
          window.Store.notify();
        } catch (e) {}
      };

      ws.onerror = () => {
        const wsStatusEl = document.getElementById('status-ws-indicator');
        if (wsStatusEl) wsStatusEl.innerHTML = '<span class="status-dot-sm" style="background:var(--healthy-mint);"></span> HTTP: LIVE';
      };

      ws.onclose = () => {
        setTimeout(initWebSocket, 4000);
      };
    } catch (e) {}
  }
  initWebSocket();

  // 6. Incident Detail Modal Controller
  window.openIncidentModal = function(incidentId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const modal = document.getElementById('incident-detail-modal');
    const content = document.getElementById('incident-modal-content');
    const state = window.Store.getState();
    const inc = state.activeIncident || {
      id: '#00483',
      time: '04:32:18 UTC',
      stream: 'crypto_market_stream',
      batch_id: '00483',
      trigger: 'NULL_RATE_BREACH',
      current_value: '27.41%',
      threshold: '15.00%',
      circuit_state: 'OPEN',
      action: 'QUARANTINE BATCH',
      checkpoint: 'cp_00482',
      recovery_status: 'COMPLETED'
    };

    if (modal && content) {
      content.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; border-bottom:2px dashed var(--deep-purple); padding-bottom:6px;">
          <span style="font-family:var(--font-heading); font-size:12px;">INCIDENT / ${inc.id || '#00483'}</span>
          <span class="status-badge badge-coral">ASSERTION FAILURE</span>
        </div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:18px;">
          <div class="telemetry-row"><span>TRIGGER:</span> <strong>${inc.trigger || inc.type}</strong></div>
          <div class="telemetry-row"><span>STREAM:</span> <strong>${inc.stream}</strong></div>
          <div class="telemetry-row"><span>BATCH:</span> <strong>#00${inc.batch_id || inc.batchId}</strong></div>
          <div class="telemetry-row"><span>CURRENT VALUE:</span> <strong style="color:red;">${inc.current_value || inc.current}</strong></div>
          <div class="telemetry-row"><span>THRESHOLD LIMIT:</span> <strong>${inc.threshold}</strong></div>
          <div class="telemetry-row"><span>CIRCUIT STATE:</span> <strong style="color:red;">OPEN</strong></div>
          <div class="telemetry-row"><span>CONTAINMENT:</span> <strong>QUARANTINED</strong></div>
          <div class="telemetry-row"><span>CHECKPOINT TARGET:</span> <strong>${inc.checkpoint || 'cp_00482'}</strong></div>
        </div>
        <div style="margin-top:16px; border-top:2px dashed var(--deep-purple); padding-top:10px; display:flex; justify-content:flex-end;">
          <button class="btn-brutalist btn-sm btn-dark" onclick="document.getElementById('incident-detail-modal').classList.add('hidden')">[ CLOSE ]</button>
        </div>
      `;
      modal.classList.remove('hidden');
    }
  };

  // 7. Event Tag Filtering
  const filterBtns = document.querySelectorAll('.filter-btn');
  filterBtns.forEach(b => {
    b.addEventListener('click', () => {
      filterBtns.forEach(btn => btn.classList.remove('active'));
      b.classList.add('active');
      const cat = b.getAttribute('data-filter');
      window.Store.setFilterCategory(cat);
    });
  });

  // 8. Policy Save Actions
  window.savePolicy = function(policyId, inputId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const input = document.getElementById(inputId);
    if (input) {
      window.Store.updatePolicyValue(policyId, input.value.trim());
      fetch(`/api/policies/${policyId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: input.value.trim() })
      }).catch(() => {});
      alert(`[POLICY SAVED] ${policyId} updated to ${input.value.trim()}`);
    }
  };

  // 9. Subscribe to State Updates
  window.Store.subscribe((state) => {
    // Top Bar Status
    const statusPillText = document.getElementById('system-status-text');
    const statusPulseDot = document.getElementById('system-status-dot');
    if (statusPillText && statusPulseDot) {
      statusPillText.textContent = `SYSTEM ${state.systemStatus}`;
      statusPulseDot.className = state.systemStatus === 'INCIDENT' ? 'pulse-dot danger' : 'pulse-dot';
    }

    // Incident Alert Banner
    const incidentBanner = document.getElementById('incident-alert-banner');
    const incidentText = document.getElementById('incident-alert-text');
    if (incidentBanner && incidentText) {
      if (state.activeIncident) {
        incidentBanner.style.display = 'flex';
        incidentText.textContent = `DATA QUALITY BREACH: ${state.activeIncident.type || state.activeIncident.trigger} on [${state.activeIncident.stream}] batch #${state.activeIncident.batchId || state.activeIncident.batch_id} (${state.activeIncident.current || state.activeIncident.current_value} > ${state.activeIncident.threshold})`;
      } else {
        incidentBanner.style.display = 'none';
      }
    }

    // Control View Metric Cards
    const metricEventsSec = document.getElementById('metric-events-sec');
    const metricConsumerLag = document.getElementById('metric-consumer-lag');
    const metricNullRate = document.getElementById('metric-null-rate');
    const metricIncidents = document.getElementById('metric-incidents-count');

    if (metricEventsSec) {
      const totalRate = state.streams.reduce((acc, s) => acc + (s.rate || 0), 0);
      metricEventsSec.textContent = totalRate.toLocaleString();
    }
    if (metricConsumerLag) {
      const totalLag = state.streams.reduce((acc, s) => acc + (s.lag || 0), 0);
      metricConsumerLag.textContent = totalLag.toLocaleString();
    }
    if (metricNullRate) {
      const nullGuard = state.guards.find(g => g.id === 'null_rate');
      if (nullGuard) {
        metricNullRate.textContent = `${Number(nullGuard.current || 0).toFixed(2)}%`;
        metricNullRate.style.color = Number(nullGuard.current || 0) > 15.00 ? 'red' : 'inherit';
      }
    }
    if (metricIncidents) {
      metricIncidents.textContent = state.activeIncident ? '01' : '00';
      metricIncidents.style.color = state.activeIncident ? 'red' : 'inherit';
    }

    // Streams Table
    const streamsTableBody = document.getElementById('streams-table-body');
    if (streamsTableBody) {
      streamsTableBody.innerHTML = state.streams.map(st => `
        <tr class="${state.selectedStreamId === st.id ? 'row-selected' : ''}" onclick="window.Store.selectStream('${st.id}')" style="cursor:pointer;">
          <td style="font-family:var(--font-heading); font-size:10px; font-weight:bold;">${st.name}</td>
          <td><span class="status-badge ${st.status === 'RUNNING' ? 'badge-mint' : 'badge-coral'}">● ${st.status}</span></td>
          <td style="font-weight:bold;">${Number(st.rate || 0).toLocaleString()} /s</td>
          <td>${Number(st.lag || 0).toLocaleString()}</td>
          <td>${Number(st.offset || 0).toLocaleString()}</td>
          <td>${st.partitions || 8}</td>
          <td><span class="status-badge badge-peach">${st.schemaVer || 'v1.0.0'}</span></td>
          <td><span class="status-badge ${st.circuit === 'CLOSED' ? 'badge-mint' : 'badge-coral'}">${st.circuit || 'CLOSED'}</span></td>
        </tr>
      `).join('');
    }

    // Pipelines Grid
    const pipelinesGrid = document.getElementById('pipelines-grid-container');
    if (pipelinesGrid) {
      pipelinesGrid.innerHTML = state.pipelines.map(p => `
        <div class="pipeline-card">
          <div style="display:flex; justify-content:space-between; align-items:center; border-bottom:2px solid var(--deep-purple); padding-bottom:6px;">
            <span style="font-family:var(--font-heading); font-size:11px; font-weight:bold;">${p.name}</span>
            <span class="status-badge badge-mint">● ${p.state}</span>
          </div>
          <div style="display:flex; flex-direction:column; gap:4px; font-size:18px;">
            <div>SOURCE: <strong>${p.sourceTopic || p.source_topic}</strong></div>
            <div>PROCESSOR: <strong>${p.processorJob || p.processor_job}</strong></div>
            <div>GUARD: <strong>${p.guardId || p.guard_id}</strong></div>
            <div>SINK: <strong>${p.sinkDest || p.sink_dest}</strong></div>
            <div style="font-size:14px; opacity:0.8; margin-top:4px;">LAST BATCH: ${p.lastExecution || p.last_execution || 'ACTIVE'}</div>
          </div>
        </div>
      `).join('');
    }

    // Guards Overview Grid
    const guardsGrid = document.getElementById('guards-grid-container');
    if (guardsGrid) {
      guardsGrid.innerHTML = state.guards.map(g => {
        const isBreached = g.status !== 'HEALTHY';
        let fillPct = Math.min(100, Math.max(5, (g.current / (g.threshold || 50)) * 50));
        if (g.id === 'null_rate') fillPct = Math.min(100, (g.current / 30) * 100);

        return `
          <div class="guard-card ${isBreached ? 'breached' : ''}">
            <div class="guard-header">
              <span style="font-family:var(--font-heading); font-size:10px; font-weight:bold;">${g.name}</span>
              <span class="status-badge ${isBreached ? 'badge-coral' : 'badge-mint'}">${g.status}</span>
            </div>
            
            <div style="display:flex; justify-content:space-between; align-items:flex-end;">
              <div>
                <div style="font-family:var(--font-heading); font-size:8px; opacity:0.8;">CURRENT</div>
                <div style="font-family:var(--font-data); font-size:36px; font-weight:bold; line-height:1; color:${isBreached ? 'red' : 'var(--deep-purple)'};">${g.metric}</div>
              </div>
              <div style="text-align:right;">
                <div style="font-family:var(--font-heading); font-size:8px; opacity:0.8;">LIMIT THRESHOLD</div>
                <div style="font-family:var(--font-data); font-size:22px; font-weight:bold;">${g.limit}</div>
              </div>
            </div>

            <div class="guard-meter-track">
              <div class="guard-meter-fill ${isBreached ? 'fill-breached' : ''}" style="width: ${fillPct}%;"></div>
              <div class="guard-meter-threshold-line" style="left: 50%;"></div>
            </div>

            <div style="display:flex; justify-content:space-between; font-size:16px; border-top:1px dashed var(--deep-purple); padding-top:6px;">
              <span>BREACHES: <strong>${String(g.breaches || 0).padStart(2,'0')}</strong></span>
              <span>TYPE: <strong>${g.type}</strong></span>
            </div>
          </div>
        `;
      }).join('');
    }

    // Events View Table
    const eventsTableBody = document.getElementById('events-page-table-body');
    if (eventsTableBody) {
      const filtered = state.selectedFilterCategory === 'ALL'
        ? state.eventLogs
        : state.eventLogs.filter(e => (e.category || 'STREAM').toUpperCase() === state.selectedFilterCategory);

      eventsTableBody.innerHTML = filtered.slice(0, 40).map(e => `
        <tr style="cursor:pointer;" onclick="window.openIncidentModal('${e.id}')">
          <td style="font-family:var(--font-heading); font-size:9px;">${e.time}</td>
          <td><span class="status-badge badge-peach">${e.category || 'STREAM'}</span></td>
          <td style="font-weight:bold;">${e.source}</td>
          <td>${e.message}</td>
          <td><span class="status-badge ${e.tag === 'HEALTHY' ? 'badge-mint' : e.tag === 'ALERT' ? 'badge-coral' : 'badge-purple'}">${e.tag}</span></td>
        </tr>
      `).join('');
    }

    // Subsystem Specific Components
    window.CircuitBreaker.render(state);
    window.Quarantine.render(state);
    window.Telemetry.render(state);
    window.TerminalREPL.renderLogs(state);
  });

  window.Store.notify();
});
