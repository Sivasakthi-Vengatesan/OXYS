/* ==========================================================================
   STREAMPULSE - QUARANTINE SUBSYSTEM CONTROLLER & PAYLOAD INSPECTOR
   ========================================================================== */

class QuarantineController {
  constructor(store) {
    this.store = store;
    this.modalEl = document.getElementById('quarantine-modal');
    this.deleteModalEl = document.getElementById('delete-confirm-modal');
  }

  inspectBatch(batchId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const s = this.store.getState();
    const batch = s.quarantine.find(q => q.batchId === batchId);
    if (!batch) return;

    this.store.state.inspectingBatchId = batchId;

    const titleEl = document.getElementById('modal-quarantine-title');
    const contentEl = document.getElementById('modal-quarantine-content');
    if (!titleEl || !contentEl) return;

    titleEl.textContent = `QUARANTINE / BATCH_${batch.batchId}`;

    const prettyJson = JSON.stringify(batch.samplePoison, null, 2);

    let html = `
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; border-bottom:2px dashed var(--deep-purple); padding-bottom:8px;">
        <div>
          <span style="font-family:var(--font-heading); font-size:10px;">STREAM: <strong>${batch.stream}</strong></span>
          <span style="font-family:var(--font-heading); font-size:10px; margin-left:12px;">TIME: <strong>${batch.timestamp}</strong></span>
        </div>
        <span class="status-badge badge-coral">${batch.status}</span>
      </div>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:16px;">
        <div style="background:var(--panel-peach); border:2px solid var(--deep-purple); padding:10px;">
          <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:4px;">REASON</div>
          <div style="color:var(--deep-purple); font-weight:bold; font-size:22px;">${batch.reason}</div>
          <div style="font-size:16px; margin-top:4px;">Current: <span style="color:red; font-weight:bold;">${batch.currentVal}</span> (Limit: ${batch.thresholdVal})</div>
        </div>
        <div style="background:var(--panel-peach); border:2px solid var(--deep-purple); padding:10px;">
          <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:4px;">CHECKPOINT / STORAGE</div>
          <div style="color:var(--deep-purple); font-weight:bold; font-size:22px;">${batch.checkpoint}</div>
          <div style="font-size:14px; margin-top:4px; word-break:break-all;">${batch.storage}</div>
        </div>
      </div>

      <div style="margin-bottom:12px;">
        <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:6px;">POISONED SAMPLE RECORD (RAW PAYLOAD DIFF):</div>
        <pre class="json-payload-viewer">${prettyJson}</pre>
      </div>

      <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px; border-top:2px dashed var(--deep-purple); padding-top:14px;">
        <div style="display:flex; gap:8px;">
          <button class="btn-brutalist btn-sm btn-mint" onclick="window.Quarantine.replayBatch('${batch.batchId}')">[ REPLAY BATCH ]</button>
          <button class="btn-brutalist btn-sm btn-coral" onclick="window.Quarantine.releaseBatch('${batch.batchId}')">[ RELEASE TO SINK ]</button>
          <button class="btn-brutalist btn-sm btn-coral" onclick="window.Quarantine.promptDelete('${batch.batchId}')">[ DELETE ]</button>
        </div>
        <button class="btn-brutalist btn-sm btn-dark" onclick="window.Quarantine.closeModal()">[ CLOSE ]</button>
      </div>
    `;

    contentEl.innerHTML = html;
    this.modalEl.classList.remove('hidden');
  }

  replayBatch(batchId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.store.addEventLog('quarantine_engine', `REPLAY INITIATED FOR BATCH #${batchId} (Applying Schema Sanitizer filter)`, 'INFO', 'QUARANTINE');
    
    const contentEl = document.getElementById('modal-quarantine-content');
    if (contentEl) {
      const banner = document.createElement('div');
      banner.style.cssText = 'background:var(--healthy-mint); border:2px solid var(--deep-purple); padding:8px; margin-top:10px; font-family:var(--font-heading); font-size:10px;';
      banner.textContent = `[OK] BATCH #${batchId} REPLAYED WITH ZERO CORRUPTION!`;
      contentEl.appendChild(banner);
    }
  }

  releaseBatch(batchId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.store.addEventLog('quarantine_engine', `OVERRIDE: BATCH #${batchId} FORCED RELEASE TO SINK BY OPERATOR`, 'ALERT', 'QUARANTINE');
    
    const contentEl = document.getElementById('modal-quarantine-content');
    if (contentEl) {
      const banner = document.createElement('div');
      banner.style.cssText = 'background:var(--alert-coral); border:2px solid var(--deep-purple); padding:8px; margin-top:10px; font-family:var(--font-heading); font-size:10px;';
      banner.textContent = `[CAUTION] BATCH #${batchId} RELEASED TO DOWNSTREAM LAKE.`;
      contentEl.appendChild(banner);
    }
  }

  promptDelete(batchId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.store.state.deletingBatchId = batchId;
    if (this.deleteModalEl) this.deleteModalEl.classList.remove('hidden');
  }

  confirmDelete() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const batchId = this.store.state.deletingBatchId;
    if (batchId) {
      this.store.state.quarantine = this.store.state.quarantine.filter(q => q.batchId !== batchId);
      this.store.addEventLog('quarantine_vault', `PERMANENT AUDIT PURGE: BATCH #${batchId} REMOVED FROM MinIO`, 'ALERT', 'QUARANTINE');
      this.store.notify();
    }
    this.closeDeleteModal();
    this.closeModal();
  }

  closeDeleteModal() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    if (this.deleteModalEl) this.deleteModalEl.classList.add('hidden');
  }

  closeModal() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    if (this.modalEl) this.modalEl.classList.add('hidden');
  }

  render(state) {
    const tableBody = document.getElementById('quarantine-table-body');
    if (!tableBody) return;

    if (state.quarantine.length === 0) {
      tableBody.innerHTML = `
        <tr>
          <td colspan="6" style="text-align:center; padding:24px; font-family:var(--font-heading); font-size:11px;">
            NO QUARANTINED BATCHES<br>
            <span style="font-family:var(--font-data); font-size:18px; opacity:0.8;">System has not isolated any unhealthy batches. ● PIPELINE HEALTHY</span>
          </td>
        </tr>
      `;
      return;
    }

    tableBody.innerHTML = state.quarantine.map(q => `
      <tr>
        <td style="font-weight:bold; font-family:var(--font-heading); font-size:10px;">${q.batchId}</td>
        <td><strong>${q.stream}</strong></td>
        <td><span class="status-badge badge-coral">${q.reason}</span></td>
        <td><span class="status-badge badge-purple">${q.status}</span></td>
        <td>${q.recordsTotal.toLocaleString()} (${q.poisonRecords.toLocaleString()} poison)</td>
        <td>
          <div style="display:flex; gap:6px;">
            <button class="btn-brutalist btn-sm" onclick="window.Quarantine.inspectBatch('${q.batchId}')">[ INSPECT ]</button>
            <button class="btn-brutalist btn-sm btn-mint" onclick="window.Quarantine.replayBatch('${q.batchId}')">[ REPLAY ]</button>
            <button class="btn-brutalist btn-sm btn-coral" onclick="window.Quarantine.promptDelete('${q.batchId}')">[ DELETE ]</button>
          </div>
        </td>
      </tr>
    `).join('');
  }
}

window.Quarantine = new QuarantineController(window.Store);
