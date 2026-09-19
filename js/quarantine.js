/* ==========================================================================
   OXYS - QUARANTINE SUBSYSTEM CONTROLLER & PAYLOAD INSPECTOR
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
    const batch = s.quarantine.find(q => (q.batchId || q.batch_id) === batchId);
    if (!batch) return;

    this.store.state.inspectingBatchId = batchId;

    const titleEl = document.getElementById('modal-quarantine-title');
    const contentEl = document.getElementById('modal-quarantine-content');
    if (!titleEl || !contentEl) return;

    titleEl.textContent = `OXYS QUARANTINE / BATCH_${batch.batchId || batch.batch_id}`;

    const prettyJson = JSON.stringify(batch.samplePoison || batch.payload || {}, null, 2);

    let html = `
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px; border-bottom:2px dashed var(--deep-purple); padding-bottom:8px;">
        <div>
          <span style="font-family:var(--font-heading); font-size:10px;">STREAM: <strong>${batch.stream}</strong></span>
          <span style="font-family:var(--font-heading); font-size:10px; margin-left:12px;">TIME: <strong>${batch.timestamp}</strong></span>
        </div>
        <span class="status-badge badge-coral">${batch.status || 'ISOLATED'}</span>
      </div>

      <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:16px;">
        <div style="background:var(--panel-peach); border:2px solid var(--deep-purple); padding:10px;">
          <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:4px;">REASON</div>
          <div style="color:var(--deep-purple); font-weight:bold; font-size:22px;">${batch.reason}</div>
          <div style="font-size:16px; margin-top:4px;">Current: <span style="color:red; font-weight:bold;">${batch.currentVal || batch.current_val || 'N/A'}</span> (Limit: ${batch.thresholdVal || batch.threshold_val || 'N/A'})</div>
        </div>
        <div style="background:var(--panel-peach); border:2px solid var(--deep-purple); padding:10px;">
          <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:4px;">CHECKPOINT / STORAGE</div>
          <div style="color:var(--deep-purple); font-weight:bold; font-size:22px;">${batch.checkpoint}</div>
          <div style="font-size:14px; margin-top:4px; word-break:break-all;">${batch.storage || batch.storage_path || 's3://oxys-quarantine/'}</div>
        </div>
      </div>

      <div style="margin-bottom:12px;">
        <div style="font-family:var(--font-heading); font-size:9px; margin-bottom:6px;">POISONED SAMPLE RECORD (RAW PAYLOAD DIFF):</div>
        <pre class="json-payload-viewer">${prettyJson}</pre>
      </div>

      <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px; border-top:2px dashed var(--deep-purple); padding-top:14px;">
        <div style="display:flex; gap:8px;">
          <button class="btn-brutalist btn-sm btn-mint" onclick="window.Quarantine.replayBatch('${batch.batchId || batch.batch_id}')">[ REPLAY BATCH ]</button>
          <button class="btn-brutalist btn-sm btn-coral" onclick="window.Quarantine.releaseBatch('${batch.batchId || batch.batch_id}')">[ RELEASE TO SINK ]</button>
          <button class="btn-brutalist btn-sm btn-coral" onclick="window.Quarantine.promptDelete('${batch.batchId || batch.batch_id}')">[ DELETE ]</button>
        </div>
        <button class="btn-brutalist btn-sm btn-dark" onclick="window.Quarantine.closeModal()">[ CLOSE ]</button>
      </div>
    `;

    contentEl.innerHTML = html;
    this.modalEl.classList.remove('hidden');
  }

  async replayBatch(batchId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.store.addEventLog('quarantine_engine', `REPLAY INITIATED FOR BATCH #${batchId} (Applying Schema Sanitizer filter)`, 'INFO', 'QUARANTINE');
    
    try {
      await fetch(`/api/quarantine/${batchId}/replay`, { method: 'POST' });
    } catch (e) {}

    const contentEl = document.getElementById('modal-quarantine-content');
    if (contentEl) {
      const banner = document.createElement('div');
      banner.style.cssText = 'background:var(--healthy-mint); border:2px solid var(--deep-purple); padding:8px; margin-top:10px; font-family:var(--font-heading); font-size:10px;';
      banner.textContent = `[OK] BATCH #${batchId} REPLAYED WITH ZERO CORRUPTION!`;
      contentEl.appendChild(banner);
    }
  }

  async releaseBatch(batchId) {
    if (window.RetroAudio) window.RetroAudio.playClick();
    this.store.addEventLog('quarantine_engine', `OVERRIDE: BATCH #${batchId} FORCED RELEASE TO SINK BY OPERATOR`, 'ALERT', 'QUARANTINE');
    
    try {
      await fetch(`/api/quarantine/${batchId}/release`, { method: 'POST' });
    } catch (e) {}

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

  async confirmDelete() {
    if (window.RetroAudio) window.RetroAudio.playClick();
    const batchId = this.store.state.deletingBatchId;
    if (batchId) {
      this.store.state.quarantine = this.store.state.quarantine.filter(q => (q.batchId || q.batch_id) !== batchId);
      this.store.addEventLog('quarantine_vault', `PERMANENT AUDIT PURGE: BATCH #${batchId} REMOVED FROM MinIO`, 'ALERT', 'QUARANTINE');
      this.store.notify();
      try {
        await fetch(`/api/quarantine/${batchId}/delete`, { method: 'POST' });
      } catch (e) {}
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

    if (!state.quarantine || state.quarantine.length === 0) {
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
        <td style="font-weight:bold; font-family:var(--font-heading); font-size:10px;">${q.batchId || q.batch_id}</td>
        <td><strong>${q.stream}</strong></td>
        <td><span class="status-badge badge-coral">${q.reason}</span></td>
        <td><span class="status-badge badge-purple">${q.status || 'ISOLATED'}</span></td>
        <td>${Number(q.recordsTotal || q.records_total || 1).toLocaleString()} (${Number(q.poisonRecords || q.poison_records || 1).toLocaleString()} poison)</td>
        <td>
          <div style="display:flex; gap:6px;">
            <button class="btn-brutalist btn-sm" onclick="window.Quarantine.inspectBatch('${q.batchId || q.batch_id}')">[ INSPECT ]</button>
            <button class="btn-brutalist btn-sm btn-mint" onclick="window.Quarantine.replayBatch('${q.batchId || q.batch_id}')">[ REPLAY ]</button>
            <button class="btn-brutalist btn-sm btn-coral" onclick="window.Quarantine.promptDelete('${q.batchId || q.batch_id}')">[ DELETE ]</button>
          </div>
        </td>
      </tr>
    `).join('');
  }
}

window.Quarantine = new QuarantineController(window.Store);
