
// CSRF helper
const CSRF_TOKEN = "{{ csrf_token or '' }}";
function authFetch(url, opts={}) {
  opts.headers = Object.assign({}, opts.headers || {}, {"X-CSRFToken": CSRF_TOKEN, "Content-Type":"application/json"});
  return fetch(url, opts);
}

// (10) Status popover -> small dialog
const CALLING = ["Not answering","Not reachable","Disconnected","Screen select"];
const PROFILE = ["R2 Pending","R3 Pending","R1 to be schedule","R2 scheduled","R3 scheduled","R1 scheduled","R1 FBP","R2 FBP","R3 FBP","HR Round Pending","HR round done","Offer letter Pending","Offer letter released","Draft offer released","Drop"];

document.querySelectorAll('.status-pop').forEach(btn=>{
  btn.addEventListener('click', (e)=>{
    const id = btn.dataset.id;
    const wrapper = document.createElement('div');
    wrapper.className = 'p-2 border rounded bg-white';
    wrapper.style.position = 'absolute';
    wrapper.style.zIndex = 1000;
    wrapper.innerHTML = `
      <div><strong>Calling</strong><select id="callSel" class="form-select form-select-sm mt-1">
        ${CALLING.map(o=>`<option>${o}</option>`).join('')}
      </select></div>
      <div class="mt-2"><strong>Profile</strong><select id="profSel" class="form-select form-select-sm mt-1">
        ${PROFILE.map(o=>`<option>${o}</option>`).join('')}
      </select></div>
      <div class="mt-2 d-flex gap-2">
        <button class="btn btn-sm btn-primary" id="saveSts">Save</button>
        <button class="btn btn-sm btn-light" id="closeSts">Cancel</button>
      </div>`;
    document.body.appendChild(wrapper);
    const r = btn.getBoundingClientRect();
    wrapper.style.left = (r.left)+'px'; wrapper.style.top = (window.scrollY + r.bottom + 6)+'px';
    wrapper.querySelector('#closeSts').onclick = ()=>wrapper.remove();
    wrapper.querySelector('#saveSts').onclick = async ()=>{
      const payload = {calling_status: wrapper.querySelector('#callSel').value, profile_status: wrapper.querySelector('#profSel').value};
      const res = await authFetch(`/api/candidates/${id}/status`, {method:'POST', body:JSON.stringify(payload)});
      const j = await res.json();
      if(j.ok){ btn.textContent = `${payload.calling_status} / ${payload.profile_status}`; }
      wrapper.remove();
    };
  });
});

// (9) Side panel quick-view
document.querySelectorAll('.quick-view').forEach(a=>{
  a.addEventListener('click', async (e)=>{
    e.preventDefault();
    const id = a.dataset.id;
    const panel = document.getElementById('sidePanel');
    panel.innerHTML = '<div class="text-muted p-2">Loading…</div>';
    const html = await fetch(`/candidate/${id}/partial`).then(r=>r.text());
    panel.innerHTML = html;
  });
});

// (8) Column show/hide
const tbl = document.getElementById('tblCandidates');
document.getElementById('toggleCols').onclick = ()=>{
  const picker = document.getElementById('colPicker');
  picker.style.display = picker.style.display==='none' ? 'block' : 'none';
  if(picker.dataset.init) return;
  const headers = Array.from(tbl.querySelectorAll('thead th')).map((th,i)=>({name: th.textContent.trim(), idx:i}));
  picker.innerHTML = headers.map(h=>`<label class="me-2"><input type="checkbox" data-idx="${h.idx}" checked> ${h.name}</label>`).join('');
  picker.dataset.init = '1';
  picker.querySelectorAll('input[type=checkbox]').forEach(cb=>{
    cb.addEventListener('change', ()=>{
      const i = parseInt(cb.dataset.idx,10);
      // toggle column i
      tbl.querySelectorAll(`tr`).forEach(tr=>{
        const cells = tr.children;
        if(cells[i]) cells[i].style.display = cb.checked ? '' : 'none';
      });
      saveCols();
    });
  });
  loadCols();
};
function saveCols(){
  const picker = document.getElementById('colPicker');
  const cols = Array.from(picker.querySelectorAll('input[type=checkbox]')).map(cb=>({idx:+cb.dataset.idx, show: cb.checked}));
  // Try server; fallback to localStorage if fails
  authFetch('/api/prefs/columns',{method:'POST',body:JSON.stringify({columns:cols})}).catch(()=>localStorage.setItem('candidate_columns', JSON.stringify(cols)));
}
async function loadCols(){
  try{
    const j = await (await fetch('/api/prefs/columns')).json();
    const cols = j.columns || JSON.parse(localStorage.getItem('candidate_columns')||'[]');
    const picker = document.getElementById('colPicker');
    cols.forEach(c=>{
      const cb = picker.querySelector(`input[data-idx="${c.idx}"]`);
      if(!cb) return;
      cb.checked = !!c.show;
      const i = c.idx;
      tbl.querySelectorAll('tr').forEach(tr=>{
        const cells = tr.children;
        if(cells[i]) cells[i].style.display = cb.checked ? '' : 'none';
      });
    });
  }catch(e){}
}

// (7) Saved filters
document.getElementById('saveFilter').onclick = async ()=>{
  const name = document.getElementById('filterName').value.trim();
  if(!name) return;
  const filter = {name, params: Object.fromEntries(new URLSearchParams(window.location.search))};
  await authFetch('/api/prefs/filters',{method:'POST',body:JSON.stringify({filter})});
  await populateFilters();
};
document.getElementById('deleteFilter').onclick = async ()=>{
  const sel = document.getElementById('savedFilters');
  if(!sel.value) return;
  await fetch('/api/prefs/filters?name='+encodeURIComponent(sel.value), {method:'DELETE', headers:{"X-CSRFToken": CSRF_TOKEN}});
  await populateFilters();
};
async function populateFilters(){
  const sel = document.getElementById('savedFilters');
  sel.innerHTML = '<option value="">Load saved…</option>';
  try{
    const j = await (await fetch('/api/prefs/filters')).json();
    (j.filters||[]).forEach(f=>{
      const opt = document.createElement('option'); opt.value = f.name; opt.textContent = f.name; sel.appendChild(opt);
    });
  }catch(e){}
}
populateFilters();
(function(){
  const rowCheckboxes = () => Array.from(document.querySelectorAll('.row-checkbox'));
  const selectAll = document.getElementById('select-all');
  if (selectAll) {
    selectAll.addEventListener('change', function() {
      rowCheckboxes().forEach(cb => cb.checked = selectAll.checked);
    });
  }
  document.getElementById('export-btn').addEventListener('click', function(){
    const checked = rowCheckboxes().filter(cb => cb.checked).map(cb => cb.value);
    const params = new URLSearchParams(window.location.search);
    if (checked.length > 0) {
      params.set('ids', checked.join(','));
    } else {
      params.delete('ids');
    }
    const url = new URL(window.location.origin + "{{ url_for('export_candidates', req_id=requirement.id) }}");
    url.search = params.toString();
    window.location.href = url.toString();
  });
  const resetBtn = document.getElementById('reset-btn');
  if (resetBtn) {
    resetBtn.addEventListener('click', function(){
      const form = document.getElementById('search-form');
      const inputs = form.querySelectorAll('input[name="name"], input[name="phone"], input[name="email"], input[name="location"]');
      inputs.forEach(i => i.value = '');
      form.submit();
    });
  }
  document.querySelectorAll('.clear-filter').forEach(el => {
    el.addEventListener('click', function(e){
      e.preventDefault();
      const param = this.getAttribute('data-param');
      const params = new URLSearchParams(window.location.search);
      params.delete(param);
      const url = window.location.pathname + (params.toString() ? ('?' + params.toString()) : '');
      window.location.href = url;
    });
  });
  document.addEventListener('click', function (e) {
    const btn = e.target.closest('.view-candidate');
    if (!btn) return;
    e.preventDefault();
    const id = btn.getAttribute('data-id');
    const target = document.getElementById('candidateModalContent');
    const modalEl = document.getElementById('candidateModal');
    if (!modalEl) { console.warn('Modal element missing'); return; }
    const bsModal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
    target.innerHTML = '<div class="p-4">Loading...</div>';
    bsModal.show();
    fetch(`{{ url_for('candidate_partial', cand_id=0) }}`.replace('/0/', `/${id}/`), {
      headers: { 'X-Requested-With': 'XMLHttpRequest' }
    })
    .then(resp => {
      if (!resp.ok) return resp.text().then(t => { throw new Error(t || 'Error loading'); });
      return resp.text();
    })
    .then(html => { target.innerHTML = html; })
    .catch(err => {
      console.error(err);
      target.innerHTML = '<div class="p-3">Error loading details</div>';
    });
  });


// --- Preview Button Logic for Excel Import Modal ---
const fileInput = document.getElementById('excelFile');
const previewBtn = document.getElementById('previewBtn');
const excelPreview = document.getElementById('excelPreview');
const excelErrors = document.getElementById('excelErrors');
const excelCommitBtn = document.getElementById('excelCommitBtn');
const excelPreviewArea = document.getElementById('excelPreviewArea');
const REQ_ID = {{ requirement.id }};
const CSRF = "{{ csrf_token() }}";

previewBtn.addEventListener('click', async function(){
  if (!fileInput.files || fileInput.files.length === 0) return;
  excelErrors.textContent = '';
  excelPreviewArea.innerHTML = '<div>Loading...</div>';
  excelPreview.querySelector('thead tr').innerHTML = '';
  excelPreview.querySelector('tbody').innerHTML = '';
  excelCommitBtn.disabled = true;
  const f = fileInput.files[0];
  const fd = new FormData();
  fd.append('file', f);
  const resp = await fetch(`/requirement/${REQ_ID}/candidates/import/upload`, {
    method: 'POST',
    headers: { 'X-CSRFToken': CSRF },
    body: fd
  });
  const data = await resp.json();
  if (!resp.ok) {
    excelErrors.textContent = data.error || 'Upload error';
    excelPreviewArea.innerHTML = '';
    return;
  }
  let rows = data.rows || [];
  // Render preview table
  const HEADS = [
    'Application Date','Job Title','Candidate Name*','Current Company','Total Exp','Phones','Emails','Notice Period','Current Location','Preferred Locations','CTC','ECTC','Key Skills','Education','Post Graduation','PF Docs','NP Details','Current CTC LPA','Expected CTC LPA','Employee Size','Companies Worked','Calling Status','Profile Status','Comments'
  ];
  const COLS = [
    'application_date','job_title','candidate_name','current_company','total_experience','phones','emails','notice_period','current_location','preferred_locations','ctc_current','ectc','key_skills','education','post_graduation','pf_docs_confirm','notice_period_details','current_ctc_lpa','expected_ctc_lpa','employee_size','companies_worked','calling_status','profile_status','comments'
  ];
  // Render preview
  const thead = excelPreview.querySelector('thead tr');
  thead.innerHTML = HEADS.map(h => `<th>${h}</th>`).join('');
  const tbody = excelPreview.querySelector('tbody');
  tbody.innerHTML = '';
  rows.forEach((r, i) => {
    const tr = document.createElement('tr');
    COLS.forEach(c => {
      const td = document.createElement('td');
      td.textContent = Array.isArray(r[c]) ? r[c].join(', ') : (r[c] ?? '');
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  if ((data.row_errors || {}) && Object.keys(data.row_errors).length) {
    excelErrors.innerHTML = Object.entries(data.row_errors).map(([row, errs]) => `<div>Row ${row}: ${errs.join(' / ')}</div>`).join('');
  }
  excelCommitBtn.disabled = rows.length === 0;
  excelPreviewArea.innerHTML = '';
});

  // ===== Bulk Import & Paste =====
  const CSRF = "{{ csrf_token() }}";
  const REQ_ID = {{ requirement.id }};
  const COLS = [
    'application_date','job_title','candidate_name','current_company','total_experience','phones','emails','notice_period','current_location','preferred_locations','ctc_current','ectc','key_skills','education','post_graduation','pf_docs_confirm','notice_period_details','current_ctc_lpa','expected_ctc_lpa','employee_size','companies_worked','calling_status','profile_status','comments'
  ];
  const HEADS = [
    'Application Date','Job Title','Candidate Name*','Current Company','Total Exp','Phones','Emails','Notice Period','Current Location','Preferred Locations','CTC','ECTC','Key Skills','Education','Post Graduation','PF Docs','NP Details','Current CTC LPA','Expected CTC LPA','Employee Size','Companies Worked','Calling Status','Profile Status','Comments'
  ];

  function renderTable(table, rows){
    const thead = table.querySelector('thead tr');
    thead.innerHTML = HEADS.map(h => `<th>${h}</th>`).join('');
    const tbody = table.querySelector('tbody');
    tbody.innerHTML = '';
    rows.forEach((r, i) => {
      const tr = document.createElement('tr');
      COLS.forEach(c => {
        const td = document.createElement('td');
        td.contentEditable = true;
        td.dataset.key = c;
        td.textContent = Array.isArray(r[c]) ? r[c].join(', ') : (r[c] ?? '');
        td.addEventListener('input', () => {
          const val = td.textContent.trim();
          if (c === 'phones' || c === 'emails') {
            r[c] = val ? val.split(/[,;|]/).map(s=>s.trim()).filter(Boolean) : [];
          } else if (['ctc_current','ectc','current_ctc_lpa','expected_ctc_lpa'].includes(c)) {
            r[c] = val ? Number(val) : null;
          } else if (c === 'employee_size') {
            r[c] = val ? parseInt(val,10) : null;
          } else if (c === 'pf_docs_confirm') {
            r[c] = /^\s*(1|true|yes|y|on)\s*$/i.test(val);
          } else {
            r[c] = val;
          }
        });
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  // Excel flow
  const excelFile = document.getElementById('excelFile');
  const excelPreview = document.getElementById('excelPreview');
  const excelErrors = document.getElementById('excelErrors');
  const excelCommitBtn = document.getElementById('excelCommitBtn');
  let excelRows = [];

  if (excelFile) {
    excelFile.addEventListener('change', async function(){
      excelErrors.textContent = '';
      excelRows = [];
      excelCommitBtn.disabled = true;
      const f = this.files[0];
      if (!f) return;
      const fd = new FormData();
      fd.append('file', f);
      const resp = await fetch(`/requirement/${REQ_ID}/candidates/import/upload`, {
        method: 'POST',
        headers: { 'X-CSRFToken': CSRF },
        body: fd
      });
      const data = await resp.json();
      if (!resp.ok) {
        excelErrors.textContent = data.error || 'Upload error';
        return;
      }
      excelRows = data.rows || [];
      renderTable(excelPreview, excelRows);
      if ((data.errors||[]).length) {
        excelErrors.innerHTML = data.errors.map(e=>`<div>• ${e}</div>`).join('');
      }
      excelCommitBtn.disabled = excelRows.length === 0;
    });
  }

  if (excelCommitBtn) {
    excelCommitBtn.addEventListener('click', async function(){
      const resp = await fetch(`/requirement/${REQ_ID}/candidates/import/commit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': CSRF },
        body: JSON.stringify({ rows: excelRows })
      });
      const data = await resp.json();
      if (!resp.ok) {
        alert(data.error || 'Save failed');
        return;
      }
      alert(`Imported ${data.inserted} candidate(s).`);
      window.location.reload();
    });
  }

  // Paste flow
  const pasteTA = document.getElementById('pasteTextarea');
  const pastePreview = document.getElementById('pastePreview');
  const pasteCommitBtn = document.getElementById('pasteCommitBtn');
  let pasteRows = [];

  function parsePasted(text){
    const lines = text.split(/\r?\n/).map(l=>l.trim()).filter(Boolean);
    const rows = [];
    for (const line of lines) {
      const parts = line.split(/\t|\s*,\s*/); // tab or comma
      const r = {};
      COLS.forEach((c, idx) => {
        r[c] = parts[idx] || '';
      });
      r.phones = r.phones ? r.phones.split(/[,;|]/).map(s=>s.trim()).filter(Boolean) : [];
      r.emails = r.emails ? r.emails.split(/[,;|]/).map(s=>s.trim()).filter(Boolean) : [];
      r.pf_docs_confirm = /^\s*(1|true|yes|y|on)\s*$/i.test(String(r.pf_docs_confirm));
      ['ctc_current','ectc','current_ctc_lpa','expected_ctc_lpa'].forEach(k => { r[k] = r[k] ? Number(r[k]) : null; });
      r.employee_size = r.employee_size ? parseInt(r.employee_size, 10) : null;
      rows.push(r);
    }
    return rows;
  }

  function renderPasteTable(){
    renderTable(pastePreview, pasteRows);
    pasteCommitBtn.disabled = pasteRows.length === 0;
  }

  if (pasteTA) {
    pasteTA.addEventListener('input', function(){
      pasteRows = parsePasted(this.value);
      renderPasteTable();
    });
  }

  if (pasteCommitBtn) {
    pasteCommitBtn.addEventListener('click', async function(){
      const resp = await fetch(`/requirement/${REQ_ID}/candidates/import/commit`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': CSRF },
        body: JSON.stringify({ rows: pasteRows })
      });
      const data = await resp.json();
      if (!resp.ok) {
        alert(data.error || 'Save failed');
        return;
      }
      alert(`Imported ${data.inserted} candidate(s).`);
      window.location.reload();
    });
  }

})();



document.addEventListener('DOMContentLoaded', function(){
  const _csrfTokenMeta = document.querySelector('meta[name="csrf-token"]');
  const _csrfToken = _csrfTokenMeta ? _csrfTokenMeta.getAttribute('content') : null;
  // Delegate click to status-popover buttons
  document.body.addEventListener('click', function(e){
    const btn = e.target.closest('.status-popover');
    if(!btn) return;
    e.preventDefault();
    // Remove any existing popovers
    const existing = document.querySelector('.status-popover-panel');
    if(existing) existing.remove();
    const id = btn.getAttribute('data-id');
    const currentProfile = btn.getAttribute('data-profile') || '';
    const currentCalling = btn.getAttribute('data-calling') || '';
    // Build popover panel
    const panel = document.createElement('div');
    panel.className = 'card status-popover-panel shadow-sm';
    panel.style.position = 'absolute';
    panel.style.zIndex = 2000;
    panel.style.minWidth = '260px';
    panel.innerHTML = `
      <div class="card-body p-2">
        <div class="mb-2"><strong>Update statuses</strong></div>
        <div class="mb-2">
          <label class="form-label small mb-1">Profile Status</label>
          <select class="form-select form-select-sm" id="pp_${id}">
            <option value="">(keep)</option>
            <option>R2 Pending</option>
            <option>R3 Pending</option>
            <option>R1 to be schedule</option>
            <option>R2 scheduled</option>
            <option>R3 scheduled</option>
            <option>R1 scheduled</option>
            <option>R1 FBP</option>
            <option>R2 FBP</option>
            <option>R3 FBP</option>
            <option>HR Round Pending</option>
            <option>HR round done</option>
            <option>Offer letter Pending</option>
            <option>Offer letter released</option>
            <option>Draft offer released</option>
            <option>Drop</option>
          </select>
        </div>
        <div class="mb-2">
          <label class="form-label small mb-1">Calling Status</label>
          <select class="form-select form-select-sm" id="cc_${id}">
            <option value="">(keep)</option>
            <option>Not answering</option>
            <option>Not reachable</option>
            <option>Disconnected</option>
            <option>Screen select</option>
          </select>
        </div>
        <div class="d-flex justify-content-end gap-2">
          <button class="btn btn-sm btn-secondary" id="cancel_${id}">Cancel</button>
          <button class="btn btn-sm btn-primary" id="save_${id}">Save</button>
        </div>
      </div>
    `;
    document.body.appendChild(panel);
    // Position panel near button
    const rect = btn.getBoundingClientRect();
    panel.style.top = (window.scrollY + rect.bottom + 6) + 'px';
    panel.style.left = (window.scrollX + rect.left) + 'px';
    // preselect current values if present
    try{ if(currentProfile) panel.querySelector('#pp_' + id).value = currentProfile; }catch(e){}
    try{ if(currentCalling) panel.querySelector('#cc_' + id).value = currentCalling; }catch(e){}
    // handlers
    panel.querySelector('#cancel_' + id).addEventListener('click', function(){ panel.remove(); });
    panel.querySelector('#save_' + id).addEventListener('click', async function(){
      const profile = panel.querySelector('#pp_' + id).value;
      const calling = panel.querySelector('#cc_' + id).value;
      const payload = {};
      if(profile) payload.profile_status = profile;
      if(calling) payload.calling_status = calling;
      if(Object.keys(payload).length === 0){ panel.remove(); return; }
      try{
        const resp = await fetch(`/candidate/${id}/status`, {
          method: 'POST',
          credentials: 'same-origin',
          headers: {'Content-Type':'application/json', 'X-CSRFToken': _csrfToken},
          body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if(!data.ok){ alert(data.error || 'Update failed'); return; }
        // Update row UI
        const row = document.querySelector('button.status-popover[data-id="' + id + '"]').closest('tr');
        if(profile){
          const headers = Array.from(document.querySelectorAll('table thead th')).map(th => th.textContent.trim().toLowerCase());
          const idx = headers.findIndex(h => h.includes('profile status'));
          if(idx >= 0){
            const cell = row.querySelector('td:nth-child(' + (idx+1) + ')');
            if(cell) cell.innerHTML = '<span class="badge ' + (profile === 'Drop' ? 'bg-danger' : 'bg-secondary') + '">' + profile + '</span>';
          }
          const btnEl = row.querySelector('button.status-popover[data-id="' + id + '"]');
          if(btnEl) btnEl.setAttribute('data-profile', profile);
        }
        if(calling){
          const btnEl = row.querySelector('button.status-popover[data-id="' + id + '"]');
          if(btnEl) btnEl.setAttribute('data-calling', calling);
          const headers = Array.from(document.querySelectorAll('table thead th')).map(th => th.textContent.trim().toLowerCase());
          const idxC = headers.findIndex(h => h.includes('calling status'));
          if(idxC >= 0){
            const cellC = row.querySelector('td:nth-child(' + (idxC+1) + ')');
            if(cellC) cellC.textContent = calling;
          }
        }
        panel.remove();
      }catch(err){ console.error(err); alert('Update failed'); }
    });
    // click outside to close
    setTimeout(()=>{
      const onDoc = (ev)=>{ if(!panel.contains(ev.target) && !btn.contains(ev.target)){ panel.remove(); document.removeEventListener('click', onDoc); } };
      document.addEventListener('click', onDoc);
    }, 50);
  });
});
