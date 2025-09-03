
(function(){
  function initials(name){
    if(!name) return "?";
    const parts = (name+"").split(/[ ._@-]+/).filter(Boolean);
    if(parts.length === 1) return parts[0].slice(0,2).toUpperCase();
    return (parts[0][0] + parts[parts.length-1][0]).toUpperCase();
  }
  const toNum = v => Number.isFinite(+v) ? +v : 0;

  function statusClass(v){
    const n = toNum(v);
    if (n > 9) return 'status-green';       // >9 = green
    if (n >= 7) return 'status-yellow';     // 7..9 = yellow
    return 'status-red';                    // <=6 = red
  }

  function createHub(rec, idx){
    const today = toNum(rec.today);
    const yest  = toNum(rec.yesterday);
    const a7    = toNum(rec.avg_last_7);
    const a30   = toNum(rec.avg_last_30);

    const el = document.createElement('article');
    el.className = 'hub';
    el.dataset.accent = ((idx % 5) + 1);

    el.innerHTML = `
      <div class="title">
        <div class="badge">${idx+1}</div>
        <div>${rec.username || 'Recruiter'}</div>
      </div>
      <div class="canvas">
        <div class="ring"></div>
        <div class="center">
          ${rec.avatar ? `<img src="${rec.avatar}" alt="">`
                       : `<div class="initials">${initials(rec.username)}</div>`}
          <div class="name" title="${rec.username || ''}">${rec.username || ''}</div>
        </div>

        <div class="sat s1">
          <div class="bubble ${statusClass(today)}">
            <div class="num">${today}</div><div class="label">Today</div>
          </div>
        </div>
        <div class="sat s2">
          <div class="bubble ${statusClass(yest)}">
            <div class="num">${yest}</div><div class="label">Yesterday</div>
          </div>
        </div>
        <div class="sat s3">
          <div class="bubble ${statusClass(a7)}">
            <div class="num">${a7.toFixed ? a7.toFixed(1) : a7}</div><div class="label">Avg (5d)</div>
          </div>
        </div>
        <div class="sat s4">
          <div class="bubble ${statusClass(a30)}">
            <div class="num">${a30.toFixed ? a30.toFixed(1) : a30}</div><div class="label">Avg (21d)</div>
          </div>
        </div>
      </div>`;
    return el;
  }

  function renderError(msg){
    document.getElementById('grid').innerHTML = `<div style="opacity:.7">${msg}</div>`;
  }

  document.addEventListener('DOMContentLoaded', function(){
    const grid = document.getElementById('grid');
    if(!grid){ return; }
    const apiUrl = grid.dataset.api;
    if(!apiUrl){ renderError('API URL not found.'); return; }

    fetch(apiUrl, {headers: {'Accept':'application/json'}})
      .then(r => {
        if(!r.ok) throw new Error('HTTP '+r.status);
        return r.json().catch(()=>({ recruiters: [], error:'Invalid JSON' }));
      })
      .then(payload => {
        const list = (payload && payload.recruiters) ? payload.recruiters : [];
        grid.innerHTML = '';
        if(!list.length){
          renderError(payload && payload.error ? ('No data. ' + payload.error) : 'No recruiters to display.');
          return;
        }
        list.forEach((rec,i)=> grid.appendChild(createHub(rec, i)));
      })
      .catch(err => {
        console.error(err);
        renderError('Failed to load data.');
      });
  });
})();
