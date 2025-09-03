(function(){
  'use strict';

  function scoreToBubbleClass(value){
    const v = Number(value);
    if(!isFinite(v)) return null;
    if(v > 9) return 'bubble--green';
    if(v >= 7) return 'bubble--yellow';
    return 'bubble--red';
  }

  async function loadData(){
    try{
      const res = await fetch('/api/recruiters/performance');
      const data = await res.json();
      if(!data || !data.recruiters){ return; }
      const map = {};
      data.recruiters.forEach(r => { map[r.username] = r; });

      // fill values into badges
      document.querySelectorAll('.rp-card').forEach(card => {
        const nameEl = card.querySelector('.rp-name');
        if(!nameEl) return;
        const uname = (nameEl.textContent || '').trim();
        const rec = map[uname] || {};

        card.querySelectorAll('.rp-badge').forEach(b => {
          const key = b.getAttribute('data-metric');
          let val = rec[key];
          if(val === undefined || val === null) {
            // leave neutral if truly missing
            b.classList.add('metric-bubble');
            b.classList.remove('bubble--green','bubble--yellow','bubble--red');
            const el = b.querySelector('.val');
            if(el) el.textContent = '';
            b.setAttribute('aria-label', 'Metric value not available');
            return;
          }

          // ensure integers show clean; floats to 2dp
          const displayVal = (typeof val === 'number' ? (Number.isInteger(val) ? val : val.toFixed(2)) : val);
          const valEl = b.querySelector('.val');
          if(valEl) valEl.textContent = displayVal;

          // apply bubble styling + color
          b.classList.add('metric-bubble');
          const cls = scoreToBubbleClass(val);
          b.classList.remove('bubble--green','bubble--yellow','bubble--red');
          if(cls) b.classList.add(cls);

          // accessibility
          b.setAttribute('aria-label', `Metric value ${displayVal}`);
          if(!b.getAttribute('title')) b.title = String(displayVal);
        });
      });
    }catch(e){
      console.error('Failed to load recruiter performance', e);
    }
  }

  document.addEventListener('DOMContentLoaded', loadData);
})();