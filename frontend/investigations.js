/* Isolated investigations never write to fleet history. */
(() => {
  'use strict';
  let report = null;
  let charts = [];
  const escape = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  const number = (value, unit = '') => value == null ? 'Not available' : typeof value === 'boolean' ? (value ? 'Yes' : 'No') : typeof value !== 'number' ? String(value) : `${value.toLocaleString(undefined, {maximumFractionDigits: 2})}${unit}`;
  const when = (value, basis) => value == null ? 'Not available' : basis === 'utc' ? new Date(value * 1000).toISOString() : `${number(value)} s uptime`;

  function resultHtml(data, plots = false) {
    return `<h2>${escape(data.status)}</h2><p class="muted">This investigation is separate from fleet history and learning.</p>` + data.results.map((result, index) => {
      const a = result.analysis, m = a.metrics, h = a.health || {};
      const recommendations = Array.isArray(a.findings?.recommendations) ? a.findings.recommendations : [];
      const nextStep = typeof recommendations[0] === 'string' ? recommendations[0] : recommendations[0]?.action || recommendations[0]?.title;
      const reasons = h.reasons || [];
      return `<section class="investigation-result"><h3>${escape(result.name)}</h3>
        <p>${escape(result.files.join(', '))}</p>
        <div class="investigation-metrics"><div><small>GC health</small><strong>${escape(h.grade || '?')} / ${escape(h.status || 'unknown')}</strong></div>
        <div><small>Events</small><strong>${number(m.event_count)}</strong></div>
        <div><small>Longest pause</small><strong>${number(m.max_pause_ms, ' ms')}</strong></div>
        <div><small>Time in GC</small><strong>${number(m.pct_time_in_gc, '%')}</strong></div></div>
        <p><strong>Time basis:</strong> ${result.time_basis === 'utc' ? 'UTC' : 'JVM uptime only'}<br>${escape(when(result.quality.start, result.time_basis))} to ${escape(when(result.quality.end, result.time_basis))}</p>
        <h4>What we found</h4><p>${escape(reasons[0] || (m.event_count ? 'No scored GC warning in the supplied sample.' : 'No supported GC events were found.'))}</p>
        <h4>Why it matters</h4><p>${m.event_count ? 'GC pauses can delay application work. Kafka service impact is not confirmed by this log alone.' : 'Health cannot be assessed from this file.'}</p>
        <h4>Next step</h4><p>${escape(nextStep || 'Compare a representative busy period with host and Kafka metrics.')}</p>
        ${(a.warnings || []).map(w => `<p class="investigation-warning">${escape(w)}</p>`).join('')}
        ${plots && a.timeline?.length ? `<div class="investigation-chart"><canvas id="investigation-plot-${index}" aria-label="Observed GC pause timeline" role="img"></canvas></div>` : ''}
        <details><summary>All observed metrics</summary><table class="investigation-table"><tbody>${Object.entries(m).map(([k,v])=>`<tr><th>${escape(k.replaceAll('_',' '))}</th><td>${escape(number(v))}</td></tr>`).join('')}</tbody></table></details>
      </section>`;
    }).join('') + `<section class="investigation-result"><h3>GC and host evidence</h3><p>${escape(data.correlation.summary)}</p>
      <p>Matched minutes: ${number(data.correlation.matched_minutes)}. CPU / pause association: ${number(data.correlation.cpu_pause_correlation)}.</p>
      ${data.correlation.sampling_note ? `<p class="muted">${escape(data.correlation.sampling_note)}</p>` : ''}
      ${data.host ? `<p>Host samples: ${number(data.host.metrics.sample_count)}. CPU busy: ${number(data.host.metrics.cpu_busy_pct_avg, '%')}. Available memory: ${number(data.host.metrics.mem_avail_mb_avg, ' MB')}.</p>${(data.host.warnings || []).map(w=>`<p class="investigation-warning">${escape(w)}</p>`).join('')}` : ''}</section>`;
  }

  async function filePayload(file) {
    if (file.size > 4 * 1024 * 1024) throw new Error('Each upload must be 4 MiB or smaller.');
    if (/\.gz$/i.test(file.name)) {
      const bytes = new Uint8Array(await file.arrayBuffer());
      let binary = '';
      for (let offset = 0; offset < bytes.length; offset += 8192) binary += String.fromCharCode(...bytes.subarray(offset, offset + 8192));
      return {name: file.name, content: btoa(binary), encoding: 'gzip-base64'};
    }
    return {name: file.name, content: await file.text(), encoding: 'text'};
  }

  function destroyCharts() { charts.forEach(chart => chart.destroy()); charts = []; }

  function drawCharts(data) {
    if (!window.Chart) return;
    data.results.forEach((result, index) => {
      const canvas = document.getElementById(`investigation-plot-${index}`);
      if (!canvas) return;
      charts.push(new Chart(canvas, {type: 'line', data: {datasets: [{label: 'GC pause (ms)',
        data: result.analysis.timeline.map(p=>({x:p.t, y:p.pause_ms})), borderColor:'#0a85c2', pointRadius:1, borderWidth:1.5}]},
        options: {responsive:true, maintainAspectRatio:false, animation:false, parsing:false,
          scales: {x:{type:'linear', ticks:{maxTicksLimit:6, callback:value=>result.time_basis==='utc' ? new Date(value*1000).toISOString().slice(11,19) : `${value}s`}, title:{display:true,text:result.time_basis==='utc'?'UTC':'JVM uptime (seconds)'}}, y:{beginAtZero:true}},
          plugins:{legend:{display:false}, tooltip:{callbacks:{title:items=>when(items[0].parsed.x,result.time_basis)}}}}}));
    });
  }

  window.openInvestigation = function() {
    let dialog = document.getElementById('investigation-dialog');
    if (!dialog) {
      const stylesheet = document.createElement('link');
      stylesheet.rel = 'stylesheet'; stylesheet.href = '/assets/investigations.css'; document.head.append(stylesheet);
      dialog = document.createElement('dialog'); dialog.id = 'investigation-dialog';
      dialog.innerHTML = `<header><div><h2>Analyze logs</h2><span class="muted">One-time investigation</span></div><button type="button" id="investigation-close" aria-label="Close investigation" title="Close">&times;</button></header>
        <form id="investigation-form"><div class="investigation-fields">
        <label>GC logs<input id="investigation-files" type="file" multiple required accept=".log,.txt,.gz,.0,.1,.2,.3,.4,.5,.6,.7,.8,.9"></label>
        <label>Host SAR export (optional)<input id="investigation-sar" type="file" accept=".json,.txt,.gz"></label>
        <label class="investigation-check"><input id="investigation-group" type="checkbox"> Files are rotations from the same JVM</label>
        <label>JVM start time (optional)<input id="investigation-anchor" type="text" placeholder="2026-09-12T00:00:00Z" title="ISO-8601 timestamp including timezone"></label>
        <label>SAR date (optional)<input id="investigation-date" type="date"></label>
        <label>SAR report timezone<input id="investigation-zone" type="text" value="UTC" title="Timezone used when the SAR report was exported"></label></div>
        <div class="investigation-actions"><button type="submit" id="investigation-submit">Analyze</button><button type="button" id="investigation-export" disabled>Download report</button><span id="investigation-status" role="status" aria-live="polite"></span></div></form>
        <div id="investigation-output"></div>`;
      document.body.append(dialog);
      document.getElementById('investigation-close').onclick = () => dialog.close();
      document.getElementById('investigation-export').onclick = () => {
        if (!report) return;
        const html = `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>GC investigation</title><style>body{font:15px system-ui;color:#0a2540;max-width:1000px;margin:32px auto;padding:0 24px}section{border-top:1px solid #d8e1ea;padding:20px 0}table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:6px;border-bottom:1px solid #d8e1ea}small{display:block}strong{margin-right:24px}.investigation-metrics{display:flex;gap:24px;flex-wrap:wrap}p,th,td{overflow-wrap:anywhere}</style><h1>GC investigation</h1><p>${escape(report.generated_at)}</p>${resultHtml(report)}<p>No uploaded data was added to fleet history or learning baselines.</p></html>`;
        const url = URL.createObjectURL(new Blob([html], {type:'text/html'}));
        const link = document.createElement('a'); link.href=url; link.download='gc-investigation.html'; document.body.append(link); link.click(); link.remove(); setTimeout(()=>URL.revokeObjectURL(url),10000);
      };
      document.getElementById('investigation-form').onsubmit = async event => {
        event.preventDefault();
        const button = document.getElementById('investigation-submit'), status = document.getElementById('investigation-status');
        const output = document.getElementById('investigation-output'), download = document.getElementById('investigation-export');
        button.disabled=true; download.disabled=true; status.textContent='Analyzing...'; report=null; destroyCharts(); output.replaceChildren();
        try {
          const selected = Array.from(document.getElementById('investigation-files').files);
          if (!selected.length || selected.length>12) throw new Error('Select between 1 and 12 GC files.');
          const files=[];
          for (const file of selected) files.push(await filePayload(file));
          if (document.getElementById('investigation-group').checked) files.forEach(file=>file.group='uploaded-jvm');
          const body={files};
          const sar=document.getElementById('investigation-sar').files[0];
          if (sar) body.sar=await filePayload(sar);
          const anchor=document.getElementById('investigation-anchor').value.trim();
          if(anchor) body.start_time=anchor;
          const date=document.getElementById('investigation-date').value;
          if(date) body.report_date=date;
          body.report_timezone=document.getElementById('investigation-zone').value.trim();
          const response=await fetch('/api/investigations/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
          const data=await response.json();
          if(!response.ok) throw new Error(data.detail || 'Analysis could not complete.');
          report=data; output.innerHTML=resultHtml(data,true); drawCharts(data); download.disabled=false; status.textContent='Analysis complete';
        } catch(error) {status.textContent=error.message;}
        finally {button.disabled=false;}
      };
    }
    dialog.showModal();
  };
  if (typeof module === 'object' && module.exports) module.exports = {resultHtml};
})();
