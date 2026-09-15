/* Presentation helpers shared by the static dashboard and focused Node tests. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.Dashboard = api;
})(globalThis, function () {
  'use strict';
  const NA = 'Not available';
  const number = value => typeof value === 'number' && Number.isFinite(value) ? value : null;
  const metric = (value, unit = '') => number(value) === null ? NA : String(value) + unit;
  const divide = (value, by) => number(value) === null ? null : value / by;
  const scaled = (value, by, unit) => number(value) === null ? NA : (value / by).toFixed(1) + unit;
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const arg = value => escape(JSON.stringify(String(value)));
  const grade = health => ['A','B','C','D','F'].includes(health?.grade) && health?.score !== null && health?.status !== 'unknown' ? health.grade : '?';
  const gradeClass = health => grade(health) === '?' ? 'unknown' : grade(health);
  const score = health => metric(health?.score, ' / 100');
  const healthLabel = health => grade(health) === '?' ? 'Unknown' : ({A:'Healthy',B:'Healthy',C:'Watch',D:'At risk',F:'Critical'}[grade(health)]);
  function timestamp(value) {
    if (number(value) !== null) return value;
    if (typeof value !== 'string' || !value.trim()) return null;
    const time = Date.parse(value);
    return Number.isFinite(time) ? time / 1000 : null;
  }
  function ageText(age) {
    if (number(age) === null || age < 0) return NA;
    if (age < 60) return '<1m ago';
    if (age < 3600) return Math.floor(age / 60) + 'm ago';
    if (age < 86400) return Math.floor(age / 3600) + 'h ago';
    return Math.floor(age / 86400) + 'd ago';
  }
  function qualityText(source) {
    const q = source?.quality || {};
    const observed = timestamp(q.last_observed_at ?? source?.last_observed_at);
    const age = number(q.age_seconds) ?? (observed === null ? null : Date.now() / 1000 - observed);
    const state = ({fresh:'Fresh',current:'Fresh',partial:'Partial',stale:'Stale',unknown:'Unknown',missing:'No data',no_data:'No data',historical:'Historical',relative:'Relative time'})[q.state] || 'Unknown';
    const available = Array.isArray(q.available_metrics) ? q.available_metrics : null;
    const missing = Array.isArray(q.missing_metrics) ? q.missing_metrics : null;
    const coverage = available && missing ? `${available.length}/${available.length + missing.length} metrics` : `coverage ${NA}`;
    return `${state} · updated ${ageText(age)} · ${coverage}`;
  }
  function qualityBand(gc, sar) {
    const item = (label, source) => {
      const missing = source?.quality?.missing_metrics || [];
      return `<div><strong>${label}</strong> ${escape(qualityText(source))}${missing.length ? `<span class="quality-missing">Missing: ${missing.map(x => escape(String(x).replace(/_/g, ' '))).join(', ')}</span>` : ''}</div>`;
    };
    return `<section class="quality-band" aria-label="Data freshness and coverage">${item('GC', gc)}${item('SAR', sar)}<div>Timezone <strong>UTC</strong></div></section>`;
  }
  function brokerRows(cluster) {
    const hosts = new Map((cluster.host_nodes || []).map(n => [n.id, n]));
    return (cluster.nodes || []).filter(n => n.role === 'broker').map(n => {
      const host = hosts.get(n.id) || {};
      return {id:n.id, label:n.node_id || n.id, health:n, host,
        healthRank:grade(n) === '?' ? null : ['F','D','C','B','A'].indexOf(grade(n)),
        cpu:number(host.cpu_busy_pct_avg), heap:number(n.heap_after_pct), disk:number(host.disk_await_ms_max),
        updated:timestamp(n.quality?.last_observed_at ?? n.last_observed_at), quality:n.quality};
    });
  }
  function sortBrokers(rows, key = 'healthRank', direction = 'asc') {
    return [...rows].sort((a, b) => {
      if (a[key] == null && b[key] != null) return 1;
      if (b[key] == null && a[key] != null) return -1;
      const order = typeof a[key] === 'string' ? a[key].localeCompare(b[key]) : (a[key] ?? 0) - (b[key] ?? 0);
      return (direction === 'desc' ? -order : order) || a.id.localeCompare(b.id);
    });
  }
  function brokerTable(rows, key = 'healthRank', direction = 'asc') {
    const columns = [['label','Broker'],['healthRank','GC health'],['cpu','CPU avg'],['heap','Heap after GC'],['disk','Disk wait max'],['updated','Updated']];
    const bar = (value, style) => number(value) === null ? `<span class="muted">${NA}</span>` : `<span class="table-value">${metric(value, '%')}<span class="table-meter ${style}" aria-hidden="true"><i style="width:${Math.max(0, Math.min(100, value))}%"></i></span></span>`;
    return `<table class="broker-table"><caption>Broker health</caption><thead><tr>${columns.map(([k,l]) => `<th scope="col" aria-sort="${key === k ? direction === 'asc' ? 'ascending' : 'descending' : 'none'}"><button type="button" data-sort="${k}" title="Sort by ${l}">${l}<span aria-hidden="true">${key === k ? direction === 'asc' ? '&#8593;' : '&#8595;' : '&#8597;'}</span></button></th>`).join('')}</tr></thead><tbody>${sortBrokers(rows, key, direction).map(n => `<tr>
      <th scope="row"><button class="table-link" onclick="selectInstance(${arg(n.id)})">${escape(n.label)}</button></th>
      <td><span class="health-text ${gradeClass(n.health)}">${grade(n.health)} | ${healthLabel(n.health)}</span></td>
      <td>${bar(n.cpu, 'cpu')}</td><td>${bar(n.heap, 'heap')}</td><td>${metric(n.disk, ' ms')}</td>
      <td class="updated-cell"><span title="${escape(qualityText(n))}">GC: ${escape(ageText(number(n.quality?.age_seconds) ?? (n.updated === null ? null : Date.now()/1000 - n.updated)))}</span><span title="${escape(qualityText(n.host))}">SAR: ${escape(qualityText(n.host).split(' · ')[1].replace('updated ', ''))}</span></td>
    </tr>`).join('') || '<tr><td colspan="6" class="muted">No brokers configured in this cluster.</td></tr>'}</tbody></table>`;
  }
  function mountBrokerTable(element, rows) {
    let key = 'healthRank', direction = 'asc';
    element.innerHTML = brokerTable(rows, key, direction);
    element.addEventListener('click', event => {
      const button = event.target.closest('[data-sort]');
      if (!button) return;
      direction = key === button.dataset.sort && direction === 'asc' ? 'desc' : 'asc';
      key = button.dataset.sort;
      element.innerHTML = brokerTable(rows, key, direction);
      element.querySelector(`[data-sort="${key}"]`).focus();
    });
  }
  function clusterSummary(cluster, rows) {
    const attention = rows.filter(n => ['C','D','F'].includes(grade(n.health)) || ['C','D','F'].includes(grade(n.host)));
    const unknown = rows.filter(n => grade(n.health) === '?').length;
    const reporting = rows.filter(n => n.host.has_data).length;
    const pauses = rows.map(n => number(n.health.max_pause_ms)).filter(n => n !== null);
    const values = [['Brokers monitored', rows.length],['Needs attention', attention.length],['Worst GC pause (24h)', pauses.length ? metric(Math.max(...pauses), ' ms') : NA],['Hosts reporting', `${reporting}/${rows.length}`]];
    const first = sortBrokers(attention)[0];
    return `<section class="cluster-summary" aria-label="Cluster summary"><div class="summary-stats">${values.map(([label,value]) => `<div><span>${label}</span><strong>${value}</strong></div>`).join('')}</div><div class="summary-next"><div><strong>${first ? `Review ${escape(first.label)} first` : unknown ? 'GC health is not available for every broker' : 'No broker alerts in the available data'}</strong><p>${unknown} broker(s) with unknown GC health. Host reporting is not time-window coverage.</p></div>${first ? `<button class="table-link" onclick="selectInstance(${arg(first.id)})">View evidence &#8594;</button>` : ''}</div></section>`;
  }
  function diagnosticSummary(gc, sar, correlation) {
    const h = gc?.health || {}, m = gc?.metrics || {};
    const gated = [gc, sar, correlation].some(s => ['stale','partial','missing','no_data'].includes(s?.quality?.state));
    const association = !gated && correlation?.n_points > 0 && (correlation.correlations || []).find(c => ['moderate','strong'].includes(c.strength) && number(c.r) !== null);
    const cpuAssociation = association && /cpu/.test(association.host_metric || '') && /pause/.test(association.gc_metric || '');
    const unknown = grade(h) === '?';
    const finding = gated ? 'Incomplete or stale evidence' : association ? 'GC and host signals are associated' : unknown ? 'Not enough evidence to assess GC health' : ['C','D','F'].includes(grade(h)) ? 'GC health needs attention' : 'No GC threshold alerts in the available data';
    const what = gated ? 'Fresh, overlapping observations are needed.' : cpuAssociation ? `GC pauses and CPU load ${association.r > 0 ? 'rise together' : 'move in opposite directions'} in the analyzed history.` : association ? `${association.label}.` : number(m.max_pause_ms) !== null ? `Longest observed GC pause: ${metric(m.max_pause_ms, ' ms')}.` : 'GC measurements are not available.';
    const why = unknown || gated ? 'Service risk cannot be assessed from this evidence.' : 'GC pauses can delay work. Customer impact has not been verified.';
    const next = unknown || gated ? 'Check collection and compare fresh GC and SAR observations.' : cpuAssociation ? 'Review broker workload and competing host processes.' : 'Review the technical evidence and compare the affected broker with its peers.';
    return `<section class="diagnostic-summary" aria-label="Diagnostic summary"><div class="diagnostic-lead"><span class="eyebrow">Diagnostic summary</span><h2>${finding}</h2><p>Association, not a confirmed cause.</p></div>${[['What we found',what],['Why it matters',why],['Next step',next]].map(([label,text]) => `<div><h3>${label}</h3><p>${escape(text)}</p></div>`).join('')}<p class="kafka-context"><strong>Kafka context:</strong> Broker request metrics not connected. Service impact not confirmed.</p></section>`;
  }
  function alignedTimeline(gc, sar) {
    const gcRelative = /relative|uptime/.test(gc?.time_basis || '');
    const valid = source => (source?.series || []).filter(p => number(p.t) !== null && p.t >= 100000000).sort((a,b) => a.t - b.t);
    const g = gcRelative ? [] : valid(gc), s = valid(sar);
    const hasAvailableMemory = s.some(p => number(p.mem_avail_avg) !== null);
    const specs = [
      ['GC pause p99 (ms)',g,'pause_p99_max','#0a85c2',1,gc?.bucket_s],
      ['CPU busy (%)',s,'cpu_busy_avg','#c77700',1,sar?.bucket_s],
      [hasAvailableMemory ? 'Available memory (GB)' : 'Memory used (%)',s,hasAvailableMemory ? 'mem_avail_avg' : 'mem_avg','#1e9e5a',hasAvailableMemory ? 1024 : 1,sar?.bucket_s],
      ['Disk wait (ms)',s,'disk_await_max','#0a85c2',1,sar?.bucket_s],
    ];
    const tracks = specs.map(([label,data,key,color,divisor,bucket]) => {
      const points = [];
      data.forEach((p,i) => {
        if (i && number(bucket) !== null && p.t - data[i-1].t > bucket * 1.5) points.push({x:data[i-1].t + bucket,y:null});
        points.push({x:p.t,y:divide(p[key],divisor)});
      });
      return {label,color,points,hasData:points.some(p => p.y !== null)};
    });
    const times = [...g,...s].map(p => p.t);
    const min = times.length ? Math.min(...times) : null, max = times.length ? Math.max(...times) : null;
    const overlap = g.length && s.length && Math.max(g[0].t,s[0].t) <= Math.min(g.at(-1).t,s.at(-1).t);
    return {min,max,aligned:!!overlap,tracks:times.length ? tracks : [],note:gcRelative ? 'GC uses relative time; it cannot be aligned with UTC host observations.' : !overlap ? 'No overlapping GC and SAR observations in this range.' : 'Shared UTC time basis. Missing observations remain gaps.'};
  }
  const utc = t => new Date(t*1000).toISOString().replace('T',' ').slice(0,16) + ' UTC';
  function timelineHtml(model) {
    return `<section class="aligned-evidence" aria-label="Aligned GC and host timeline"><div class="section-heading"><h2>Aligned timeline</h2><span class="muted">${model.min === null ? NA : utc(model.min) + ' to ' + utc(model.max)}</span></div><p class="muted">${model.note}</p>${model.tracks.map((t,i) => `<div class="timeline-track"><h3>${t.label}</h3>${t.hasData ? `<div class="timeline-chart"><canvas id="alignedChart${i}" role="img" aria-label="${t.label}"></canvas></div>` : `<p class="timeline-empty">${NA} in this range</p>`}</div>`).join('') || '<p class="timeline-empty">No timestamped GC or host observations in this range.</p>'}</section>`;
  }
  function drawTimeline(model, Chart, document, registry) {
    const linked = [];
    let selectedTime = null;
    const cursor = {id:'sharedEvidenceCursor',afterEvent(chart, args) {
      selectedTime = args.event.type === 'mouseout' ? null : chart.scales.x.getValueForPixel(args.event.x);
      linked.forEach(c => c.draw());
    },afterDraw(chart) {
      if (selectedTime === null) return;
      const x = chart.scales.x.getPixelForValue(selectedTime), area = chart.chartArea;
      if (x < area.left || x > area.right) return;
      const ctx = chart.ctx; ctx.save(); ctx.strokeStyle = '#8497a8'; ctx.setLineDash([3,3]); ctx.beginPath(); ctx.moveTo(x,area.top); ctx.lineTo(x,area.bottom); ctx.stroke(); ctx.restore();
    }};
    model.tracks.forEach((track,i) => {
      const canvas = document.getElementById('alignedChart'+i);
      if (!track.hasData || !canvas) return;
      const chart = new Chart(canvas, {type:'line',data:{datasets:[{label:track.label,data:track.points,borderColor:track.color,borderWidth:1.6,pointRadius:track.points.length < 3 ? 3 : 0,tension:0,spanGaps:false}]},options:{responsive:true,maintainAspectRatio:false,animation:false,parsing:false,interaction:{mode:'nearest',intersect:false},plugins:{legend:{display:false},tooltip:{callbacks:{title:items => items.length ? utc(items[0].parsed.x) : ''}}},scales:{x:{type:'linear',min:model.min,max:model.max === model.min ? model.max + 1 : model.max,ticks:{maxTicksLimit:6,callback:value => new Date(value*1000).toISOString().slice(model.max-model.min > 86400 ? 5 : 11,16)}},y:{beginAtZero:true,afterFit:axis => {axis.width=52;},ticks:{maxTicksLimit:3}}}},plugins:[cursor]});
      registry['aligned'+i] = chart; linked.push(chart);
    });
  }
  function learningSummary(anomaly, includeLink = true) {
    const model = ({robust_zscore:'Robust baseline',isolation_forest:'Isolation forest'})[anomaly?.method] || 'No model output';
    const ready = ({ready:'Ready',partial:'Partial evidence',insufficient_data:'More history needed'})[anomaly?.readiness] || 'Not available';
    const active = anomaly?.method && anomaly.method !== 'none';
    return `<section class="learning-summary"><h2>Learning insights <span class="preview-label">Tech preview</span></h2><p><strong>${model}</strong><br>${ready}</p>${active ? `<p>${anomaly.is_anomalous ? 'Unusual pattern detected' : 'No anomaly flagged'}. Score: ${metric(anomaly.overall_anomaly_score, ' / 100')}.</p>` : ''}<p class="muted">Advisory. Not a probability.</p><details><summary>Model details</summary><p class="muted">${escape(anomaly?.notice || 'Separate from deterministic health grading.')}</p>${Object.entries(anomaly?.model_status || {}).map(([k,v]) => `<p class="muted">${escape(k.replace(/_/g,' '))}: ${escape(typeof v === 'object' ? JSON.stringify(v) : String(v).replace(/_/g,' '))}</p>`).join('')}</details>${includeLink ? '<button class="table-link" onclick="openMlPreview()">ML Tech Preview &#8594;</button>' : ''}</section>`;
  }
  return {NA,number,metric,divide,scaled,grade,gradeClass,score,healthLabel,qualityText,qualityBand,brokerRows,sortBrokers,brokerTable,mountBrokerTable,clusterSummary,diagnosticSummary,alignedTimeline,timelineHtml,drawTimeline,learningSummary};
});
