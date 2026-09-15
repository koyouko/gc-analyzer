const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
const modulePath = path.join(__dirname, 'dashboard.js');
const dashboard = fs.existsSync(modulePath) ? require(modulePath) : {};

function page() {
  const elements = new Map();
  const get = id => {
    if (!elements.has(id)) elements.set(id, {innerHTML: '', style: {}, classList: {toggle() {}, add() {}, remove() {}}, addEventListener() {}, focus() {}});
    return elements.get(id);
  };
  const context = {Dashboard: dashboard, document: {body: {classList: {contains: () => false, toggle() {}}}, getElementById: get, querySelectorAll: () => []}, localStorage: {getItem() {}, setItem() {}}, setInterval() {}, clearInterval() {}, setTimeout() {}, fetch: async () => ({ok: true, json: async () => ({})})};
  context.Chart = class { static defaults = {font: {}}; constructor(el, config) {this.config = config;this.options=config.options;} destroy() {} update() {} };
  for (const id of html.matchAll(/id="([\w-]+)"/g)) context[id[1]] = get(id[1]);
  vm.createContext(context);
  const script = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]).join('\n').replace(/^boot\(\);$/m, '');
  vm.runInContext(script, context);
  return {run: code => vm.runInContext(code, context), get, context};
}

test('assets and investigation entry point are same-origin', () => {
  assert.match(html, /src="\/assets\/vendor\/chart\.umd\.js"/);
  assert.match(html, /src="\/assets\/investigations\.js"/);
  assert.match(html, /onclick="openInvestigation\(\)"[^>]*>Analyze logs/);
  assert.doesNotMatch(html, /https:\/\/(cdnjs|fonts\.)/);
});

test('missing metrics are not formatted as zero or a dash', () => {
  const p = page();
  for (const value of ['null', 'undefined', 'NaN', 'Infinity', "''"])
    assert.equal(p.run(`fmtMetric(${value}, '%')`), 'Not available');
  assert.equal(p.run("fmtMetric(0, '%')"), '0%');
});

test('unknown host health remains unknown with unavailable score and metrics', () => {
  const p = page();
  const output = p.run("renderSarSection({metrics:{sample_count:1,cpu_busy_pct_avg:null},health:{grade:'?',score:null,status:'unknown'},findings:{}}, null)");
  assert.match(output, /grade unknown">\?/);
  assert.match(output, /Not available/);
  assert.doesNotMatch(output, /null|undefined|No host resource pressure detected|grade F/);
});

test('missing GC evidence does not produce healthy diagnostic cards', () => {
  const cards = page().run('diagCards({}, [])');
  assert.ok(cards.every(c => c.s === 'unknown'));
  assert.ok(cards.every(c => c.d.includes('Not available')));
});

test('pause histogram excludes missing observations', () => {
  const buckets = page().run('pauseBucketsFromSeries([{pause_max:null},{pause_max:0},{pause_max:25}])');
  assert.equal(buckets.reduce((sum, b) => sum + b.count, 0), 2);
});

test('host charts retain null gaps including converted units', () => {
  const p = page();
  p.run('drawHostFullCharts({series:[{t:1700000000,cpu_busy_avg:null,mem_cached_avg:null,net_tx_avg:null,net_rx_avg:null}]})');
  for (const expression of ['charts.hostCpu.config.data.datasets[0].data[0]', 'charts.hostMem.config.data.datasets[3].data[0]', 'charts.hostNet.config.data.datasets[1].data[0]'])
    assert.equal(p.run(expression), null);
});

test('quality uses observations, not snapshot generation time', () => {
  assert.equal(typeof dashboard.qualityText, 'function');
  const output = dashboard.qualityText({now: 1800000000, quality: {state: 'stale', last_observed_at: 1700000000, age_seconds: 7200, available_metrics: ['cpu'], missing_metrics: ['disk']}});
  assert.match(output, /Stale/);
  assert.match(output, /2h ago/);
  assert.match(output, /1\/2 metrics/);
  assert.match(dashboard.qualityText({now: 1800000000}), /Not available/);
});

test('broker table joins by id and sorts missing values last in either direction', () => {
  assert.equal(typeof dashboard.brokerRows, 'function');
  const nodes = [{id:'b2', role:'broker', grade:'?', score:null}, {id:'b1', role:'broker', grade:'C', score:70, heap_after_pct:54}, {id:'c1', role:'controller'}];
  const rows = dashboard.brokerRows({nodes, host_nodes:[{id:'b1', cpu_busy_pct_avg:0, disk_await_ms_max:7}]});
  assert.equal(rows.length, 2);
  assert.equal(rows.find(r => r.id === 'b1').cpu, 0);
  for (const direction of ['asc','desc']) assert.equal(dashboard.sortBrokers(rows, 'cpu', direction).at(-1).id, 'b2');
  const table = dashboard.brokerTable(rows);
  assert.match(table, /GC health/);
  assert.match(table, /aria-sort=/);
  assert.match(table, /Not available/);
  assert.match(table, /\?[^<]*Unknown/);
  assert.match(dashboard.brokerTable([{...rows[0], id:'<img src=x>', label:'<img src=x>'}]), /&lt;img/);
});

test('aligned timeline uses actual timestamps with a shared UTC domain and null gaps', () => {
  assert.equal(typeof dashboard.alignedTimeline, 'function');
  const result = dashboard.alignedTimeline({series:[{t:1700000000,pause_p99_max:20},{t:1700001200,pause_p99_max:null}]}, {series:[{t:1700000300,cpu_busy_avg:80},{t:1700000900,cpu_busy_avg:null}]});
  assert.equal(result.min, 1700000000);
  assert.equal(result.max, 1700001200);
  assert.equal(result.tracks[0].points[1].y, null);
  assert.equal(result.tracks[1].points[0].x, 1700000300);
  assert.equal(result.tracks[1].points[1].y, null);
  assert.equal(dashboard.alignedTimeline({time_basis:'relative',series:[{t:100,pause_p99_max:20}]},{series:[{t:1700000300,cpu_busy_avg:80}]}).aligned, false);
  assert.equal(dashboard.alignedTimeline({series:[]}, {series:[]}).tracks.length, 0);
});

test('diagnostic summary distinguishes association and missing Kafka context', () => {
  assert.equal(typeof dashboard.diagnosticSummary, 'function');
  const output = dashboard.diagnosticSummary({health:{grade:'C'},metrics:{max_pause_ms:300}}, null, {n_points:10,correlations:[{strength:'strong',r:0.9,gc_metric:'pause_max',host_metric:'cpu_busy_avg',label:'Pause vs CPU'}]});
  for (const label of ['What we found','Why it matters','Next step','not a confirmed cause','Kafka context','not connected']) assert.ok(output.includes(label), label);
  assert.match(dashboard.diagnosticSummary({health:{grade:'?',score:null}},null,null), /Not enough evidence/);
});

test('legacy authentication and operational workflows remain available', () => {
  const p = page();
  for (const fn of ['doLogin','logout','openSettings','toggleJobsPanel','selectCluster','selectInstance','selectHost','changeRange','changeHostRange','submitSarUpload','renderCapacityOutlook','openMlPreview'])
    assert.equal(p.run(`typeof ${fn}`), 'function', fn);
});

test('empty fleet offers isolated analysis without claiming live health', () => {
  const p = page();
  p.run('FLEET={total_instances:0,counts:{},active_alerts:[],regions:[],now:1700000000};renderFleet()');
  assert.match(p.get('main').innerHTML, /No clusters connected/);
  assert.match(p.get('main').innerHTML, /openInvestigation/);
  assert.doesNotMatch(p.get('main').innerHTML, /No GC issues detected/);
});

test('cluster and host rollups tolerate nullable metrics without healthy zero claims', () => {
  const p = page();
  p.run(`renderCluster({cluster:'empty',region:'Canada',env:'Production',status:'unknown',now:1700000000,counts:{total:1,healthy:0,unhealthy:0},nodes:[{id:'b1',role:'broker',status:'unknown',grade:'?',score:null}],attention:[],memory:{used_mb:null,total_heap_mb:null,avg_util_pct:null},telemetry:{worst_pause_ms:null},config:{heap_by_role:{},gc_engine:[]},host_nodes:[],host_summary:{n_with_data:0},host_counts:{}},null,null)`);
  assert.doesNotMatch(p.get('main').innerHTML, /null%|undefined%|NaN|All nodes healthy|All hosts healthy/);
  assert.match(p.get('main').innerHTML, /Not available/);
});

test('instance overview renders unknown health, summary and aligned evidence without invented values', () => {
  const p = page();
  p.run("renderInstance({instance:{id:'b1'},metrics:{},health:{grade:'?',score:null,status:'unknown'},alerts:[],findings:{}},{series:[]},null,null,null,null,null)");
  const output = p.get('main').innerHTML;
  assert.match(output, /grade unknown">\?/);
  assert.match(output, /What we found/);
  assert.match(output, /alignedEvidence/);
  assert.match(output, /changeRange/);
  assert.doesNotMatch(output, /undefined|NaN|null%|No issues detected|>100%</);
});

test('event mix with no counts does not invent a full GC percentage', () => {
  const output = page().run('renderGcEventMix({})');
  assert.match(output, /Not available/);
  assert.doesNotMatch(output, /100%/);
});

test('advisories are labelled and learning output is separate from correlation', () => {
  const p = page();
  assert.match(p.run('renderScalingAdvisor(null)'), /Advisory/);
  assert.match(p.run('renderForecastPanel(null)'), /Tech preview/);
  assert.match(p.run('renderCapacityOutlook(null)'), /Advisory/);
  const correlation = p.run("renderCorrelationSection({verdict:'mixed',n_points:10,days:7,correlations:[],findings:[]},{method:'test',is_anomalous:true})");
  assert.match(correlation, /not a confirmed cause/);
  assert.doesNotMatch(correlation, /anomaly-badge/);
});

test('static exports disable Analyze logs without changing the export injection marker', () => {
  assert.ok(html.includes("const SC={"));
  assert.ok(html.includes("typeof __STATIC__"));
});

test('aligned memory uses available SAR measurements without estimating memory headroom', () => {
  const model=dashboard.alignedTimeline({series:[]},{series:[{t:1700000000,mem_avg:60}]});
  assert.equal(model.tracks[2].label,'Memory used (%)');
  assert.equal(model.tracks[2].points[0].y,60);
});

test('snapshot and chart labels use explicit UTC instead of browser local time', () => {
  const p=page();
  assert.equal(p.run('fmtTime(1700000000)'), '2023-11-14 22:13 UTC');
  assert.equal(p.run('fmtTime(null)'), 'Not available');
  assert.match(dashboard.clusterSummary({nodes:[]},[]), /Worst GC pause \(24h\)/);
});

test('learning readiness is concise and the actual technical model status is expandable', () => {
  const output=dashboard.learningSummary({method:'robust_zscore',readiness:'partial',overall_anomaly_score:44,notice:'Long technical model explanation.',model_status:{isolation_forest:'not_run'}});
  assert.match(output,/Robust baseline/);
  assert.match(output,/Partial evidence/);
  assert.match(output,/Advisory\. Not a probability\./);
  assert.match(output,/<details[\s\S]*Long technical model explanation/);
  assert.doesNotMatch(output.split('<details')[0],/Long technical model explanation/);
});

test('forecast machine states are rendered with readable labels', () => {
  const output=page().run("renderCapacityOutlook({risk:'no_data',horizon_days:90,days:30,headline:'More history needed',signals:[{label:'CPU',status:'trend_only',risk:'no_data',current:null,unit:'%',confidence:'low'}]})");
  assert.match(output,/>More history needed</);
  assert.match(output,/>Trend only</);
});

test('changing host range to empty removes the previous charts', async () => {
  const p=page();
  p.run("CURRENT_INSTANCE='b1';renderHostView('b1',{metrics:{sample_count:1},health:{grade:'?',score:null},findings:{}},{series:[{t:1700000000,cpu_busy_avg:80}]},null);fetchOptional=async()=>({series:[]})");
  await p.run("changeHostRange('1h')");
  assert.match(p.get('main').innerHTML,/No samples in this range/);
  assert.equal(p.run('Object.keys(charts).length'),0);
});

test('theme changes recolor existing axes without switching the current view', () => {
  const p=page();
  p.run("charts.example={options:{scales:{x:{ticks:{color:'#5b7287'},grid:{color:'#dde5ee'}}}},update(){}};document.body.classList.contains=()=>true;applyTheme('dark',true)");
  assert.equal(p.run('charts.example.options.scales.x.ticks.color'),'#93acc4');
  assert.equal(p.run('charts.example.options.scales.x.grid.color'),'#1f405e');
});

test('technical GC chart labels use UTC and do not assert Kafka service tolerance', () => {
  const p=page();
  assert.equal(p.run('seriesLabels([{t:1700000000}]).labels[0]'),'22:13');
  assert.doesNotMatch(p.run('diagCards({max_pause_ms:600},[])[2].d'), /Kafka tolerance/);
});

test('forecast and component tables label null metrics as unavailable', () => {
  const p=page();
  const output=p.run("renderCapacityOutlook({risk:'no_data',horizon_days:90,days:30,headline:'More history needed',signals:[{label:'CPU',status:'trend_only',risk:'no_data',current:null,unit:'%',confidence:null,warn_threshold:null,crit_threshold:null}]})");
  assert.doesNotMatch(output, /null%|undefined|>—</);
  assert.match(output,/Not available/);
  assert.match(p.run("ncard({id:'controller',role:'controller',grade:'?',score:null,status:'unknown'})"),/Not available/);
});

test('dedicated ML preview fetches actual model readiness and does not claim ML is absent', async () => {
  const p=page();
  p.run("CURRENT_INSTANCE='broker-1';fetchOptional=async()=>({method:'robust_zscore',readiness:'partial',overall_anomaly_score:31,notice:'Advisory output',model_status:{robust_zscore:'used',isolation_forest:'not_run'}})");
  await p.run('openMlPreview()');
  const output=p.get('main').innerHTML;
  assert.match(output,/Robust baseline/);
  assert.match(output,/Partial evidence/);
  assert.match(output,/robust zscore: used/);
  assert.match(output,/No persistent or continuous learning/);
  assert.doesNotMatch(output,/No trained model is running yet|not training yet|design candidate/);
});
