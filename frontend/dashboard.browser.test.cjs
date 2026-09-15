// UI-only fixtures: no live collection, database, or external requests.
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

async function main() {
  const browser = await chromium.launch({headless:true,executablePath:process.env.BROWSER_EXECUTABLE});
  const context = await browser.newContext({viewport:{width:1440,height:1000}});
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const now = Math.floor(Date.now()/1000);
  const quality = {state:'fresh',last_observed_at:now-120,age_seconds:120,available_metrics:['cpu','heap','disk'],missing_metrics:[]};
  const health = {grade:'C',score:74,status:'watch',reasons:['Pause durations need review.']};
  const nodes = ['broker-3','broker-1','broker-2'].map((id,i) => ({id,node_id:id,role:'broker',group:'brokers',status:i===2?'ok':'watch',grade:['D','C','A'][i],score:[55,74,96][i],heap_after_pct:[72,54,46][i],max_pause_ms:[450,184,42][i],quality,alerts:[]}));
  const hosts = nodes.map((n,i) => ({...n,cpu_busy_pct_avg:[88,61,null][i],disk_await_ms_max:[18,7,null][i],has_data:i!==2,quality:i!==2?quality:{state:'missing',last_observed_at:null,age_seconds:null,available_metrics:[],missing_metrics:['cpu','disk']}}));
  const cluster = {cluster:'Kafka-Prod',region:'Canada',env:'Production',status:'watch',now,counts:{total:3,healthy:1,unhealthy:2},nodes,attention:[],host_nodes:hosts,host_counts:{total:3,healthy:1,unhealthy:1},host_summary:{n_with_data:2,cpu_busy_avg:74.5,cpu_busy_peak:88,disk_await_worst:18},memory:{used_mb:18432,total_heap_mb:32768,used_pct:56.2,peak_util_pct:72},telemetry:{worst_pause_ms:450,avg_throughput_pct:98.8},config:{heap_by_role:{broker:[8192]},gc_engine:['G1'],pause_target_ms:200,log_format:'Unified'}};
  const fleet = {now,total_instances:3,fleet_status:'watch',counts:{ok:1,watch:2},active_alerts:[],regions:[{region:'Canada',status:'watch',envs:[{env:'Production',status:'watch',clusters:[{cluster:'Kafka-Prod',status:'watch',groups:[{group:'brokers',label:'Brokers',count:3,status:'watch',instances:nodes}]}]}]}]};
  const metrics = {throughput_pct:98.8,pct_time_in_gc:1.2,avg_pause_ms:22,p99_pause_ms:184,max_pause_ms:450,gc_per_min:3,full_count:0,young_count:400,heap_max_mb:8192,avg_heap_after_pct:54,peak_heap_after_pct:72,promotion_trend_pct:10};
  const sarMetrics = {sample_count:24,cpu_busy_pct_avg:61,cpu_busy_pct_max:88,mem_used_pct_avg:54,mem_used_pct_max:65,disk_await_ms_max:18,swap_used_pct_max:0};
  const snap = {instance:{id:'broker-3',node_id:'broker-3',region:'Canada',env:'Production',cluster:'Kafka-Prod',role:'broker',collector:'G1',heap_max_mb:8192},metrics,health,quality,alerts:[],findings:{pros:[],cons:[],recommendations:['Compare fresh host observations.']}};
  const sar = {instance:snap.instance,metrics:sarMetrics,health,quality,findings:{}};
  const gcSeries = {bucket_s:300,series:Array.from({length:24},(_,i)=>({t:now-7200+i*300,pause_p99_max:i>12&&i<17?184:22,pause_max:i>12&&i<17?450:42,heap_used_avg:4096,heap_used_max:5120,heap_after_pct_avg:54,full_gc:0,time_in_gc_avg:1.2,throughput_avg:98.8})),heap_max_mb:8192};
  const sarSeries = {bucket_s:300,series:gcSeries.series.map((p,i)=>({t:p.t,cpu_busy_avg:i>12&&i<17?88:42,cpu_busy_max:91,mem_avg:54,mem_max:65,swap_max:0,disk_await_max:6,disk_util_max:15}))};
  let loggedIn = false, empty = false;
  await page.route('**/*', async route => {
    const url = new URL(route.request().url());
    assert.equal(url.hostname,'dashboard.test','UI requested a remote dependency');
    if (!url.pathname.startsWith('/api/')) {
      const file = url.pathname === '/' ? 'index.html' : url.pathname.replace('/assets/','');
      const absolute = path.join(__dirname,file);
      if (!fs.existsSync(absolute)) return route.fulfill({status:404,body:'Not found'});
      return route.fulfill({contentType:file.endsWith('.js')?'text/javascript':file.endsWith('.css')?'text/css':'text/html',body:fs.readFileSync(absolute)});
    }
    let body = null, status = 200;
    const p = url.pathname;
    if (p === '/api/login') {loggedIn = true; body = {ok:true};}
    else if (p === '/api/logout') {loggedIn = false; body = {ok:true};}
    else if (p === '/api/me') {status=loggedIn?200:401;body=loggedIn?{user:'UI fixture',role:'admin'}:{detail:'login required'};}
    else if (p === '/api/fleet') body=empty?{...fleet,total_instances:0,regions:[],counts:{}}:fleet;
    else if (p === '/api/jobs') body={jobs:[]};
    else if (p === '/api/clusters') body={clusters:[{key:'Kafka-Prod',name:'Kafka-Prod'}]};
    else if (p === '/api/cluster/Kafka-Prod') body=cluster;
    else if (p.endsWith('/sar/series')) body=url.searchParams.get('range')==='1h'?{series:[]}:sarSeries;
    else if (p.endsWith('/series')) body=url.searchParams.get('range')==='1h'?{series:[]}:gcSeries;
    else if (p.endsWith('/sar')) body=sar;
    else if (p.endsWith('/correlation')) body={verdict:'mixed',n_points:24,days:30,correlations:[{label:'GC pause and CPU load',gc_metric:'pause_max',host_metric:'cpu_busy_avg',r:0.88,strength:'strong'}],findings:[]};
    else if (p.endsWith('/anomalies')) body={method:'robust_zscore',readiness:'partial',overall_anomaly_score:31,notice:'No persistent model is retained after this analysis.',model_status:{robust_zscore:'used',isolation_forest:'not_run'}};
    else if (/^\/api\/instance\/[^/]+$/.test(p)) body=snap;
    return route.fulfill({status,contentType:'application/json',body:JSON.stringify(body)});
  });
  const waitFor = async (selector, text) => {
    await page.waitForFunction(({selector,text}) => document.querySelector(selector)?.textContent.includes(text),{selector,text});
  };
  const noOverflow = async label => assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true,label);
  try {
    await page.goto('http://dashboard.test/');
    await page.locator('#loginUser').fill('fixture');
    await page.locator('#loginPass').fill('fixture');
    await page.locator('.login-btn').click();
    await waitFor('#main','Fleet overview');
    await page.locator('#sidebar .indent2 .lbl').click();
    await page.waitForSelector('.broker-table');
    await page.locator('[data-sort="cpu"]').click();
    assert.equal(await page.locator('.broker-table tbody tr').last().locator('th').textContent(),'broker-2');
    await page.locator('[data-sort="cpu"]').click();
    assert.equal(await page.locator('.broker-table tbody tr').last().locator('th').textContent(),'broker-2');
    await page.screenshot({path:'/tmp/gc-dashboard-cluster-desktop.png'});
    await page.locator('.broker-table .table-link').filter({hasText:'broker-3'}).click();
    await page.waitForSelector('#alignedChart0');
    await waitFor('#main','What we found');
    const axes=await page.evaluate(()=>[charts.aligned0.scales.x.min,charts.aligned1.scales.x.min,charts.aligned0.scales.x.max,charts.aligned1.scales.x.max]);
    assert.equal(axes[0],axes[1]);assert.equal(axes[2],axes[3]);
    const ink=await page.locator('#alignedChart0').evaluate(canvas=>{const data=canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data;let count=0;for(let i=3;i<data.length;i+=4)if(data[i])count++;return count;});
    assert.ok(ink>100,'timeline canvas is not blank');
    await page.screenshot({path:'/tmp/gc-dashboard-broker-desktop.png'});
    await page.locator('#themeBtn').click();
    assert.equal(await page.locator('body').evaluate(el=>el.classList.contains('theme-dark')),true);
    await page.screenshot({path:'/tmp/gc-dashboard-broker-dark.png'});
    await page.locator('#themeBtn').click();
    await page.locator('#mlPreviewBtn').click();
    await waitFor('#main','Robust baseline');
    await waitFor('#main','Partial evidence');
    await page.screenshot({path:'/tmp/gc-dashboard-model-preview.png'});
    await page.locator('#sidebar .indent4 .lbl').filter({hasText:'broker-3'}).click();
    await page.waitForSelector('#alignedChart0');
    await page.locator('#gcTechnical summary').click();
    await page.locator('[data-tab="pause"]').click();
    assert.equal(await page.locator('[data-tab="pause"]').evaluate(el=>el.classList.contains('on')),true);
    await page.locator('#gcTechnical summary').click();
    await page.locator('#evidenceRange').selectOption('1h');
    await waitFor('#alignedEvidence','No timestamped');
    await page.locator('#evidenceRange').selectOption('24h');
    await page.waitForSelector('#alignedChart0');
    await page.locator('.range-toolbar a').click();
    await waitFor('#main','Host health analysis');
    await page.locator('#main select').selectOption('1h');
    await waitFor('#main','No samples in this range');
    await page.locator('#settingsBtn').click();
    await waitFor('#main','Cluster Management');
    await page.locator('#jobsBtn').click();
    await waitFor('#jobsList','No background jobs');
    await page.locator('#jobsBtn').click();
    await page.locator('#analyzeLogsBtn').click();
    await page.waitForSelector('#investigation-dialog[open]');
    await page.locator('#investigation-close').click();
    await page.setViewportSize({width:390,height:844});
    await noOverflow('mobile settings overflow');
    await page.locator('#sidebar .indent2 .lbl').click();
    await page.waitForSelector('.broker-table');
    await noOverflow('mobile cluster overflow');
    await page.screenshot({path:'/tmp/gc-dashboard-cluster-mobile.png',fullPage:true});
    await page.locator('.broker-table .table-link').filter({hasText:'broker-3'}).click();
    await page.waitForSelector('#alignedChart0');
    await noOverflow('mobile broker overflow');
    await page.screenshot({path:'/tmp/gc-dashboard-broker-mobile.png',fullPage:true});
    await page.locator('#analyzeLogsBtn').click();
    await page.waitForSelector('#investigation-dialog[open]');
    await noOverflow('mobile investigation overflow');
    await page.locator('#investigation-close').click();
    empty=true;
    await page.locator('header h1').first().click();
    await page.locator('#refreshBtn').click();
    await waitFor('#main','No clusters connected');
    await page.screenshot({path:'/tmp/gc-dashboard-empty-mobile.png',fullPage:true});
    await page.locator('#logoutBtn').click();
    await page.waitForSelector('#loginModal',{state:'visible'});
    assert.deepEqual(errors,[],'browser JavaScript errors');
    console.log('PASS: desktop/mobile, local Chart.js pixels, sorting, auth, theme, ranges, host, settings, jobs, investigation entry and empty fleet. Screenshots: /tmp/gc-dashboard-*.png');
  } finally {await browser.close();}
}
main().catch(error=>{console.error(error);process.exitCode=1;});
