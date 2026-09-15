const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.join(__dirname, '..');
const modulePath = path.join(root, 'frontend/prometheus-settings.js');
const frontend = fs.existsSync(modulePath) ? require(modulePath) : {};
const feature = name => {
  assert.equal(typeof frontend[name], 'function', `Prometheus frontend exports ${name}`);
  return frontend[name];
};
const defaults = () => ({
  version: 1, base_url: '', authentication: 'none', timeout_seconds: 5, scrape_interval_seconds: 60,
  filters: {job: [], region: ['emea', 'amer', 'apac'], tier: ['dev', 'uat', 'prod', 'sandbox', 'stage'],
    infra: ['icp', 'phy'], az: [], service: ['kafka', 'connect', 'registry', 'zookeeper'], instance: []},
  service_roles: {kafka: 'broker', connect: 'connect', registry: 'schema-registry', zookeeper: 'zookeeper'},
});
const connection = (revision, status = 'not_tested') => ({status, revision, checked_at: status === 'not_tested' ? null : '2026-09-14T12:00:00Z', message: '', latency_ms: status === 'not_tested' ? null : 12});
const document = (saved = true, revision = 'r1') => ({config: {...defaults(), base_url: saved ? 'http://prometheus.example.test:9090' : ''}, config_path: '/local/config/prometheus.json', saved, configured: saved, revision, connection: connection(revision)});
const draft = () => feature('configToForm')(document().config);
const response = (data, status = 200) => ({ok: status >= 200 && status < 300, status, json: async () => data});
const deferred = () => {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return {promise, resolve};
};
function controller(responses, extra = {}) {
  const calls = [], updates = [];
  const instance = feature('createController')({isAdmin: true, isCurrent: () => true,
    request: async (url, options) => {calls.push({url, options}); return responses.shift();},
    onChange: state => updates.push(structuredClone(state)), ...extra});
  return {instance, calls, updates};
}

test('defaults hydrate exactly, without inventing an endpoint', () => {
  const form = feature('configToForm')(defaults());
  assert.equal(form.base_url, '');
  assert.equal(form.timeout_seconds, '5');
  assert.equal(form.scrape_interval_seconds, '60');
  assert.equal(form.filters.region, 'emea, amer, apac');
  assert.equal(form.filters.tier, 'dev, uat, prod, sandbox, stage');
  assert.deepEqual(form.service_roles, Object.entries(defaults().service_roles).map(([service, role]) => ({service, role})));
});

test('form serializes the exact config shape and preserves cross-region placement and literal labels', () => {
  const form = draft();
  form.filters.job = ' AMER-ZK,EMEA.*\namer=literal ';
  form.filters.region = 'emea';
  form.filters.az = 'AZ-1\nAZ-2';
  form.service_roles.push({service: 'kraft', role: 'controller'}, {service: 'custom', role: 'other'});
  const config = feature('formToConfig')(form);
  assert.deepEqual(config, {...document().config,
    filters: {...defaults().filters, job: ['AMER-ZK', 'EMEA.*', 'amer=literal'], region: ['emea'], az: ['AZ-1', 'AZ-2']},
    service_roles: {...defaults().service_roles, kraft: 'controller', custom: 'other'}});
});

test('label lists support comma/newline separators without case folding or regex expansion', () => {
  assert.deepEqual(feature('parseList')('a, B\r\nc.*,,\n', 64, 'job'), ['a', 'B', 'c.*']);
  assert.deepEqual(frontend.parseList('', 64, 'job'), []);
});

test('label limits are 64 per label and 256 instances', () => {
  const form = draft();
  for (const label of Object.keys(form.filters)) {
    const max = label === 'instance' ? 256 : 64;
    form.filters[label] = Array.from({length: max}, (_, i) => `value-${i}`).join(',');
    assert.equal(feature('formToConfig')(form).filters[label].length, max);
    form.filters[label] += ',overflow';
    assert.throws(() => frontend.formToConfig(form), new RegExp(`${label}.*${max}`, 'i'));
    form.filters[label] = '';
  }
});

test('numeric timings accept their endpoints and reject non-integers and out-of-range values', () => {
  for (const [key, max] of [['timeout_seconds', 30], ['scrape_interval_seconds', 3600]]) {
    for (const value of ['1', String(max)]) {
      const form = draft(); form[key] = value;
      assert.equal(feature('formToConfig')(form)[key], Number(value));
    }
    for (const value of ['', '0', '-1', String(max + 1), '1.5', 'Infinity', 'NaN', 'five']) {
      const form = draft(); form[key] = value;
      assert.throws(() => feature('formToConfig')(form), /whole number|integer/i);
    }
  }
});

test('URL supports HTTP and HTTPS base addresses with optional ports and proxy paths', () => {
  for (const url of ['http://prometheus.example.test:9091/prometheus', 'https://prometheus.example.test', 'https://prometheus.example.test:9443/prometheus', 'https://prometheus.example.test/api/v10']) {
    const form = draft(); form.base_url = url;
    assert.equal(feature('formToConfig')(form).base_url, url);
  }
});

test('URL rejects unsupported schemes, credentials, queries and fragments for HTTP and HTTPS', () => {
  for (const url of ['', 'prometheus.example.test:9090', 'ftp://prometheus.example.test', 'javascript:alert(1)',
    ...['http', 'https'].flatMap(scheme => [`${scheme}://u:p@prometheus.example.test`, `${scheme}://@prometheus.example.test`, `${scheme}://prometheus.example.test?x=1`, `${scheme}://prometheus.example.test/#x`])]) {
    const form = draft(); form.base_url = url;
    assert.throws(() => feature('formToConfig')(form), /HTTP|URL|credentials|query|fragment/i);
  }
});

test('URL rejects /api/v1 endpoints including endpoints beneath a reverse proxy prefix', () => {
  for (const scheme of ['http', 'https']) {
    for (const suffix of ['/api/v1', '/api/v1/', '/api/v1/query', '/prometheus/api/v1/query']) {
      const form = draft(); form.base_url = `${scheme}://prometheus.example.test${suffix}`;
      assert.throws(() => feature('formToConfig')(form), /base URL|\/api\/v1|endpoint/i);
    }
  }
});

test('URL rejects port zero and control characters before browser URL parsing can discard them', () => {
  for (const scheme of ['http', 'https']) {
    for (const url of [`${scheme}://prometheus.example.test:0`, `${scheme}://prometheus.example.test:65536`,
      ...['\u0000', '\u0001', '\t', '\n', '\r', '\u007f'].flatMap(control => [
        `${scheme}://prometheus.example.test/prometheus${control}`, `${control}${scheme}://prometheus.example.test`])]) {
      const form = draft(); form.base_url = url;
      assert.throws(() => feature('formToConfig')(form), /URL|port|control/i);
    }
  }
});

test('service aliases are bounded, uniquely keyed, and use supported roles', () => {
  const form = draft();
  form.service_roles = Array.from({length: 32}, (_, i) => ({service: `service-${i}`, role: 'other'}));
  assert.equal(Object.keys(feature('formToConfig')(form).service_roles).length, 32);
  form.service_roles.push({service: 'extra', role: 'other'});
  assert.throws(() => frontend.formToConfig(form), /32/);
  for (const rows of [[{service: '', role: 'broker'}], [{service: 'kafka', role: 'invalid'}], [{service: 'kafka', role: 'broker'}, {service: 'kafka', role: 'other'}]]) {
    form.service_roles = rows;
    assert.throws(() => frontend.formToConfig(form), /service|role|duplicate/i);
  }
  form.service_roles = [{service: '__proto__', role: 'other'}];
  assert.equal(Object.hasOwn(frontend.formToConfig(form).service_roles, '__proto__'), true);
});

test('rendered form exposes bounded steppers, fixed authentication and exact label fields', () => {
  const html = feature('settingsHtml')({document: document(false), draft: feature('configToForm')(defaults()), busy: null, dirty: false});
  assert.match(html, /type="url"/);
  assert.match(html, /type="number"[^>]*min="1"[^>]*max="30"[^>]*step="1"/);
  assert.match(html, /type="number"[^>]*min="1"[^>]*max="3600"[^>]*step="1"/);
  assert.match(html, /Authentication[\s\S]*None/);
  assert.doesNotMatch(html, /<select[^>]*authentication/);
  for (const label of Object.keys(defaults().filters)) assert.match(html, new RegExp(`id="prom-filter-${label}"`));
  assert.match(html, /<table/);
  assert.match(html, /Stored for future data integration/);
  assert.match(html, /not.*ingest/i);
});

test('settings labels distinguish Prometheus saving and expected scrape metadata from cluster operations', () => {
  const html = feature('settingsHtml')({document: document(), draft: draft(), dirty: true, busy: null});
  assert.equal(html.match(/id="prom-save"[^>]*>([^<]+)/)?.[1], 'Save settings');
  assert.equal(html.match(/for="prom-scrape_interval_seconds">([^<]+)/)?.[1], 'Expected scrape interval (seconds)');
});

test('server text and draft values are escaped in HTML including path, errors and connection details', () => {
  const doc = document();
  doc.config_path = '/local/<img src=x onerror=alert(1)>.json';
  doc.config.base_url = 'http://example.test/" autofocus="true';
  doc.connection = {...connection('r1', 'failed'), message: '<script>bad</script>', checked_at: '<bad-time>', latency_ms: '<bad-latency>'};
  const form = feature('configToForm')(doc.config);
  form.filters.job = '</textarea><img src=x>';
  form.service_roles.push({service: '" onfocus="bad', role: 'other'});
  const state = {document: doc, draft: form, dirty: false, busy: null, error: '<b>conflict</b>'};
  const html = feature('settingsHtml')(state) + feature('statusHtml')(state);
  assert.doesNotMatch(html, /<img|<script|<bad-time>|<bad-latency>|<b>conflict|" autofocus="true|" onfocus="bad/);
  assert.match(html, /&lt;img/);
  assert.match(html, /&lt;script&gt;bad/);
  assert.match(html, /&lt;b&gt;conflict/);
});

test('configuration state stays separate from last test and edits hide saved success', () => {
  const doc = document(); doc.connection = connection('r1', 'connected');
  const state = {document: doc, dirty: false, busy: null};
  assert.match(feature('statusHtml')(state), /Configured/);
  assert.match(frontend.statusHtml(state), /Connected/);
  assert.match(frontend.statusHtml(state), /2026-09-14/);
  assert.match(frontend.statusHtml(state), /12 ms/);
  const dirty = frontend.statusHtml({...state, dirty: true});
  assert.match(dirty, /Configured/);
  assert.match(dirty, /Unsaved changes/);
  assert.doesNotMatch(dirty, /Connected|12 ms/);
  doc.connection.revision = 'old';
  assert.doesNotMatch(frontend.statusHtml(state), /Connected/);
  assert.match(frontend.statusHtml({document: document(false)}), /Not configured/);
});

test('backend UTC timestamps with fractional seconds remain explicitly labelled UTC', () => {
  const doc = document();
  doc.connection = {...connection('r1', 'connected'), checked_at: '2026-09-14T12:00:00.123456+00:00'};
  assert.match(feature('statusHtml')({document: doc}), /2026-09-14 12:00:00\.123 UTC/);
});

test('GET/PUT/POST use only the same-origin contract and save invalidates last test', async () => {
  const first = document(); first.connection = connection('r1', 'connected');
  const saved = document(true, 'r2'); saved.config.timeout_seconds = 7;
  const tested = {connection: connection('r2', 'connected')};
  const {instance, calls} = controller([response(first), response(saved), response(tested)]);
  await instance.load();
  instance.edit({...instance.state.draft, timeout_seconds: '7'});
  assert.equal(feature('controls')(instance.state).canTest, false);
  await instance.test(); assert.equal(calls.length, 1);
  await instance.save();
  assert.equal(instance.state.document.revision, 'r2');
  assert.equal(instance.state.document.connection.status, 'not_tested');
  assert.equal(instance.state.dirty, false);
  await instance.test();
  assert.equal(instance.state.document.connection.status, 'connected');
  assert.deepEqual(calls.map(call => [call.url, call.options.method]), [
    ['/api/settings/prometheus', 'GET'], ['/api/settings/prometheus', 'PUT'], ['/api/settings/prometheus/test', 'POST']]);
  assert.deepEqual(JSON.parse(calls[1].options.body), {config: saved.config, revision: 'r1'});
  assert.deepEqual(JSON.parse(calls[2].options.body), {revision: 'r2'});
  for (const {options} of calls) assert.equal(options.credentials, 'same-origin');
});

test('unsaved defaults cannot be tested and invalid saves do not send requests', async () => {
  const {instance, calls} = controller([response(document(false))]);
  await instance.load();
  await instance.test(); await instance.save();
  assert.equal(calls.length, 1);
  assert.match(instance.state.error, /HTTP|URL/i);
});

test('Save and Test lock during work and cannot overlap or overwrite a newer draft', async () => {
  const pending = deferred();
  const {instance, calls} = controller([response(document()), pending.promise]);
  await instance.load();
  instance.edit({...instance.state.draft, timeout_seconds: '6'});
  const save = instance.save();
  assert.equal(feature('controls')(instance.state).canSave, false);
  assert.equal(frontend.controls(instance.state).canTest, false);
  instance.edit({...instance.state.draft, timeout_seconds: '9'});
  assert.equal(instance.state.draft.timeout_seconds, '6');
  await instance.test(); await instance.save();
  assert.equal(calls.length, 2);
  const saved = document(true, 'r2'); saved.config.timeout_seconds = 6;
  pending.resolve(response(saved)); await save;
  assert.equal(instance.state.draft.timeout_seconds, '6');
});

test('conflicts retain the draft, explain recovery, and block writes/tests until reload', async () => {
  const {instance, calls} = controller([response(document()), response({detail: 'Stale <revision>'}, 409), response(document(true, 'r2'))]);
  await instance.load(); instance.edit({...instance.state.draft, timeout_seconds: '6'});
  await instance.save();
  assert.equal(instance.state.draft.timeout_seconds, '6');
  assert.match(instance.state.error, /changed|conflict/i);
  assert.match(instance.state.error, /reload/i);
  assert.match(feature('statusHtml')(instance.state), /&lt;revision&gt;/);
  assert.equal(frontend.controls(instance.state).canSave, false);
  await instance.test(); assert.equal(calls.length, 2);
  await instance.load();
  assert.equal(instance.state.document.revision, 'r2');
  assert.equal(instance.state.error, '');
});

test('HTTP validation, authorization, network and unreadable responses are readable and recoverable', async () => {
  for (const result of [response({detail: [{loc: ['body', 'config', 'base_url'], msg: 'Invalid <URL>'}]}, 422),
    response({detail: 'Invalid <URL>'}, 400), response({}, 401), response({}, 403), response({}, 500),
    response({detail: 'Two connection tests are already running. Try again shortly.'}, 429),
    response({detail: 'Cannot read Prometheus configuration. Check the JSON, file permissions, and configuration path.'}, 503),
    {ok: false, status: 502, json: async () => {throw new Error('not JSON');}}]) {
    const {instance} = controller([result]); await instance.load();
    assert.equal(instance.state.busy, null);
    assert.ok(instance.state.error);
    assert.doesNotMatch(instance.state.error, /\[object Object\]/);
    assert.doesNotMatch(feature('statusHtml')(instance.state), /<URL>/);
  }
  const {instance} = controller([], {request: async () => {throw new TypeError('Failed to fetch');}});
  await instance.load(); assert.match(instance.state.error, /Failed to fetch|server/i);
});

test('non-admin mounts never request configuration or expose the editor', async () => {
  const {instance, calls} = controller([], {isAdmin: false});
  await instance.load(); await instance.save(); await instance.test();
  assert.equal(calls.length, 0);
  assert.match(feature('settingsHtml')(instance.state), /admin/i);
  assert.doesNotMatch(frontend.settingsHtml(instance.state), /<form/);
});

for (const action of ['load', 'save', 'test']) {
  test(`${action} completion cannot render after navigation away`, async () => {
    const pending = deferred(); let current = true;
    const {instance, updates} = controller(action === 'load' ? [pending.promise] : [response(document()), pending.promise], {isCurrent: () => current});
    if (action !== 'load') await instance.load();
    if (action === 'save') instance.edit({...instance.state.draft, timeout_seconds: '6'});
    const work = instance[action](); const before = updates.length;
    current = false;
    pending.resolve(action === 'test' ? response({connection: connection('r1', 'connected')}) : response(document(true, 'r2')));
    await work;
    assert.equal(updates.length, before);
  });
}

test('older GET completion cannot replace a later request, including its error/finally path', async () => {
  for (const oldResponse of [response(document(true, 'old')), response({detail: 'old failure'}, 500)]) {
    const pending = deferred();
    const {instance, updates} = controller([pending.promise, response(document(true, 'new'))]);
    const old = instance.load(); await instance.load();
    const before = updates.length;
    pending.resolve(oldResponse); await old;
    assert.equal(instance.state.document.revision, 'new');
    assert.equal(updates.length, before);
    assert.equal(instance.state.error, '');
  }
});

test('mismatched POST revision cannot show a successful test of this config', async () => {
  const {instance} = controller([response(document()), response({connection: connection('old', 'connected')})]);
  await instance.load(); await instance.test();
  assert.doesNotMatch(feature('statusHtml')(instance.state), /Connected/);
  assert.match(instance.state.error, /revision|changed|reload/i);
});

// Minimal DOM boundary for event and lifetime tests; no browser or added dependencies.
function mountRoot() {
  const nodes = new Map();
  const root = {id: 'prometheus-settings', isConnected: true, innerHTML: '',
    querySelector: selector => {
      if (!nodes.has(selector)) nodes.set(selector, {innerHTML: '', textContent: '', value: '', hidden: false, disabled: false, focus() {}});
      return nodes.get(selector);
    },
    querySelectorAll: () => root.rows,
    setAttribute() {}, rows: [],
  };
  root.ownerDocument = {getElementById: () => root};
  root.seed = form => {
    for (const key of ['base_url', 'timeout_seconds', 'scrape_interval_seconds']) root.querySelector(`#prom-${key}`).value = form[key];
    for (const [key, value] of Object.entries(form.filters)) root.querySelector(`#prom-filter-${key}`).value = value;
    root.rows = form.service_roles.map(row => ({querySelector: selector => ({value: selector === '[data-prom-service]' ? row.service : row.role})}));
  };
  return root;
}

test('mounted Save settings label is preserved across edits, saving and completion', async () => {
  const root = mountRoot(), pending = deferred();
  const results = [response(document()), pending.promise];
  const mounted = feature('mount')(root, {isAdmin: true, isCurrent: () => true, request: async () => results.shift()});
  await mounted.ready;
  assert.equal(root.querySelector('#prom-save').textContent, 'Save settings');
  mounted.edit({...mounted.state.draft, timeout_seconds: '6'});
  assert.equal(root.querySelector('#prom-save').textContent, 'Save settings');
  const work = mounted.save();
  assert.equal(root.querySelector('#prom-save').textContent, 'Saving settings...');
  const saved = document(true, 'r2'); saved.config.timeout_seconds = 6;
  pending.resolve(response(saved)); await work;
  assert.equal(root.querySelector('#prom-save').textContent, 'Save settings');
});

test('mount reads live controls, submits the form, and keeps ordinary typing in place', async () => {
  const root = mountRoot(), calls = [];
  const saved = document(true, 'r2'); saved.config.base_url = 'http://prometheus.example.test:9091';
  const results = [response(document()), response(saved)];
  const mounted = feature('mount')(root, {isAdmin: true, isCurrent: () => true,
    request: async (url, options) => {calls.push({url, options}); return results.shift();}});
  await mounted.ready;
  root.seed(draft());
  const before = root.innerHTML;
  root.querySelector('#prom-base_url').value = saved.config.base_url;
  root.oninput({target: {}});
  assert.equal(root.innerHTML, before, 'typing does not replace the editor or move the caret');
  assert.equal(mounted.state.dirty, true);
  let prevented = false;
  await root.onsubmit({preventDefault() {prevented = true;}});
  assert.equal(prevented, true);
  assert.deepEqual(JSON.parse(calls[1].options.body), {config: saved.config, revision: 'r1'});
});

test('mount DOM identity guards reject detached and superseded roots', async () => {
  for (const leave of [root => {root.isConnected = false;}, root => {root.ownerDocument.getElementById = () => ({});}]) {
    const root = mountRoot(), pending = deferred();
    const mounted = feature('mount')(root, {isAdmin: true, isCurrent: () => true, request: () => pending.promise});
    const before = root.innerHTML;
    leave(root); pending.resolve(response(document())); await mounted.ready;
    assert.equal(root.innerHTML, before);
  }
  const root = mountRoot(), pending = deferred();
  const old = feature('mount')(root, {isAdmin: true, isCurrent: () => true, request: () => pending.promise});
  const current = frontend.mount(root, {isAdmin: true, isCurrent: () => true, request: async () => response(document(true, 'new'))});
  await current.ready;
  const before = root.innerHTML;
  pending.resolve(response(document(true, 'old'))); await old.ready;
  assert.equal(root.innerHTML, before);
  assert.equal(current.state.document.revision, 'new');
});

function page(withModule = false) {
  const elements = new Map();
  const get = id => {
    if (!elements.has(id)) elements.set(id, {innerHTML: '', style: {}, classList: {contains: () => false, toggle() {}, add() {}, remove() {}}, focus() {}, addEventListener() {}});
    return elements.get(id);
  };
  const html = fs.readFileSync(path.join(root, 'frontend/index.html'), 'utf8');
  const context = {document: {body: get('body'), getElementById: get, querySelectorAll: () => []},
    localStorage: {getItem() {}, setItem() {}}, setInterval() {}, clearInterval() {}, setTimeout() {},
    Dashboard: require(path.join(root, 'frontend/dashboard.js')), fetch: async () => response({clusters: []}),
    Chart: class {static defaults = {font: {}};}, console, alert: msg => {throw new Error(msg);}};
  const mounts = [];
  if (withModule) context.PrometheusSettings = {mount: (...args) => mounts.push(args)};
  vm.createContext(context);
  vm.runInContext([...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]).join('\n').replace(/^boot\(\);$/m, ''), context);
  return {run: code => vm.runInContext(code, context), get, context, mounts};
}

test('Settings integrates local assets and safely tolerates an absent module in existing VM tests', () => {
  const html = fs.readFileSync(path.join(root, 'frontend/index.html'), 'utf8');
  assert.match(html, /src="\/assets\/prometheus-settings\.js"/);
  assert.match(html, /href="\/assets\/prometheus-settings\.css"/);
  const p = page();
  p.run("CURRENT_ROLE='admin';SELECTED='settings';renderSettingsView([])");
  assert.match(p.get('main').innerHTML, /prometheus-settings/);
  for (const marker of ['obYaml', 'obSaveBtn', 'obSubmit', 'settingsClusterList']) assert.ok(p.get('main').innerHTML.includes(marker));
  assert.equal(p.run('typeof editClusterConfig'), 'function');
  assert.equal(p.run('typeof collectSelectedCluster'), 'function');
});

test('admin Settings mounts with navigation guard; non-admin entry does not fetch', async () => {
  const p = page(true);
  p.run("CURRENT_ROLE='admin';SELECTED='settings';renderSettingsView([])");
  assert.equal(p.mounts.length, 1);
  assert.equal(p.mounts[0][1].isCurrent(), true);
  p.run('SELECTED=null'); assert.equal(p.mounts[0][1].isCurrent(), false);
  let calls = 0; p.context.fetch = async () => {calls++; return response({clusters: []});};
  p.run("CURRENT_ROLE='viewer'"); await p.run('openSettings()');
  assert.equal(calls, 0);
});

test('late Settings load cannot navigate back from another view', async () => {
  const p = page(true), pending = deferred();
  p.context.fetch = async () => pending.promise;
  p.run("CURRENT_ROLE='admin';renderTree=()=>{}");
  const work = p.run('openSettings()');
  p.run("SELECTED=null;document.getElementById('main').innerHTML='Fleet'");
  pending.resolve(response({clusters: []})); await work;
  assert.equal(p.get('main').innerHTML, 'Fleet');
  assert.equal(p.mounts.length, 0);
});

test('cluster deletion refreshes only cluster controls without discarding Prometheus drafts or navigating back', async () => {
  for (const selected of ['settings', null]) {
    const p = page();
    p.run(`SELECTED=${JSON.stringify(selected)};CLUSTER_TO_DELETE='old';SELECTED_CLUSTER='old';EDITING_CLUSTER='old';load=()=>{};document.getElementById('main').innerHTML='Keep current view and draft'`);
    await p.run('executeClusterDeletion()');
    assert.equal(p.get('main').innerHTML, 'Keep current view and draft');
    if (selected === 'settings') assert.equal(p.run('SELECTED_CLUSTER'), null);
  }
});
