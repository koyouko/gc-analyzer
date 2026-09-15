/* Configuration only: Prometheus requests are made by the application server. */
(() => {
  'use strict';
  const API = '/api/settings/prometheus';
  const LABELS = ['job', 'region', 'tier', 'infra', 'az', 'service', 'instance'];
  const ROLES = ['broker', 'connect', 'schema-registry', 'zookeeper', 'controller', 'other'];
  const mounts = new WeakMap();
  const escape = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[char]));

  function parseList(value, limit, label) {
    const values = [...new Set(String(value).split(/[,\r\n]/).map(item => item.trim()).filter(Boolean))];
    if (values.length > limit) throw new Error(`${label}: use at most ${limit} exact values.`);
    return values;
  }

  function configToForm(config) {
    return {base_url: config.base_url, timeout_seconds: String(config.timeout_seconds),
      scrape_interval_seconds: String(config.scrape_interval_seconds),
      filters: Object.fromEntries(LABELS.map(label => [label, config.filters[label].join(', ')])),
      service_roles: Object.entries(config.service_roles).map(([service, role]) => ({service, role}))};
  }

  function formToConfig(form) {
    if (/[\x00-\x1f\x7f]/.test(form.base_url)) throw new Error('The URL cannot contain control characters.');
    const base_url = form.base_url.trim();
    let url;
    try { url = new URL(base_url); } catch (_) { throw new Error('Enter a complete HTTP or HTTPS URL, including the host and optional port.'); }
    if (!['http:', 'https:'].includes(url.protocol) || !url.hostname || !/^https?:\/\//i.test(base_url)) throw new Error('The Prometheus URL must use HTTP or HTTPS.');
    if (url.username || url.password || base_url.includes('@')) throw new Error('The URL cannot contain credentials. Authentication is None.');
    if (base_url.includes('?') || base_url.includes('#')) throw new Error('The URL cannot contain a query or fragment.');
    if (/\s|\\/.test(base_url)) throw new Error('The URL cannot contain whitespace or backslashes.');
    if (url.port && (Number(url.port) < 1 || Number(url.port) > 65535)) throw new Error('The URL port must be between 1 and 65535.');
    if (/\/api\/v1(?:\/|$)/.test(url.pathname)) throw new Error('Enter the server base URL, not an /api/v1 endpoint.');
    const integer = (value, max, label) => {
      const number = Number(value);
      if (String(value).trim() === '' || !Number.isInteger(number) || number < 1 || number > max) throw new Error(`${label}: enter a whole number from 1 to ${max}.`);
      return number;
    };
    if (form.service_roles.length > 32) throw new Error('Use at most 32 service mappings.');
    const seen = new Set();
    const entries = form.service_roles.map(({service, role}) => {
      service = service.trim();
      if (!service) throw new Error('Each service mapping needs an exact service value.');
      if (seen.has(service)) throw new Error(`Duplicate service mapping: ${service}.`);
      if (!ROLES.includes(role)) throw new Error(`Unsupported role for service ${service}.`);
      seen.add(service);
      return [service, role];
    });
    return {version: 1, base_url, authentication: 'none',
      timeout_seconds: integer(form.timeout_seconds, 30, 'Timeout'),
      scrape_interval_seconds: integer(form.scrape_interval_seconds, 3600, 'Expected scrape interval'),
      filters: Object.fromEntries(LABELS.map(label => [label, parseList(form.filters[label], label === 'instance' ? 256 : 64, label)])),
      service_roles: Object.fromEntries(entries)};
  }

  function controls(state) {
    const ready = state.isAdmin !== false && !!state.document && !state.busy && !state.conflict;
    return {canSave: ready && (state.dirty || !state.document.saved),
      canTest: ready && !state.dirty && state.document.saved && state.document.configured};
  }

  function statusHtml(state) {
    const doc = state.document;
    const connection = doc?.connection;
    const currentTest = doc?.saved && !state.dirty && !state.conflict && !state.testUnavailable && connection?.revision === doc.revision;
    let testLabel = 'Not tested', testClass = '', detail = '';
    if (state.dirty) detail = 'Save changes before testing.';
    else if (!doc?.saved) detail = 'Save a configuration before testing.';
    else if (state.testUnavailable) detail = 'Last test could not be confirmed. Test again.';
    else if (currentTest && ['connected', 'failed'].includes(connection.status)) {
      testLabel = connection.status === 'connected' ? 'Connected' : 'Failed';
      testClass = connection.status;
      const checked = connection.checked_at ? new Date(connection.checked_at) : null;
      const timestamp = checked && Number.isFinite(checked.getTime()) ? checked.toISOString().replace('T', ' ').replace(/(?:\.000)?Z$/, ' UTC') : 'Time unavailable';
      const latency = Number.isFinite(connection.latency_ms) && connection.latency_ms >= 0 ? ` / ${connection.latency_ms} ms` : '';
      detail = `${timestamp}${latency}${connection.message ? ` / ${connection.message}` : ''}`;
    }
    if (state.busy === 'test') { testLabel = 'Testing...'; testClass = ''; detail = 'Checking the saved query API connection.'; }
    return `${doc ? `<dl class="prom-state-grid"><div><dt>Configuration</dt><dd>${doc.configured ? 'Configured' : 'Not configured'}</dd><p class="muted">${state.dirty ? 'Unsaved changes' : doc.saved ? 'Saved' : 'Not saved'}</p></div>
      <div><dt>Last connection test</dt><dd class="${testClass}">${testLabel}</dd><p class="muted">${escape(detail)}</p></div></dl>` : ''}
      ${state.error ? `<p class="prom-error" role="alert">${escape(state.error)}</p>` : ''}
      ${state.busy === 'load' ? '<p class="muted">Loading Prometheus settings...</p>' : ''}
      ${state.busy === 'save' ? '<p class="muted">Saving configuration...</p>' : ''}
      ${state.notice ? `<p class="muted">${escape(state.notice)}</p>` : ''}`;
  }

  function roleRowsHtml(rows) {
    return rows.map((row, index) => `<tr data-prom-role-row><td><input data-prom-service aria-label="Service value ${index + 1}" type="text" value="${escape(row.service)}" required spellcheck="false" autocomplete="off"></td>
      <td><select data-prom-role aria-label="Analyzer role ${index + 1}">${ROLES.map(role => `<option value="${role}"${role === row.role ? ' selected' : ''}>${role}</option>`).join('')}</select></td>
      <td><button type="button" class="btn ghost prom-icon" data-prom-remove="${index}" title="Remove service mapping ${index + 1}" aria-label="Remove service mapping ${index + 1}">&times;</button></td></tr>`).join('');
  }

  function settingsHtml(state) {
    if (state.isAdmin === false) return '<p class="muted">Prometheus settings require an administrator.</p>';
    const doc = state.document, form = state.draft;
    const available = controls(state);
    return `<h2 id="prom-title">Prometheus</h2>
      ${doc ? `<p class="prom-path">Configuration file <code>${escape(doc.config_path)}</code></p>` : ''}
      <div id="prom-status" role="status" aria-live="polite">${statusHtml(state)}</div>
      <button type="button" class="btn ghost" id="prom-reload"${!state.error && !state.conflict ? ' hidden' : ''}${state.busy ? ' disabled' : ''}>Reload saved settings</button>
      ${doc && form ? `<form id="prom-form"><fieldset id="prom-fields"${state.busy ? ' disabled' : ''}><legend class="prom-sr-only">Prometheus configuration</legend>
        <div class="prom-connection-fields">
          <label class="prom-url" for="prom-base_url">Prometheus URL<input id="prom-base_url" type="url" required spellcheck="false" autocomplete="off" placeholder="http://host:9090" value="${escape(form.base_url)}"></label>
          <div class="prom-auth"><span>Authentication</span><strong>None</strong></div>
          <label for="prom-timeout_seconds">Timeout (seconds)<input id="prom-timeout_seconds" type="number" min="1" max="30" step="1" required value="${escape(form.timeout_seconds)}"></label>
          <label for="prom-scrape_interval_seconds">Expected scrape interval (seconds)<input id="prom-scrape_interval_seconds" type="number" min="1" max="3600" step="1" required value="${escape(form.scrape_interval_seconds)}"></label>
        </div>
        <div class="prom-scope-heading"><h3>Label scope</h3><span class="muted">Exact values, comma or newline separated</span></div>
        <div class="prom-filter-grid">${LABELS.map(label => `<label for="prom-filter-${label}"${label === 'instance' ? ' class="prom-instances"' : ''}><span>${label}<small>Maximum ${label === 'instance' ? 256 : 64}</small></span>
          <textarea id="prom-filter-${label}" rows="2" spellcheck="false" placeholder="All values">${escape(form.filters[label])}</textarea></label>`).join('')}</div>
        <div class="prom-scope-heading"><h3>Service mapping</h3><span class="muted">Maximum 32</span></div>
        <table class="prom-role-table"><thead><tr><th scope="col">Service value</th><th scope="col">Analyzer role</th><th scope="col"><span class="prom-sr-only">Remove</span></th></tr></thead><tbody id="prom-role-rows">${roleRowsHtml(form.service_roles)}</tbody></table>
        <button type="button" class="btn ghost prom-icon" id="prom-add-role" title="Add service mapping" aria-label="Add service mapping"${form.service_roles.length >= 32 ? ' disabled' : ''}>+</button>
        <p class="muted prom-scope-note">Stored for future data integration. These settings do not ingest telemetry or change Prometheus scrapes.</p>
      </fieldset>
      <div class="prom-actions"><button type="submit" class="btn" id="prom-save"${available.canSave ? '' : ' disabled'}>Save settings</button>
        <button type="button" class="btn ghost" id="prom-test"${available.canTest ? '' : ' disabled'}>Test connection</button>
        <span id="prom-test-hint" class="muted">${state.dirty || !doc.saved ? 'Save before testing.' : 'Tests the saved query API, not metric availability or continuous health.'}</span></div></form>` : ''}`;
  }

  function errorDetail(data, status) {
    if (typeof data?.detail === 'string') return data.detail;
    if (Array.isArray(data?.detail)) return data.detail.map(item => `${Array.isArray(item.loc) ? `${item.loc.join('.')}: ` : ''}${item.msg || 'Invalid value'}`).join('; ');
    if (status === 401) return 'Your session has expired. Sign in again.';
    if (status === 403) return 'An administrator account is required.';
    return `The application server returned HTTP ${status}.`;
  }

  function createController({isAdmin = false, request = (...args) => fetch(...args), isCurrent = () => true, onChange = () => {}} = {}) {
    const state = {isAdmin, document: null, draft: null, dirty: false, busy: null, error: '', notice: '', conflict: false, testUnavailable: false};
    let token = 0;
    const emit = rebuild => { if (isCurrent()) onChange(state, rebuild); };
    async function run(action) {
      if (!isAdmin || !isCurrent()) return;
      const buttons = controls(state);
      if (action === 'save' && !buttons.canSave || action === 'test' && !buttons.canTest) return;
      let body;
      try {
        if (action === 'save') body = {config: formToConfig(state.draft), revision: state.document.revision};
        if (action === 'test') body = {revision: state.document.revision};
      } catch (error) {state.error = error.message; emit(false); return;}
      const requestToken = ++token;
      const current = () => isCurrent() && requestToken === token;
      state.busy = action; state.error = ''; state.notice = '';
      emit(false);
      let rebuild = false;
      try {
        const response = await request(action === 'test' ? `${API}/test` : API, {
          method: {load: 'GET', save: 'PUT', test: 'POST'}[action], credentials: 'same-origin', cache: 'no-store',
          headers: {'Accept': 'application/json', ...(body ? {'Content-Type': 'application/json'} : {})},
          ...(body ? {body: JSON.stringify(body)} : {}),
        });
        const data = await response.json().catch(() => null);
        if (!current()) return;
        if (!response.ok) {
          const error = new Error(errorDetail(data, response.status));
          error.status = response.status; throw error;
        }
        if (action === 'test') {
          if (!data?.connection || !['not_tested', 'connected', 'failed'].includes(data.connection.status)) throw new Error('The application server returned an invalid connection result.');
          if (data.connection.revision !== state.document.revision) {
            const error = new Error('Connection test revision no longer matches.'); error.status = 409; throw error;
          }
          state.document = {...state.document, connection: data.connection};
          state.testUnavailable = false;
        } else {
          if (!data?.config || typeof data.config_path !== 'string' || typeof data.saved !== 'boolean' || typeof data.configured !== 'boolean' || !Object.hasOwn(data, 'revision') || !data.connection) throw new Error('The application server returned an invalid settings document. Reload saved settings.');
          const draft = configToForm(data.config);
          state.document = data; state.draft = draft; state.dirty = false;
          state.conflict = false; state.testUnavailable = false;
          if (action === 'save') state.notice = 'Configuration saved. Connection not yet tested.';
          rebuild = true;
        }
      } catch (error) {
        if (!current()) return;
        if (error.status === 409) {
          state.conflict = true;
          state.error = `Settings changed on the server. Reload saved settings before saving or testing again. ${error.message}`;
        } else state.error = error.message || 'Unable to reach the application server.';
        if (action === 'test') state.testUnavailable = true;
      } finally {
        if (current()) {state.busy = null; emit(rebuild);}
      }
    }
    return {state, load: () => run('load'), save: () => run('save'), test: () => run('test'),
      edit(draft) {
        if (!isAdmin || !isCurrent() || state.busy || !state.document) return;
        state.draft = draft;
        state.dirty = JSON.stringify(draft) !== JSON.stringify(configToForm(state.document.config));
        state.error = state.conflict ? state.error : ''; state.notice = '';
        emit(false);
      }};
  }

  function mount(root, options = {}) {
    if (!root) return null;
    const token = {};
    mounts.set(root, token);
    const current = () => mounts.get(root) === token && root.isConnected && root.ownerDocument.getElementById(root.id) === root && (!options.isCurrent || options.isCurrent());
    let rendered = false;
    const find = selector => root.querySelector(selector);
    const controller = createController({...options, isCurrent: current, onChange(state, rebuild) {
      if (!current()) return;
      if (rebuild || !rendered) {root.innerHTML = settingsHtml(state); rendered = true;}
      else find('#prom-status').innerHTML = statusHtml(state);
      root.setAttribute('aria-busy', state.busy ? 'true' : 'false');
      const available = controls(state);
      if (find('#prom-fields')) find('#prom-fields').disabled = !!state.busy;
      if (find('#prom-save')) {find('#prom-save').disabled = !available.canSave; find('#prom-save').textContent = state.busy === 'save' ? 'Saving settings...' : 'Save settings';}
      if (find('#prom-test')) {find('#prom-test').disabled = !available.canTest; find('#prom-test').textContent = state.busy === 'test' ? 'Testing...' : 'Test connection';}
      if (find('#prom-test-hint') && state.document) find('#prom-test-hint').textContent = state.dirty || !state.document.saved ? 'Save before testing.' : 'Tests the saved query API, not metric availability or continuous health.';
      if (find('#prom-add-role')) find('#prom-add-role').disabled = !!state.busy || state.draft.service_roles.length >= 32;
      if (find('#prom-reload')) {find('#prom-reload').hidden = !state.error && !state.conflict; find('#prom-reload').disabled = !!state.busy;}
    }});
    function readForm() {
      return {base_url: find('#prom-base_url').value, timeout_seconds: find('#prom-timeout_seconds').value,
        scrape_interval_seconds: find('#prom-scrape_interval_seconds').value,
        filters: Object.fromEntries(LABELS.map(label => [label, find(`#prom-filter-${label}`).value])),
        service_roles: Array.from(root.querySelectorAll('[data-prom-role-row]'), row => ({
          service: row.querySelector('[data-prom-service]').value, role: row.querySelector('[data-prom-role]').value}))};
    }
    root.oninput = root.onchange = () => {if (current() && controller.state.draft) controller.edit(readForm());};
    root.onsubmit = async event => {event.preventDefault(); if (current()) {controller.edit(readForm()); await controller.save();}};
    root.onclick = async event => {
      if (!current() || controller.state.busy) return;
      const button = event.target.closest('button');
      if (!button) return;
      if (button.id === 'prom-test') {controller.edit(readForm()); await controller.test();}
      else if (button.id === 'prom-reload') {
        if (!controller.state.dirty || root.ownerDocument.defaultView.confirm('Discard unsaved Prometheus changes and reload the saved settings?')) await controller.load();
      } else if (button.id === 'prom-add-role' || button.hasAttribute('data-prom-remove')) {
        const draft = readForm();
        let focusIndex;
        if (button.id === 'prom-add-role') {
          if (draft.service_roles.length >= 32) return;
          draft.service_roles.push({service: '', role: 'other'}); focusIndex = draft.service_roles.length - 1;
        } else {
          const index = Number(button.getAttribute('data-prom-remove'));
          draft.service_roles.splice(index, 1); focusIndex = Math.min(index, draft.service_roles.length - 1);
        }
        controller.edit(draft);
        find('#prom-role-rows').innerHTML = roleRowsHtml(draft.service_roles);
        const row = root.querySelectorAll('[data-prom-role-row]')[focusIndex];
        (row ? row.querySelector('[data-prom-service]') : find('#prom-add-role')).focus();
      }
    };
    if (!options.isAdmin) {root.innerHTML = settingsHtml(controller.state); controller.ready = Promise.resolve();}
    else controller.ready = controller.load();
    return controller;
  }

  const exported = {configToForm, formToConfig, parseList, controls, settingsHtml, statusHtml, createController, mount};
  if (typeof module === 'object' && module.exports) module.exports = exported;
  if (typeof window !== 'undefined') window.PrometheusSettings = exported;
})();
