# Prometheus Settings Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development and test-driven-development. Keep edits disjoint and preserve the existing uncommitted diagnostics changes.

**Goal:** Configure the user-approved HTTP/no-auth Prometheus connection through Settings, with an explicit file location and honest connection-test state.

**Architecture:** Store this app's connection settings in a separate, Git-ignored `prometheus.json` at project root, overridable with `GC_PROMETHEUS_CONFIG`. Add admin-only read/save/test APIs and an unframed Prometheus section in existing Settings. Do not change the external scrape server, poll telemetry automatically, or fabricate cluster mappings.

**Tech Stack:** Existing FastAPI, Python standard library, JSON, and same-origin JavaScript/CSS. No new runtime dependencies.

## Contract

Configuration keys: `version:1`, `base_url`, `authentication:"none"`, `timeout_seconds:5`, `scrape_interval_seconds:60`, `filters` keyed by `job,region,tier,infra,az,service,instance` (arrays of exact raw values), and `service_roles` (raw service -> analyzer role).

Defaults preserve EMEA/AMER/APAC, DEV/UAT/PROD/sandbox/stage, icp/phy, and kafka/connect/registry/zookeeper. Empty job, AZ, and instance arrays impose no selection. The internal address is supplied only in the local ignored config, never a committed default or test fixture. Cross-region ZooKeeper placement is intentional; no region/job mismatch rejection.

GET `/api/settings/prometheus` returns `{config,config_path,saved,configured,revision,connection}`. `connection` is `{status:"not_tested"|"connected"|"failed",checked_at,message,revision,latency_ms}` and describes the last test of that revision, not continuous monitoring. PUT accepts `{config,revision}` with conflict detection and atomic restrictive-permission persistence. POST `/api/settings/prometheus/test` accepts `{revision}`, tests only the saved configuration, and never saves unsaved form values. All three require admin. Tests query `vector(1)` through the documented instant-query API, not the target inventory; no redirects or authentication forwarding.

Source: https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries (retrieved through local Firecrawl). This test proves query API access only, not Kafka series availability.

## Tasks

- [x] Backend: write failing tests for defaults, strict validation, bounded JSON, atomic saves, permissions, stale revision, invalid existing files, HTTP result validation, no redirects, failure/timeout handling, auth, and no incidental telemetry writes. Implement `gcanalyzer/prometheus_settings.py`, `gcanalyzer/prometheus_client.py`, and thin APIs in `app.py`. Focused suite: 43 passed; independent review found no actionable issues.
- [x] Frontend: add `frontend/prometheus-settings.js` and `.css`, integrate into `renderSettingsView`, preserve cluster workflows. Show saved versus last-tested state and actual config path. Edit URL, numeric timing controls, exact-value label lists, and service aliases. Save/Test disabled during work; edits invalidate displayed test success. Escape server-provided text and tolerate failures. Add focused Node tests before implementation. HTTP/HTTPS validation matches the backend; the user's local address remains HTTP. 33 focused Node tests plus 27 existing tests pass.
- [x] Local setup/docs: ignore `prometheus.json`, add a generic example JSON and environment-variable example, create the user's local HTTP config, update README/user guide and private requirements notes. Explain that filters and mappings are stored for later integration, not live telemetry ingestion.
- [x] Verify: full Python and JavaScript suites, focused code review, browser save/reload/validation/test on the isolated review server. Verify narrow/mobile layout, keep live collection disabled, and report whether the internal endpoint is reachable. Do not commit or push.

## Verification Record

- Python suite: 288 passed; one existing Starlette TestClient deprecation warning.
- JavaScript suites: 60 passed (33 Prometheus, 27 existing dashboard/investigation tests).
- Static export asset regression reproduced, fixed with the explicit JS/CSS allowlist, and verified; exports do not read the private connection settings.
- Backend and frontend/spec reviews: no actionable findings.
- Browser: admin Settings displays the private JSON path and all saved fields. Changed timeout from 5 to 6, saved and reloaded to confirm persistence, then restored 5. URL normalization and unsupported-scheme validation verified. Unsaved changes disable testing and hide earlier test results.
- Browser visuals: effective CSS viewports 1440x1000 and 390x844, no section overflow; light/dark themes checked, no JavaScript error logs. Temporary viewport overrides reset and the original light theme restored.
- Saved endpoint test: failed DNS resolution from this Mac, with an explicit failure message and timestamp. This does not validate live metric availability. No network restrictions or external Prometheus configuration were changed.
- Isolated review app remains at `http://127.0.0.1:8085/`, demo data only, scheduler disabled. Private JSON permissions are 0600 and Git ignore is verified. No commit, push, or packaging performed.

## Boundaries

Excel inventory is deferred until cluster-specific metric analysis. No hostname transcription from photographs, cluster inference, Prometheus scrape changes, deployment package rebuild, or auth feature expansion. Generic example and documentation must not expose the internal address. Existing dirty files and unrelated user changes remain untouched.
