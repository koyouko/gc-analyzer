# Diagnostics Improvements Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or superpowers:executing-plans. Work in disjoint files and run failing regression tests before production edits.

**Goal:** Deliver the approved dashboard and reliable GC/SAR and isolated ad-hoc diagnostics without requiring Prometheus access.

**Architecture:** Preserve the modular Python service and its same-origin static dashboard. Add an isolated, stateless investigation endpoint, explicit source quality, bounded collection, and conservative advisory forecasts. No external monitoring configuration is changed.

**Tech Stack:** FastAPI, SQLite, Python standard library, Chart.js, existing JavaScript dashboard.

## Approved Scope

- [x] GC correctness: preserve relative time, recognize pause-only records, deduplicate rotations/restarts, reject empty health grading, keep consumed offsets transactional and partial records retryable, bound SSH commands.
- [x] SAR correctness: fixed ingestion grid, late-arrival deduplication, AM/PM/timezone handling, missing-data preservation, live versus historical clocks and source freshness.
- [x] Advice: healthy/unknown correlation states, stale/partial-data gates, cautious Kafka wording, forecast coverage gates, flat-baseline noise floors, explicit actual model status.
- [x] Dashboard: approved navy/cyan layout, broker comparison table, data quality and concise diagnostic summary, aligned GC/host evidence, retain settings/jobs/auth/theme/time-range workflows.
- [x] Ad-hoc: authorized bounded multi-file/gzip analysis, optional SAR and supplied UTC start anchor, no live history/training writes, downloadable evidence report.
- [x] Runtime/docs: local static assets, separate install/demo from ordinary startup, accurate README/user guide, targeted tests and browser checks.
- [x] Review: inspect combined diff, run full tests and browser workflows on an isolated database, report any remaining limitations.

## Ownership and Contracts

1. GC worker: `parser.py`, `analyzer.py`, `collector.py`, `scheduler.py`, `ingest.py`, dedicated regression tests. Preserve public signatures with optional keyword additions. Unknown health uses grade `?`, status `unknown`, score null. Parser never invents absolute timestamps; optional `start_time` epoch anchors uptime. `parse_file` supports bounded gzip. Shared result remains `metrics`, `health`, `findings`, `timeline`, `warnings`.
2. SAR worker: `sar_parser.py`, `sar_analyzer.py`, `sar_ingest.py`, `store.py`, SAR/quality regression tests. Snapshot `quality` has `state`, `last_observed_at`, `age_seconds`, and available/missing metrics as applicable. Preserve nullable fields through reads. Explicit `now` is historical; implicit live time is wall clock unless explicitly configured demo mode.
3. Advice worker: `correlate.py`, `scaling_advisor.py`, `forecast.py`, `ml_insights.py`, focused regression tests. Read `quality` where present, tolerate nulls, do not claim causality from association. Withhold predictions for stale, insufficient, or sparse history; keep all model output advisory.
4. UI worker: `frontend/index.html`, new `frontend/dashboard.css` and `frontend/dashboard.js` if useful, frontend tests. Preserve legacy controls; use explicit `?`/unknown states and source-quality fields. Add an `Analyze logs` navigation command calling `openInvestigation()` implemented separately.
5. Integrator: `app.py`, new `investigations.py`, `frontend/investigations.js`, startup scripts, local assets, documentation, integration tests, screenshots. Own cross-worker compatibility and final review.

## Verification

For each regression, first reproduce the failure with an isolated temporary database or in-memory fixture; implement and rerun the targeted tests. Full suite command in this environment:

```sh
.venv/bin/python -c "import sys; sys.path.append('/opt/homebrew/lib/python3.14/site-packages'); import pytest; raise SystemExit(pytest.main(['-q']))"
```

Ad-hoc contract: POST `/api/investigations/analyze`, existing authenticated sessions required. JSON `files` contains `{name, content, encoding}` with text or base64 gzip, optional `sar` export and optional `start_time` ISO-8601 with timezone. Return one analysis per file group, source quality, UTC/relative time basis, and actionable concise findings. Enforce per-request and decompressed limits. No persistence to fleet history or training. Browser must render untrusted names/results using escaped text and export escaped standalone HTML.

Run the server with scheduler disabled on an isolated temporary database for validation. Verify login, fleet/cluster/node views, theme, SAR/no-data state, ad-hoc valid/invalid files and report export at desktop and narrow widths. Do not start production SSH collection.

### Verification Notes

- Live browser checks use `http://127.0.0.1:8085` with `/tmp/gc-analyzer-review-20260912/review.db`, an empty config directory, demo mode, and scheduler disabled. No production collection was run.
- Verified login, fleet/cluster/broker views, sorting, model selection/status, settings, jobs, responsive dark/light layouts, and real sample GC/SAR uploads. A separate Full-GC upload displays its specific recommendation.
- Standalone dashboard export generated at `/tmp/gc-analyzer-review-20260912/snapshot.html`; embedded assets/data and escaped investigation HTML are covered by automated tests. The in-app browser blocked opening the local HTML file by policy and did not expose a report-download event. Native download confirmation remains unverified in that browser.
- Test-only Playwright fixtures cover desktop/mobile canvas pixels and operational UI flows without live collection. Playwright is a developer test dependency, not an app runtime dependency.
- Final Python suite: **245 passed**, with one Starlette TestClient/httpx deprecation warning. JavaScript unit tests: **27 passed**. Shell syntax, Python compilation, and `git diff --check` passed.
- Final review added durable GC collection-quality markers committed atomically with offsets. Complete malformed/unsupported records cannot stall future polls; incomplete suffixes remain retryable. Unknown collection quality suppresses current health claims without erasing confirmed recent GC alerts.
- The final server was restarted and its controller status confirmed RUNNING on port 8085. Authenticated fleet, broker comparison, and aligned evidence views loaded after restart with no browser JavaScript errors. Changes remain local and have not been pushed.
- A GPT-5.6 reviewer completed the final focused recheck with no open findings; its GC integration suite passed 114 tests. This is not a claim that the deferred deployment, live collection, or model-validation work has been completed.

## Deferred Explicitly

Live Prometheus integration awaits the user's scrape/exporter configuration and validated instance-to-cluster mapping. Jobs follow `bvp-<region>-<env>` and contain multiple clusters. Do not infer a cluster from job membership. Keep internal endpoint details out of committed sample configs.

A durable collector process/queue migration, new seasonal model dependencies, automatic model promotion, and final RHEL bundle packaging are follow-on work requiring their own deployment/load validation. This change establishes correctness and visible guardrails without claiming these broader migrations are complete.
