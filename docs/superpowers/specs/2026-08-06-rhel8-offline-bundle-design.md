# RHEL 8 Offline Deployment Bundle Design

## Goal

Produce one offline deployment archive for RHEL 8.10 x86_64 containing the GC
Analyzer backend, the separate Next.js frontend, and every required runtime and
application dependency. The target server has no internet access but permits
installation of RPM files copied to it.

Packaging is a release step, not a test mechanism. The archive must not be
created unless the upgraded application and the complete offline installation
flow pass all required checks.

## Target Platform

- Operating system: RHEL 8.10
- Architecture: x86_64
- Python runtime: Python 3.12
- Node.js runtime: Node.js 22
- Backend port: 8083
- Frontend port: 3000
- Backend address used by the frontend proxy: `http://127.0.0.1:8083`

## Application Dependencies

The backend includes the complete transitive wheel set for:

- FastAPI 0.110 or newer
- Uvicorn 0.27 or newer
- Paramiko 3.4 or newer
- PyYAML 6.0 or newer
- scikit-learn and its dependencies for IsolationForest analysis

The frontend will be upgraded and locked to:

- Next.js 16.3.0
- React 19.2.8
- ReactDOM 19.2.8
- Chart.js 4.4.4
- react-chartjs-2 5.2.0
- TypeScript 5.6.2
- `@types/node` 20.16.5
- `@types/react` 18.3.5
- `@types/react-dom` 18.3.0

The lockfile is the source of truth for the exact npm dependency graph. The
Python wheel inventory and hashes are captured in the bundle manifest.

## Bundle Layout

The output is `gc-analyzer-rhel8.10-x86_64-offline.tar.gz` with this layout:

```text
gc-analyzer-offline/
  app/                  application source, frontend source, and lockfiles
  rpms/                 RHEL-compatible Python and runtime RPM closure
  python-wheels/        backend wheels and transitive dependencies
  npm-cache/            npm cache sufficient for npm ci --offline
  install-offline.sh    local-only installation entry point
  verify-offline.sh     post-install backend/frontend verification
  MANIFEST.sha256       checksum for every shipped artifact
  VERSIONS.txt          OS, architecture, runtime, and dependency versions
```

Development state is excluded: `.venv`, `node_modules`, `.next`, databases,
SQLite sidecar files, `.env`, session secrets, users, logs, PID files, test
caches, and local cluster credentials.

## Build Flow

The bundle builder runs in an x86_64 RHEL 8-compatible environment. It obtains
the RPM dependency closure, downloads Linux x86_64 Python wheels, populates the
npm cache from the committed lockfile, copies an allowlisted application tree,
and generates the version and checksum manifests.

The builder fails on missing wheels, missing npm tarballs, architecture
mismatches, unresolved RPM dependencies, dirty generated output, or checksum
generation errors. Source compilation on the target server is not allowed.

## Test-Before-Packaging Gate

The following checks run before the archive creation step:

1. Run the complete Python test suite with the selected Python dependency set.
2. Compile all Python modules and verify backend imports.
3. Run frontend type checking and the production Next.js build on Node.js 22.
4. Run the frontend dependency audit and reject critical or high findings.
5. Start the backend and verify `/api/health` returns HTTP 200.
6. Start the frontend with `BACKEND_URL=http://127.0.0.1:8083` and verify its
   same-origin `/api/health` proxy returns HTTP 200.
7. Rehearse the installer in a clean RHEL 8-compatible x86_64 container with
   network access disabled.
8. Repeat backend and frontend smoke tests using only bundled dependencies.

Any failure prevents archive creation. The final tarball is created only after
all eight checks pass.

## Target Installation Behavior

The installer verifies RHEL major/minor version, x86_64 architecture, bundle
checksums, free disk space, and root privileges for RPM installation. It then
installs only local RPMs, creates the Python virtual environment from the local
wheelhouse, installs frontend dependencies from the local npm cache, runs the
production frontend build, and executes the post-install verifier.

No target installation command may contact Red Hat, PyPI, npm, GitHub, or any
other network package source. Application runtime traffic remains limited to
the configured user-facing ports and SSH collection from monitored nodes.

## Failure Handling

The installer stops immediately on checksum, RPM, wheel, npm, build, or health
check failure. It reports the failing stage and exits nonzero. It does not
delete an existing database, configuration, user file, session secret, or
cluster definition. Re-running the installer with the same bundle is expected
to be idempotent.

## Acceptance Criteria

- One checksum-verified archive installs on clean RHEL 8.10 x86_64 without
  internet access.
- Python, Node.js, backend libraries, frontend packages, and ML dependencies
  resolve entirely from the archive.
- The backend and frontend run simultaneously on the same host.
- The frontend proxies API requests to the local backend successfully.
- No critical or high frontend dependency vulnerability is accepted.
- Packaging is impossible until the complete pre-package test gate succeeds.
