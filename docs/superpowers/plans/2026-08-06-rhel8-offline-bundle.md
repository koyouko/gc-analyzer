# RHEL 8 Offline Deployment Bundle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and verify one no-internet RHEL 8.10 x86_64 archive containing the GC Analyzer FastAPI backend, Next.js frontend, Python wheels, npm cache, and local RPM dependency closure.

**Architecture:** A release builder creates a staging directory from an allowlisted source tree, downloads only Linux x86_64 dependencies, and generates checksums. A separate clean-room harness installs that staging directory in a network-disabled UBI 8.10 container; only a successful backend/frontend smoke test allows the tarball creation function to run.

**Tech Stack:** Bash 4+, Python 3.12, FastAPI, pytest, Node.js 22, npm 10+, Next.js 16.3.0, React 19.2.8, Docker/Podman, UBI 8.10, local DNF/RPM installation.

---

## File Structure

- Modify `web/package.json`: patched frontend versions, Node engine, test/build scripts.
- Modify `web/package-lock.json`: exact audited npm graph.
- Modify `web/next.config.js`: Next.js 16-compatible proxy configuration.
- Modify `web/app/cluster/[cluster]/page.tsx`: asynchronous route parameters.
- Modify `web/app/host/[id]/page.tsx`: asynchronous route parameters.
- Modify `web/app/instance/[id]/page.tsx`: asynchronous route parameters.
- Create `requirements-offline.txt`: exact offline backend and ML top-level versions.
- Create `offline/rhel8-packages.txt`: RPM roots whose full closure is downloaded.
- Create `offline/app-files.txt`: explicit source allowlist for the release archive.
- Create `offline/build-bundle.sh`: connected build-host staging and gate orchestration.
- Create `offline/install-offline.sh`: target-side local-only installation.
- Create `offline/verify-offline.sh`: backend/frontend import and HTTP verification.
- Create `offline/test-clean-room.sh`: network-disabled UBI 8.10 rehearsal.
- Create `offline/README.md`: short build, transfer, install, and verification reference.
- Create `tests/test_offline_bundle.py`: packaging policy and failure-gate tests.
- Modify `.gitignore`: generated offline staging and archives.
- Modify `README.md`: link to offline deployment instructions.

### Task 1: Upgrade and Audit the Next.js Frontend

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Modify: `web/next.config.js`
- Modify: `web/app/cluster/[cluster]/page.tsx`
- Modify: `web/app/host/[id]/page.tsx`
- Modify: `web/app/instance/[id]/page.tsx`

- [ ] **Step 1: Record the current failing baseline**

Run:

```bash
cd web
npm ci
npm audit --omit=dev --audit-level=high
npm run build
```

Expected: audit fails for Next.js 14.2.15 and the current production build is not accepted.

- [ ] **Step 2: Upgrade exact frontend dependencies**

Set `web/package.json` to:

```json
{
  "name": "bsp-kafka-gc-analyzer-web",
  "version": "1.0.0",
  "private": true,
  "engines": { "node": ">=22 <23", "npm": ">=10" },
  "scripts": {
    "dev": "next dev",
    "typecheck": "tsc --noEmit",
    "build": "next build",
    "start": "next start",
    "audit:prod": "npm audit --omit=dev --audit-level=high"
  },
  "dependencies": {
    "next": "16.3.0",
    "react": "19.2.8",
    "react-dom": "19.2.8",
    "chart.js": "4.5.1",
    "react-chartjs-2": "5.3.1"
  },
  "devDependencies": {
    "typescript": "5.6.2",
    "@types/node": "20.16.5",
    "@types/react": "19.2.18",
    "@types/react-dom": "19.2.4"
  }
}
```

Run `npm install --package-lock-only` to regenerate the lockfile.

- [ ] **Step 3: Apply Next.js 16 route and configuration changes**

Remove the obsolete `eslint` key from `next.config.js`. Convert each dynamic page to await its route params, for example:

```tsx
export default async function Page({ params }: {
  params: Promise<{ cluster: string }>;
}) {
  const { cluster } = await params;
  return <ClusterView cluster={decodeURIComponent(cluster)} />;
}
```

Apply the same shape for `host/[id]` and `instance/[id]`.

- [ ] **Step 4: Verify the upgraded frontend**

Run:

```bash
cd web
npm ci
npm run typecheck
npm run audit:prod
npm run build
```

Expected: all commands exit zero; audit reports no high or critical vulnerabilities; `.next` is created.

- [ ] **Step 5: Commit the frontend upgrade**

```bash
git add web/package.json web/package-lock.json web/next.config.js web/app
git commit -m "fix: upgrade frontend dependencies"
```

### Task 2: Define the Offline Python Dependency Set

**Files:**
- Create: `requirements-offline.txt`
- Modify: `tests/test_offline_bundle.py`

- [ ] **Step 1: Write a failing dependency-policy test**

Create `tests/test_offline_bundle.py` with:

```python
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_offline_requirements_are_exact_and_include_ml():
    lines = [line.strip() for line in (ROOT / "requirements-offline.txt").read_text().splitlines()
             if line.strip() and not line.startswith("#")]
    assert "fastapi==0.141.1" in lines
    assert "uvicorn==0.52.1" in lines
    assert "paramiko==5.0.0" in lines
    assert "PyYAML==6.0.3" in lines
    assert "scikit-learn==1.9.0" in lines
    assert all("==" in line for line in lines)
```

- [ ] **Step 2: Run the test and verify RED**

Run `python -m pytest tests/test_offline_bundle.py -q`.

Expected: FAIL because `requirements-offline.txt` does not exist.

- [ ] **Step 3: Add exact offline requirements**

Create `requirements-offline.txt`:

```text
fastapi==0.141.1
uvicorn==0.52.1
paramiko==5.0.0
PyYAML==6.0.3
scikit-learn==1.9.0
```

- [ ] **Step 4: Verify Python dependency policy and application tests**

Run:

```bash
python -m pytest tests/test_offline_bundle.py -q
python -m pytest -q
python -m compileall -q gcanalyzer seed tests
```

Expected: dependency test passes; complete suite reports 75 or more passing tests; compileall exits zero.

- [ ] **Step 5: Commit the Python dependency set**

```bash
git add requirements-offline.txt tests/test_offline_bundle.py
git commit -m "build: lock offline Python dependencies"
```

### Task 3: Add Bundle Inputs and Allowlist

**Files:**
- Create: `offline/rhel8-packages.txt`
- Create: `offline/app-files.txt`
- Modify: `tests/test_offline_bundle.py`
- Modify: `.gitignore`

- [ ] **Step 1: Add failing allowlist and RPM tests**

Append:

```python
def test_release_allowlist_excludes_runtime_secrets():
    allowlist = (ROOT / "offline/app-files.txt").read_text()
    forbidden = (".env", ".venv", "users.json", ".session_secret", "node_modules", ".next", ".db")
    assert not any(item in allowlist for item in forbidden)


def test_rhel_roots_include_python_and_operational_tools():
    packages = set((ROOT / "offline/rhel8-packages.txt").read_text().split())
    assert {"python3.12", "python3.12-pip", "ca-certificates", "curl", "tar", "gzip"} <= packages
```

- [ ] **Step 2: Run tests and verify RED**

Expected: both fail because input manifests do not exist.

- [ ] **Step 3: Add manifests**

Create `offline/rhel8-packages.txt`:

```text
python3.12
python3.12-pip
ca-certificates
curl
tar
gzip
shadow-utils
```

Create `offline/app-files.txt`:

```text
frontend
gcanalyzer
seed
web/app
web/components
web/lib
web/next.config.js
web/package.json
web/package-lock.json
web/tsconfig.json
requirements.txt
requirements-offline.txt
manage-app.sh
README.md
architecture_and_user_guide.html
```

Add to `.gitignore`:

```text
offline/dist/
offline/work/
```

- [ ] **Step 4: Run tests and verify GREEN**

Run `python -m pytest tests/test_offline_bundle.py -q`.

- [ ] **Step 5: Commit manifests**

```bash
git add .gitignore offline/rhel8-packages.txt offline/app-files.txt tests/test_offline_bundle.py
git commit -m "build: define offline bundle inputs"
```

### Task 4: Implement the Connected Bundle Builder

**Files:**
- Create: `offline/build-bundle.sh`
- Modify: `tests/test_offline_bundle.py`

- [ ] **Step 1: Add failing builder-policy tests**

Append tests asserting that `offline/build-bundle.sh`:

```python
def test_builder_targets_rhel8_x86_64_and_gates_archive():
    script = (ROOT / "offline/build-bundle.sh").read_text()
    assert "linux/amd64" in script
    assert "ubi8/ubi:8.10" in script
    assert "requirements-offline.txt" in script
    assert "npm ci" in script
    assert "npm audit --omit=dev --audit-level=high" in script
    assert "test-clean-room.sh" in script
    assert script.index("test-clean-room.sh") < script.index("tar -czf")
```

- [ ] **Step 2: Run the test and verify RED**

Expected: FAIL because the builder does not exist.

- [ ] **Step 3: Implement builder stages**

Implement Bash functions with `set -euo pipefail`:

```bash
preflight
test_source
prepare_stage
download_rpms
download_node_runtime
download_python_wheels
populate_npm_cache
copy_application
write_version_manifest
write_checksums
run_clean_room
create_archive
```

Use `docker run --platform linux/amd64` with `registry.access.redhat.com/ubi8/ubi:8.10` for RPM and Python artifacts. Download Python wheels with:

```bash
python3.12 -m pip download --only-binary=:all: \
  --dest /bundle/python-wheels \
  -r /src/requirements-offline.txt
```

Download `node-v22.22.3-linux-x64.tar.xz`, verify it against the matching upstream `SHASUMS256.txt`, and extract it into `node-runtime/`. Populate npm cache in a Node 22 Linux x86_64 container using the committed lockfile and `npm ci --cache /bundle/npm-cache`. Copy only paths from `offline/app-files.txt`. Run the audit before entering the network-disabled clean-room test. Invoke `create_archive` only after `run_clean_room` succeeds.

- [ ] **Step 4: Verify script syntax and policy tests**

Run:

```bash
bash -n offline/build-bundle.sh
python -m pytest tests/test_offline_bundle.py -q
```

Expected: both exit zero.

- [ ] **Step 5: Commit builder**

```bash
git add offline/build-bundle.sh tests/test_offline_bundle.py
git commit -m "build: add connected offline bundle builder"
```

### Task 5: Implement Local-Only Installer and Verifier

**Files:**
- Create: `offline/install-offline.sh`
- Create: `offline/verify-offline.sh`
- Modify: `tests/test_offline_bundle.py`

- [ ] **Step 1: Add failing installer policy tests**

Append:

```python
def test_installer_is_local_only_and_preserves_state():
    script = (ROOT / "offline/install-offline.sh").read_text()
    assert "--disablerepo=*" in script
    assert "--no-index" in script
    assert "--find-links" in script
    assert "npm ci --offline" in script
    assert "MANIFEST.sha256" in script
    assert "uname -m" in script
    assert "VERSION_ID" in script
    assert "rm -rf /var/lib/gc-analyzer" not in script
```

- [ ] **Step 2: Run tests and verify RED**

Expected: FAIL because the installer does not exist.

- [ ] **Step 3: Implement installer**

The installer must:

1. Require root for RPM installation.
2. Verify `VERSION_ID=8.10` and `uname -m=x86_64`.
3. Validate `sha256sum -c MANIFEST.sha256` before installation.
4. Install local RPMs with DNF repositories disabled.
5. Install the bundled Node.js 22.22.3 Linux x64 runtime under `/opt/gc-analyzer/runtime/node`.
6. Copy the allowlisted application to `/opt/gc-analyzer` without replacing `/var/lib/gc-analyzer` or `/etc/gc-analyzer`.
7. Create `.venv` with Python 3.12 and use `pip --no-index --find-links`.
8. Run `npm ci --offline --cache` and `npm run build` as the service user.
9. Invoke `verify-offline.sh` and exit nonzero on failure.

- [ ] **Step 4: Implement verifier**

Verify versions/imports with:

```bash
/opt/gc-analyzer/.venv/bin/python -c \
  'import fastapi, uvicorn, paramiko, yaml, sklearn, sqlite3'
/opt/gc-analyzer/runtime/node/bin/node --version
test -f /opt/gc-analyzer/web/.next/BUILD_ID
```

Add condition-based backend/frontend startup, HTTP checks for backend `/api/health`, frontend `/`, and frontend-proxied `/api/health`, followed by graceful process cleanup.

- [ ] **Step 5: Run tests and shell syntax checks**

```bash
bash -n offline/install-offline.sh offline/verify-offline.sh
python -m pytest tests/test_offline_bundle.py -q
```

- [ ] **Step 6: Commit installer and verifier**

```bash
git add offline/install-offline.sh offline/verify-offline.sh tests/test_offline_bundle.py
git commit -m "build: add offline installer and verifier"
```

### Task 6: Add the Network-Disabled Clean-Room Gate

**Files:**
- Create: `offline/test-clean-room.sh`
- Modify: `tests/test_offline_bundle.py`

- [ ] **Step 1: Add a failing gate-order test**

Append assertions that the clean-room command contains `--network none`, mounts the staged bundle read-only, copies it inside the container, and calls `install-offline.sh` before `verify-offline.sh`.

- [ ] **Step 2: Run the test and verify RED**

Expected: FAIL because the clean-room harness does not exist.

- [ ] **Step 3: Implement clean-room test**

Use:

```bash
docker run --rm --platform linux/amd64 --network none \
  -v "$STAGE_DIR:/incoming:ro" \
  registry.access.redhat.com/ubi8/ubi:8.10 \
  /bin/bash -lc 'cp -a /incoming /bundle && /bundle/install-offline.sh --container-test'
```

The container-test path skips systemd registration but runs local RPM installation, Python wheel installation, offline npm installation/build, backend startup, frontend startup, and proxy checks.

- [ ] **Step 4: Run static tests and shell syntax**

```bash
bash -n offline/test-clean-room.sh
python -m pytest tests/test_offline_bundle.py -q
```

- [ ] **Step 5: Commit clean-room gate**

```bash
git add offline/test-clean-room.sh tests/test_offline_bundle.py
git commit -m "test: gate bundle on offline RHEL clean room"
```

### Task 7: Document the Offline Bundle

**Files:**
- Create: `offline/README.md`
- Modify: `README.md`
- Modify: `tests/test_control_script_static.py`

- [ ] **Step 1: Add a failing documentation test**

Assert that both documents name RHEL 8.10 x86_64, Python 3.12, Node.js 22, `build-bundle.sh`, `install-offline.sh`, and the no-internet clean-room gate.

- [ ] **Step 2: Run the test and verify RED**

Run `python -m pytest tests/test_control_script_static.py -q`.

- [ ] **Step 3: Write concise operator documentation**

Document connected build-host prerequisites, output archive name, checksum verification, transfer, local install, ports 3000/8083, and how to read installer/verification failures. State that the archive contains no credentials or databases.

- [ ] **Step 4: Verify documentation tests**

Run `python -m pytest tests/test_control_script_static.py -q`.

- [ ] **Step 5: Commit documentation**

```bash
git add README.md offline/README.md tests/test_control_script_static.py
git commit -m "docs: add offline RHEL deployment guide"
```

### Task 8: Run the Full Gate and Create the Archive

**Files:**
- Generated: `offline/dist/gc-analyzer-rhel8.10-x86_64-offline.tar.gz`
- Generated: `offline/dist/gc-analyzer-rhel8.10-x86_64-offline.tar.gz.sha256`

- [ ] **Step 1: Run all source tests before bundle staging**

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m compileall -q gcanalyzer seed tests
bash -n manage-app.sh run.sh start-local.command offline/*.sh
cd web && npm ci && npm run typecheck && npm run audit:prod && npm run build
```

Expected: all commands exit zero; no high or critical npm audit findings.

- [ ] **Step 2: Build staging content and execute clean-room rehearsal**

Run `./offline/build-bundle.sh`.

Expected sequence: RPM resolution, Linux wheel resolution, npm cache creation, checksums, network-disabled RHEL 8.10 install, backend health, frontend health, proxy health, then archive creation.

- [ ] **Step 3: Verify final archive and contents**

```bash
cd offline/dist
shasum -a 256 -c gc-analyzer-rhel8.10-x86_64-offline.tar.gz.sha256
tar -tzf gc-analyzer-rhel8.10-x86_64-offline.tar.gz
```

Expected: checksum succeeds; archive contains `app/`, `rpms/`, `node-runtime/`, `python-wheels/`, `npm-cache/`, installer, verifier, manifests, and no forbidden runtime/development files.

- [ ] **Step 4: Re-run the complete repository suite after packaging**

Run `.venv/bin/python -m pytest -q` and `git diff --check`.

- [ ] **Step 5: Commit implementation sources, not generated archive**

```bash
git add .gitignore README.md requirements-offline.txt web offline tests
git commit -m "feat: add tested RHEL 8 offline deployment bundle"
```

The generated archive remains ignored because it is a large release artifact; report its absolute path, byte size, and SHA-256 to the user.
