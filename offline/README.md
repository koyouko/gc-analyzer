# RHEL 8 Offline Deployment

Target: **RHEL 8.10, x86_64**, with a normal Minimal Install OS and root access.
No internet, Docker, Git, Node.js, application Python, pip, npm, compiler, or
manual application dependency installation is needed on the target.
The installer installs the bundled local RPMs, as permitted by your administrator.
This is not a replacement for installing RHEL itself: Bash, coreutils, DNF and
its system `/usr/libexec/platform-python` interpreter must be intact.

## Install

Transfer the complete repository, including `offline/releases/rhel8.10-x86_64/`,
to the server. From the repository root run:

```bash
sudo bash offline/deploy.sh
```

Do not run `pip install`, `npm install`, or `manage-app.sh deploy` on the
air-gapped server. The offline entrypoint verifies the release, installs local
RPMs with all package repositories disabled, installs Python wheels without an
index, and builds the frontend using the bundled npm cache in offline mode.

The dependency archive is stored as ordinary Git files split into 40 MiB parts,
so GitHub downloads and normal clones contain the actual payloads. Git LFS is
not required. Keep every part and both checksum manifests together.

Allow **10 GiB free disk space** for extraction, build, installed files and
upgrade backup, plus separate capacity for retained logs and history. A machine
with at least **4 GiB RAM** is recommended for the frontend production build.

## Included Software

| Component | Bundled version |
| --- | --- |
| Application Python | RHEL Python 3.12 RPMs, including pip and runtime libraries |
| FastAPI | 0.141.1 |
| Uvicorn | 0.52.1 |
| Paramiko | 5.0.0 |
| PyYAML | 6.0.3 |
| scikit-learn | 1.9.0, with NumPy, SciPy and all transitive wheels |
| Node.js / npm | 22.22.3 / bundled npm 10 |
| Next.js | 16.3.5 |
| Offline frontend compiler | Next SWC WebAssembly 16.3.5, using webpack on RHEL 8's existing glibc 2.28 |
| React / React DOM | 19.2.8 |
| Chart.js | 4.5.1 for the optional Next client; the dashboard's vendor asset is also local |
| OS packages | Python, certificates, networking/archive tools, service-account tools, systemd, DNF, C++ runtime and their RPM dependency closure |

`VERSIONS.txt` inside the verified archive lists every RPM and Python wheel with
its exact filename and SHA-256, plus the source commit and immutable build images.
`app/web/package-lock.json` records all frontend dependencies. `MANIFEST.sha256`
covers the entire payload. Third-party licenses shipped with packages remain
inside the RPMs, wheels, Node distribution and npm packages.

The offline installer preloads the portable compiler from the npm cache. Native
SWC compatibility warnings during the build are expected on RHEL 8; the bundled
WebAssembly fallback completes the build without downloads or replacing glibc.

## Access And Control

The public dashboard is `http://SERVER:3000`. The backend listens only on
`127.0.0.1:8083`. The frontend forwards the current dashboard, assets and API to
that backend; it includes the Prometheus Settings screen.

```bash
sudo gc-analyzerctl status
sudo gc-analyzerctl restart
sudo gc-analyzerctl stop
sudo gc-analyzerctl start
sudo gc-analyzerctl logs -n 100
sudo gc-analyzerctl verify
```

Fresh installations generate unique passwords. Retrieve the root-only initial
credentials in `/etc/gc-analyzer/bootstrap-credentials.json`, put them in your
approved password manager, then delete that initial-credentials file. Existing
users and passwords are preserved on an upgrade. No production credentials or
internal Prometheus addresses are included in the public repository.

| Location | Purpose |
| --- | --- |
| `/opt/gc-analyzer` | Application and private Python/Node runtimes |
| `/var/lib/gc-analyzer` | Persistent history and writable application state |
| `/var/lib/gc-analyzer/prometheus.json` | Prometheus settings, editable through Settings > Prometheus |
| `/etc/gc-analyzer/clusters` | GC/SAR collection and host mapping |
| `/etc/gc-analyzer/users.json` | Password hashes |
| `/etc/gc-analyzer/gc-analyzer.env` | Administrator-managed backend environment overrides |

Prometheus is read-only from this application. Configure the HTTP endpoint and
labels in Settings after installation. Cluster membership and physical region
are distinct, so intentional cross-region ZooKeeper members are not reassigned.
The Excel mapping can be added later. SSH permissions, source-host `sysstat`
collection, DNS, firewall access and live metric availability remain environment
configuration, not packaged application dependencies.

## Release Qualification

The builder tests source on Linux x86_64, validates TypeScript and production
builds, audits frontend production dependencies, resolves RPMs against an empty
RPM database, and rehearses two installations with Docker networking disabled.
Backend, current dashboard assets and same-origin API responses must pass before
an archive is emitted. See the release verification report for actual results.

Container qualification uses Red Hat UBI 8.10 under Linux x86_64 emulation when
built on an ARM Mac. It does not substitute for native RHEL acceptance testing
of systemd boot, SELinux policy, firewall rules, corporate certificates or live
SSH/Prometheus access. No claim of native server testing is implied.

## Rebuild On A Connected Build Host

Use a clean committed checkout with Bash 4+, Python 3, Git, curl, tar and a
current Docker supporting `docker image inspect --platform`:

```bash
bash offline/build-bundle.sh
```

Only committed allowlisted application files are packaged. Outputs go to the
ignored `offline/work` and `offline/dist` directories. Dependency payloads must
be regenerated and the offline gates repeated whenever application dependencies
change; editing a requirements file alone does not refresh the shipped release.
