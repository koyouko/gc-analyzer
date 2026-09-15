# Offline Release Verification

Qualification date: **2026-09-14**.

Target: **RHEL 8.10 x86_64**, normal Minimal Install OS, root access, local RPM
installation permitted. No internet or separate application dependency setup is
required. The existing OS must have working Bash, coreutils, DNF and its system
Python interpreter. Docker is used only on the build/test machine.

Packaged application source: `c19dfc5d2d4375ca662d663338e36bcadde7c5eb`.
The release-payload commit adds the binary parts and this report to that source.

## Payload

- 172 signed Red Hat RPMs, x86_64/noarch only, including Python 3.12.14 and pip.
- 28 Python wheels, including FastAPI, Uvicorn, Paramiko, PyYAML, scikit-learn,
  NumPy, SciPy and all transitive dependencies.
- Node.js 22.22.3 and npm 10.9.8.
- Complete locked frontend npm cache: Next.js 16.3.5, React 19.2.8, Chart.js 4.5.1
  and their dependencies, including development/build packages.
- Next SWC WebAssembly 16.3.5 for offline webpack builds on stock glibc 2.28.
- Current dashboard assets, application source, documentation, installer,
  runtime verifier, checksums and path/permission/symlink inventory.

The archive's `VERSIONS.txt` records exact RPM and wheel filenames and SHA-256
hashes. `app/web/package-lock.json` records the complete frontend dependency set
and integrity values. The release is ordinary Git content, not Git LFS pointers.

Archive: `gc-analyzer-rhel8.10-x86_64-offline.tar.gz`, approximately 372 MiB,
stored in ten parts of at most 40 MiB under `releases/rhel8.10-x86_64/`.

SHA-256: `f8e0f1218e7d5d44801d4e9dd2988b313bca5c79213f49785ce442ee8807e7ea`.

## Results

| Gate | Result |
| --- | --- |
| Python suite on Linux x86_64 / Python 3.12 | 581 passed; 1 skipped because the non-root rejection test runs in a root container |
| JavaScript suite on Linux x86_64 / Node 22.22.3 | 62 passed, 0 failed |
| TypeScript | Passed |
| Frontend production build | Passed |
| Frontend production dependency audit | 0 vulnerabilities reported at qualification time |
| RPM closure installed into an empty RPM database, network disabled | Passed |
| Full fresh and repeat offline installation | Both passed with networking disabled; Python/Node/npm, backend, dashboard, API proxy and systemd unit-file validation passed; no application processes leaked |
| Final split archive extraction and integrity | Passed with RHEL 8's Python 3.6: 10 parts, 6,397 archive entries, complete gzip/checksum/path/permission/symlink verification |

An additional RHEL 8.10 x86_64 compatibility probe built the frontend with the
bundled WASM compiler as a non-root account, with networking disabled and glibc
2.28 unchanged. Next started, the dashboard and six referenced assets returned
HTTP 200, and its proxied `/api/health` returned HTTP 200. Sharp 0.35.4 also loaded
and rendered a PNG without additional native libraries.

The earlier native SWC failure was reproduced and fixed without replacing RHEL's
glibc or downgrading Next. Native SWC warnings on RHEL 8 are expected before the
preloaded portable compiler is selected.

## Qualification Boundary

The immutable Red Hat UBI 8.10 image was
`sha256:ee704c97fed5798553a164a573cb41f9ddbafe9c54370222a205662e4d236557`.
Linux x86_64 containers ran under emulation on an ARM Mac. All actual local RPM,
wheel, npm-cache and runtime tests use that RHEL-compatible userspace; the source
frontend gate additionally uses the pinned Node 22.22.3 Debian image recorded
in `VERSIONS.txt`.

This is not a native RHEL server acceptance test. Systemd unit files are checked
with `systemd-analyze verify`, but native boot, SELinux, firewall policy, corporate
certificates, live SSH/SAR collection and access to the private Prometheus server
still need validation in the deployment environment.

## Security And Configuration

The payload excludes real users, session secrets, private Prometheus endpoints,
history databases and local logs. A fresh installation generates unique initial
passwords in a root-only file. Prometheus is configured after installation through
Settings; its persistent file is `/var/lib/gc-analyzer/prometheus.json`.

The previous remote revision tracked `.session_secret` and `users.json`. This
release removes those files from tracking and ignores local copies; it does not
rewrite old Git history. Rotate any session keys and passwords used from that
earlier setup.

## Deploy

Transfer the complete repository, including all release parts, then run from its
root directory:

```bash
sudo bash offline/deploy.sh
```

The installer performs the local dependency installation itself. See
[the offline guide](README.md) for access, service control and configuration paths.
