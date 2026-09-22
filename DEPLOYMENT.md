# GC Analyzer - Production Deployment Guide (Internet, Nexus & Artifactory)

This guide documents how to deploy **GC Analyzer** without bundled dependency binaries, downloading packages dynamically from the public internet or from enterprise artifact repositories such as **Sonatype Nexus** or **JFrog Artifactory**.

---

## 1. Prerequisites

### Operating System & Hardware
* **OS**: Linux (RHEL 8 / 9, Rocky Linux, AlmaLinux, CentOS Stream, Fedora, Ubuntu 22.04+, Debian 12+)
* **Hardware**: Minimum 2 CPUs, 4 GiB RAM, 10 GiB free disk space
* **System packages**: Bash 4+, Python 3.10+ (Python 3.12 recommended), Node.js 20+ or 22+, npm 10+, and standard system tools (`systemd`, `tar`, `gzip`, `findutils`).

---

## 2. Quick Start: Direct Internet Installation

If the deployment host has direct internet access to PyPI, npmjs.org, and OS distribution repositories:

```bash
# 1. Clone repository (branch: online-install)
git clone -b online-install https://github.com/koyouko/gc-analyzer.git
cd gc-analyzer

# 2. Run the production installer as root
sudo bash install.sh
```

The installer will:
1. Ensure system packages are installed via `dnf`/`yum`/`apt-get`.
2. Provision system service user/group (`gc-analyzer:gc-analyzer`).
3. Set up directory structure under `/opt/gc-analyzer`, `/var/lib/gc-analyzer`, and `/etc/gc-analyzer`.
4. Create Python virtual environment and install dependencies via `pip` from PyPI.
5. Install frontend packages via `npm ci` and compile production Next.js assets.
6. Generate session secrets and bootstrap credentials.
7. Configure and start systemd services (`gc-analyzer-backend` and `gc-analyzer-frontend`).

---

## 3. Deploying with Sonatype Nexus

In enterprise environments, internal build/deployment hosts often reach external package repositories only through a **Sonatype Nexus Repository Manager**.

### Nexus Repository Setup
Ensure your Nexus administrator has configured:
1. **PyPI (Proxy or Group)**:
   - Name example: `pypi-group` or `pypi-proxy`
   - Remote URL: `https://pypi.org/`
   - Index URL format: `https://<nexus-host>/repository/pypi-group/simple`
2. **npm (Proxy or Group)**:
   - Name example: `npm-group` or `npm-proxy`
   - Remote URL: `https://registry.npmjs.org/`
   - Registry URL format: `https://<nexus-host>/repository/npm-group/`
3. **YUM / RPM (Optional Proxy)**:
   - Name example: `rhel-appstream-proxy` or `rocky-base-proxy`

### Running the Installer with Nexus

Run `install.sh` passing your Nexus endpoints:

```bash
sudo bash install.sh \
  --pip-index-url "https://nexus.corp.local/repository/pypi-group/simple" \
  --trusted-host "nexus.corp.local" \
  --npm-registry "https://nexus.corp.local/repository/npm-group/"
```

### Alternative: Using Environment Variables

You can also export environment variables before running the installation:

```bash
export PIP_INDEX_URL="https://nexus.corp.local/repository/pypi-group/simple"
export PIP_TRUSTED_HOST="nexus.corp.local"
export NPM_CONFIG_REGISTRY="https://nexus.corp.local/repository/npm-group/"

sudo -E bash install.sh
```

---

## 4. Deploying with JFrog Artifactory

If your organization uses **JFrog Artifactory** as its universal artifact management platform:

### Artifactory Repository Setup
1. **PyPI Virtual / Remote Repository**:
   - Name example: `pypi-virtual`
   - Index URL format: `https://artifactory.corp.local/artifactory/api/pypi/pypi-virtual/simple`
2. **npm Virtual / Remote Repository**:
   - Name example: `npm-virtual`
   - Registry URL format: `https://artifactory.corp.local/artifactory/api/npm/npm-virtual/`
3. **RPM / YUM Repository**:
   - Configured in `/etc/yum.repos.d/artifactory.repo`

### Running the Installer with Artifactory

```bash
sudo bash install.sh \
  --pip-index-url "https://artifactory.corp.local/artifactory/api/pypi/pypi-virtual/simple" \
  --trusted-host "artifactory.corp.local" \
  --npm-registry "https://artifactory.corp.local/artifactory/api/npm/npm-virtual/"
```

---

## 5. Enterprise Certificates & Corporate Proxy Configuration

### Private / Corporate CA Certificates
If your Nexus/Artifactory instance uses a corporate internal root certificate:

1. **Add CA Certificate to System Trust Store**:
   ```bash
   # On RHEL / CentOS / Rocky:
   sudo cp corp-ca.crt /etc/pki/ca-trust/source/anchors/
   sudo update-ca-trust

   # On Ubuntu / Debian:
   sudo cp corp-ca.crt /usr/local/share/ca-certificates/
   sudo update-ca-certificates
   ```

2. **Configure Node / npm for Corporate CA**:
   ```bash
   export NODE_EXTRA_CA_CERTS="/etc/pki/ca-trust/source/anchors/corp-ca.crt"
   # Or for npm directly:
   npm config set cafile /etc/pki/ca-trust/source/anchors/corp-ca.crt
   ```

3. **Configure Pip for Corporate CA**:
   ```bash
   export PIP_CERT="/etc/pki/ca-trust/source/anchors/corp-ca.crt"
   # Or trust host directly:
   export PIP_TRUSTED_HOST="nexus.corp.local"
   ```

### HTTP/HTTPS Proxy
If accessing external repositories through an outbound forward proxy:
```bash
export HTTP_PROXY="http://proxy.corp.local:8080"
export HTTPS_PROXY="http://proxy.corp.local:8080"
export NO_PROXY="localhost,127.0.0.1,.corp.local"
sudo -E bash install.sh
```

---

## 6. System Layout & Service Management

### Filesystem Layout

| Path | Description |
| --- | --- |
| `/opt/gc-analyzer` | Application source, compiled Next.js build, and Python virtualenv (`.venv`) |
| `/var/lib/gc-analyzer` | Persistent database (`gc_history.db`), Prometheus settings (`prometheus.json`) |
| `/etc/gc-analyzer` | User accounts (`users.json`), cluster YAML definitions (`clusters/`), environment config |
| `/etc/gc-analyzer/bootstrap-credentials.json` | Generated initial passwords (root-readable only) |

### Service Control Commands

Manage the deployed system services with `gc-analyzerctl`:

```bash
# Check service health and status
sudo gc-analyzerctl status

# View live consolidated service logs
sudo gc-analyzerctl logs

# Restart services after configuration changes
sudo gc-analyzerctl restart

# Stop or Start services
sudo gc-analyzerctl stop
sudo gc-analyzerctl start
```

Or manage via standard `systemctl`:
```bash
sudo systemctl status gc-analyzer-frontend gc-analyzer-backend
```

---

## 7. Local / Development Setup (manage-app.sh)

For local developers running GC Analyzer without systemd:

```bash
# Configure repository URLs if using Nexus/Artifactory
export PIP_INDEX_URL="https://nexus.corp.local/repository/pypi-group/simple"
export NPM_CONFIG_REGISTRY="https://nexus.corp.local/repository/npm-group/"

# Initialize virtualenv and dependencies
./manage-app.sh setup

# Start the application locally on default port (8083)
./manage-app.sh start 8083

# Check status
./manage-app.sh status 8083
```
