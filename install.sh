#!/usr/bin/env bash
# ==============================================================================
# GC Analyzer - Production & Service Installer (Online / Enterprise Repository)
# ==============================================================================
# This script installs and deploys GC Analyzer on Linux systems.
# Dependencies are downloaded dynamically from:
#   - The public internet (default: PyPI, npmjs.org, distro package repositories)
#   - OR configured enterprise repositories (e.g. Sonatype Nexus, JFrog Artifactory)
#
# Usage:
#   sudo bash install.sh [options]
#
# Options:
#   --pip-index-url <URL>        Custom PyPI index URL (e.g. Nexus/Artifactory PyPI proxy)
#   --pip-extra-index-url <URL>  Secondary/extra PyPI index URL
#   --trusted-host <HOST>        Trust host for pip SSL verification (e.g. nexus.corp)
#   --npm-registry <URL>         Custom npm registry URL (e.g. Nexus/Artifactory npm proxy)
#   --prefix <PATH>              Installation root (default: /opt/gc-analyzer)
#   --state-dir <PATH>           Data/state directory (default: /var/lib/gc-analyzer)
#   --config-dir <PATH>          Configuration directory (default: /etc/gc-analyzer)
#   --skip-os-packages           Do not attempt to install OS packages via dnf/yum/apt
#   --skip-frontend              Skip Next.js frontend build (backend-only mode)
#   --no-start                   Do not start systemd services immediately after install
#   -h, --help                   Display this help message
#
# Environment Variables (can be used instead of CLI flags):
#   PIP_INDEX_URL                Primary pip package index
#   PIP_EXTRA_INDEX_URL          Secondary pip package index
#   PIP_TRUSTED_HOST             Trusted hostname for pip
#   NPM_CONFIG_REGISTRY          Primary npm registry URL (or NPM_REGISTRY)
#   NODE_EXTRA_CA_CERTS          Path to corporate CA bundle for Node/npm
# ==============================================================================

set -euo pipefail
umask 027

APP_ROOT="/opt/gc-analyzer"
STATE_ROOT="/var/lib/gc-analyzer"
CONFIG_ROOT="/etc/gc-analyzer"
SERVICE_USER="gc-analyzer"
SERVICE_GROUP="gc-analyzer"

PIP_INDEX_URL="${PIP_INDEX_URL:-}"
PIP_EXTRA_INDEX_URL="${PIP_EXTRA_INDEX_URL:-}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-}"
NPM_REGISTRY="${NPM_CONFIG_REGISTRY:-${NPM_REGISTRY:-}}"

SKIP_OS_PACKAGES=0
SKIP_FRONTEND=0
START_SERVICES=1
SKIP_SYSTEMD=0

log_info() {
    printf '\033[1;34m[INFO]\033[0m %s\n' "$1"
}

log_ok() {
    printf '\033[1;32m[OK]\033[0m %s\n' "$1"
}

log_warn() {
    printf '\033[1;33m[WARN]\033[0m %s\n' "$1" >&2
}

log_err() {
    printf '\033[1;31m[ERROR]\033[0m %s\n' "$1" >&2
    exit 1
}

show_help() {
    cat <<'EOF'
GC Analyzer - Production & Service Installer (Online / Enterprise Repository)

Usage:
  sudo bash install.sh [options]

Options:
  --pip-index-url <URL>        Custom PyPI index URL (e.g. Nexus/Artifactory PyPI proxy)
  --pip-extra-index-url <URL>  Secondary/extra PyPI index URL
  --trusted-host <HOST>        Trust host for pip SSL verification (e.g. nexus.corp)
  --npm-registry <URL>         Custom npm registry URL (e.g. Nexus/Artifactory npm proxy)
  --prefix <PATH>              Installation root (default: /opt/gc-analyzer)
  --state-dir <PATH>           Data/state directory (default: /var/lib/gc-analyzer)
  --config-dir <PATH>          Configuration directory (default: /etc/gc-analyzer)
  --skip-os-packages           Do not attempt to install OS packages via dnf/yum/apt
  --skip-frontend              Skip Next.js frontend build (backend-only mode)
  --systemd                    Install systemd units (default when running as root)
  --skip-systemd               Do not create or enable systemd service units
  --no-start                   Do not start systemd services immediately after install
  -h, --help                   Display this help message

Environment Variables:
  PIP_INDEX_URL                Primary pip package index
  PIP_EXTRA_INDEX_URL          Secondary pip package index
  PIP_TRUSTED_HOST             Trusted hostname for pip
  NPM_CONFIG_REGISTRY          Primary npm registry URL (or NPM_REGISTRY)
  NODE_EXTRA_CA_CERTS          Path to corporate CA bundle for Node/npm
EOF
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pip-index-url)
            PIP_INDEX_URL="$2"
            shift 2
            ;;
        --pip-extra-index-url)
            PIP_EXTRA_INDEX_URL="$2"
            shift 2
            ;;
        --trusted-host)
            PIP_TRUSTED_HOST="$2"
            shift 2
            ;;
        --npm-registry)
            NPM_REGISTRY="$2"
            shift 2
            ;;
        --prefix)
            APP_ROOT="$2"
            shift 2
            ;;
        --state-dir)
            STATE_ROOT="$2"
            shift 2
            ;;
        --config-dir)
            CONFIG_ROOT="$2"
            shift 2
            ;;
        --skip-os-packages)
            SKIP_OS_PACKAGES=1
            shift
            ;;
        --skip-frontend)
            SKIP_FRONTEND=1
            shift
            ;;
        --systemd)
            SKIP_SYSTEMD=0
            shift
            ;;
        --skip-systemd)
            SKIP_SYSTEMD=1
            shift
            ;;
        --no-start)
            START_SERVICES=0
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            log_err "Unknown argument: $1 (run with --help for options)"
            ;;
    esac
done

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
CLUSTERS_ROOT="$CONFIG_ROOT/clusters"
ENV_FILE="$CONFIG_ROOT/gc-analyzer.env"
GC_USERS_FILE="$CONFIG_ROOT/users.json"
SESSION_SECRET_FILE="$CONFIG_ROOT/.session_secret"
PYTHON_ROOT="$APP_ROOT/.venv"
SYSTEMD_UNIT_ROOT="/etc/systemd/system"

# 1. Privilege Check
if [[ "$EUID" -ne 0 ]]; then
    log_warn "Installer is not running as root. System accounts and systemd units will be skipped."
    IS_ROOT=0
else
    IS_ROOT=1
fi

log_info "============================================================"
log_info " Starting GC Analyzer Installation"
log_info " Source:       $SCRIPT_DIR"
log_info " Target Prefix: $APP_ROOT"
log_info " State Dir:    $STATE_ROOT"
log_info " Config Dir:   $CONFIG_ROOT"
if [[ -n "$PIP_INDEX_URL" ]]; then
    log_info " PyPI Index:   $PIP_INDEX_URL"
else
    log_info " PyPI Index:   Default (Public PyPI)"
fi
if [[ -n "$NPM_REGISTRY" ]]; then
    log_info " npm Registry: $NPM_REGISTRY"
else
    log_info " npm Registry: Default (https://registry.npmjs.org/)"
fi
log_info "============================================================"

# 2. Install OS packages if permitted and package manager exists
if [[ "$IS_ROOT" -eq 1 && "$SKIP_OS_PACKAGES" -eq 0 ]]; then
    if command -v dnf >/dev/null 2>&1; then
        log_info "Checking system packages with DNF..."
        dnf install -y python312 python3.12-pip nodejs npm tar gzip findutils || \
        dnf install -y python3 python3-pip nodejs npm tar gzip findutils || \
        log_warn "Package installation via DNF completed with warnings; continuing with existing system tools."
    elif command -v yum >/dev/null 2>&1; then
        log_info "Checking system packages with YUM..."
        yum install -y python3 python3-pip nodejs npm tar gzip findutils || \
        log_warn "Package installation via YUM completed with warnings; continuing."
    elif command -v apt-get >/dev/null 2>&1; then
        log_info "Checking system packages with APT..."
        apt-get update -y && apt-get install -y python3 python3-venv python3-pip nodejs npm tar gzip || \
        log_warn "Package installation via APT completed with warnings; continuing."
    fi
fi

# 3. Locate Python 3.10+
resolve_python() {
    local candidate ver
    for candidate in python3.12 python3.11 python3.10 python3; do
        if command -v "$candidate" >/dev/null 2>&1; then
            ver=$("$candidate" -c 'import sys; print(sys.version_info[0]*100 + sys.version_info[1])' 2>/dev/null || echo 0)
            if (( ver >= 310 )); then
                echo "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

SYS_PYTHON=$(resolve_python || true)
[[ -n "$SYS_PYTHON" ]] || log_err "Python 3.10 or newer is required but not found in PATH."
log_ok "Using Python: $("$SYS_PYTHON" --version 2>&1) ($SYS_PYTHON)"

# 4. Service Account setup
if [[ "$IS_ROOT" -eq 1 ]]; then
    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        log_info "Creating service group $SERVICE_GROUP..."
        groupadd --system "$SERVICE_GROUP"
    fi
    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        log_info "Creating service user $SERVICE_USER..."
        useradd --system --gid "$SERVICE_GROUP" --home-dir "$STATE_ROOT" \
            --create-home --shell /sbin/nologin "$SERVICE_USER"
    fi
fi

# 5. Directory layout and copy application source
log_info "Preparing application directories..."
mkdir -p "$APP_ROOT" "$STATE_ROOT" "$CONFIG_ROOT" "$CLUSTERS_ROOT"

log_info "Deploying application code to $APP_ROOT..."
cp -a "$SCRIPT_DIR/gcanalyzer" "$APP_ROOT/"
cp -a "$SCRIPT_DIR/seed" "$APP_ROOT/"
cp -a "$SCRIPT_DIR/web" "$APP_ROOT/"
cp -a "$SCRIPT_DIR/requirements.txt" "$APP_ROOT/"
cp -a "$SCRIPT_DIR/manage-app.sh" "$APP_ROOT/"
cp -a "$SCRIPT_DIR/README.md" "$APP_ROOT/"
[[ -f "$SCRIPT_DIR/architecture_and_user_guide.html" ]] && cp -a "$SCRIPT_DIR/architecture_and_user_guide.html" "$APP_ROOT/"
[[ -f "$SCRIPT_DIR/clusters" ]] && cp -a "$SCRIPT_DIR/clusters/." "$CLUSTERS_ROOT/"

# Ensure configuration files
[[ -f "$CONFIG_ROOT/config.json" ]] || printf '{\n  "concurrency": 20\n}\n' > "$CONFIG_ROOT/config.json"
[[ -f "$ENV_FILE" ]] || touch "$ENV_FILE"

# 6. Build Python Virtual Environment from Internet or Configured PyPI Repository
log_info "Setting up Python virtual environment in $PYTHON_ROOT..."
"$SYS_PYTHON" -m venv "$PYTHON_ROOT"

pip_install() {
    local cmd=("$PYTHON_ROOT/bin/python" -m pip install --upgrade pip)
    if [[ -n "$PIP_INDEX_URL" ]]; then
        cmd+=(--index-url "$PIP_INDEX_URL")
    fi
    if [[ -n "$PIP_EXTRA_INDEX_URL" ]]; then
        cmd+=(--extra-index-url "$PIP_EXTRA_INDEX_URL")
    fi
    if [[ -n "$PIP_TRUSTED_HOST" ]]; then
        cmd+=(--trusted-host "$PIP_TRUSTED_HOST")
    fi
    "${cmd[@]}"

    local req_cmd=("$PYTHON_ROOT/bin/python" -m pip install)
    if [[ -n "$PIP_INDEX_URL" ]]; then
        req_cmd+=(--index-url "$PIP_INDEX_URL")
    fi
    if [[ -n "$PIP_EXTRA_INDEX_URL" ]]; then
        req_cmd+=(--extra-index-url "$PIP_EXTRA_INDEX_URL")
    fi
    if [[ -n "$PIP_TRUSTED_HOST" ]]; then
        req_cmd+=(--trusted-host "$PIP_TRUSTED_HOST")
    fi
    req_cmd+=(-r "$APP_ROOT/requirements.txt")
    "${req_cmd[@]}"
}

log_info "Installing Python dependencies (using pip)..."
pip_install
log_ok "Python environment installed successfully."

# 7. Frontend Installation & Build (npm from internet or configured registry)
if [[ "$SKIP_FRONTEND" -eq 0 ]]; then
    if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
        log_info "Building Next.js frontend in $APP_ROOT/web..."
        (
            cd "$APP_ROOT/web"
            npm_install_args=(ci)
            if [[ -n "$NPM_REGISTRY" ]]; then
                npm_install_args+=(--registry "$NPM_REGISTRY")
            fi
            
            # If package-lock is slightly different on target node version, fallback to install
            if ! npm "${npm_install_args[@]}"; then
                log_warn "npm ci failed, retrying with npm install..."
                npm_install_args=(install)
                [[ -n "$NPM_REGISTRY" ]] && npm_install_args+=(--registry "$NPM_REGISTRY")
                npm "${npm_install_args[@]}"
            fi

            log_info "Compiling Next.js production build..."
            BACKEND_URL="http://127.0.0.1:8083" NEXT_TELEMETRY_DISABLED=1 npm run build
        )
        log_ok "Frontend built successfully."
    else
        log_warn "Node.js or npm not found in PATH; skipping Next.js frontend build."
    fi
fi

# 8. Secret Generation & Symlinks
ensure_link() {
    local link_path=$1 target=$2
    if [[ ! -e "$link_path" && ! -L "$link_path" ]]; then
        ln -s "$target" "$link_path"
    fi
}

ensure_link "$APP_ROOT/users.json" "$GC_USERS_FILE"
ensure_link "$APP_ROOT/.session_secret" "$SESSION_SECRET_FILE"
ensure_link "$APP_ROOT/clusters" "$CLUSTERS_ROOT"
ensure_link "$APP_ROOT/config.json" "$CONFIG_ROOT/config.json"

if [[ ! -f "$SESSION_SECRET_FILE" ]]; then
    "$PYTHON_ROOT/bin/python" -c 'import secrets; print(secrets.token_hex(32))' > "$SESSION_SECRET_FILE"
fi

if [[ ! -f "$GC_USERS_FILE" ]]; then
    log_info "Creating bootstrap administrator credentials..."
    (
        cd "$APP_ROOT"
        GC_USERS_FILE="$GC_USERS_FILE" "$PYTHON_ROOT/bin/python" - <<'PY'
import json, os, secrets
from pathlib import Path
from gcanalyzer import auth

credentials = {
    "admin": os.environ.get("GC_ADMIN_PASSWORD") or secrets.token_urlsafe(20),
    "readonly": os.environ.get("GC_READONLY_PASSWORD") or secrets.token_urlsafe(20),
}
os.environ["GC_ADMIN_PASSWORD"] = credentials["admin"]
os.environ["GC_READONLY_PASSWORD"] = credentials["readonly"]

cred_file = Path(os.environ["GC_USERS_FILE"]).with_name("bootstrap-credentials.json")
with cred_file.open("w") as fh:
    os.chmod(cred_file, 0o600)
    json.dump(credentials, fh, indent=2)
    fh.write("\n")

auth.load_users()
print(f"Generated initial credentials saved to: {cred_file}")
PY
    )
fi

# 9. Apply Permissions
if [[ "$IS_ROOT" -eq 1 ]]; then
    log_info "Applying permissions..."
    chown -R root:"$SERVICE_GROUP" "$APP_ROOT"
    chmod -R u=rwX,g=rX,o= "$APP_ROOT"
    chmod 0750 "$APP_ROOT"

    chown -R "$SERVICE_USER:$SERVICE_GROUP" "$STATE_ROOT" "$CLUSTERS_ROOT"
    chmod 0750 "$STATE_ROOT" "$CLUSTERS_ROOT"

    chown root:"$SERVICE_GROUP" "$CONFIG_ROOT" "$ENV_FILE" "$SESSION_SECRET_FILE"
    chmod 0750 "$CONFIG_ROOT"
    chmod 0640 "$ENV_FILE" "$SESSION_SECRET_FILE"

    if [[ -f "$CONFIG_ROOT/config.json" ]]; then
        chown "$SERVICE_USER:$SERVICE_GROUP" "$CONFIG_ROOT/config.json"
        chmod 0640 "$CONFIG_ROOT/config.json"
    fi
    if [[ -f "$GC_USERS_FILE" ]]; then
        chown "$SERVICE_USER:$SERVICE_GROUP" "$GC_USERS_FILE"
        chmod 0600 "$GC_USERS_FILE"
    fi
    if [[ -f "$CONFIG_ROOT/bootstrap-credentials.json" ]]; then
        chown root:root "$CONFIG_ROOT/bootstrap-credentials.json"
        chmod 0600 "$CONFIG_ROOT/bootstrap-credentials.json"
    fi
fi

# 10. Systemd Service Setup
if [[ "$IS_ROOT" -eq 1 && "$SKIP_SYSTEMD" -eq 0 && -d "$SYSTEMD_UNIT_ROOT" ]] && command -v systemctl >/dev/null 2>&1; then
    log_info "Installing systemd service units..."
    
    # Backend unit
    cat > "$SYSTEMD_UNIT_ROOT/gc-analyzer-backend.service" <<UNIT
[Unit]
Description=GC Analyzer Backend Service
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$APP_ROOT
EnvironmentFile=-$ENV_FILE
Environment=GC_DB=$STATE_ROOT/gc_history.db
Environment=GC_USERS_FILE=$GC_USERS_FILE
Environment=GC_PROMETHEUS_CONFIG=$STATE_ROOT/prometheus.json
Environment=GC_SCHED_ENABLED=1
ExecStart=$PYTHON_ROOT/bin/python -m gcanalyzer.app --host 127.0.0.1 --port 8083 --db $STATE_ROOT/gc_history.db
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

    # Frontend unit
    NODE_BIN=$(command -v node || echo "/usr/bin/node")
    cat > "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service" <<UNIT
[Unit]
Description=GC Analyzer Frontend Service
After=network.target gc-analyzer-backend.service
Requires=gc-analyzer-backend.service

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_GROUP
WorkingDirectory=$APP_ROOT/web
Environment=HOME=$STATE_ROOT
Environment=NODE_ENV=production
Environment=BACKEND_URL=http://127.0.0.1:8083
ExecStart=$NODE_BIN $APP_ROOT/web/node_modules/next/dist/bin/next start -H 0.0.0.0 -p 3000
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

    # gc-analyzerctl CLI
    cat > /usr/local/sbin/gc-analyzerctl <<'CONTROL'
#!/usr/bin/env bash
set -euo pipefail
CMD="${1:-help}"
case "$CMD" in
    start|stop|restart|status)
        systemctl "$CMD" gc-analyzer-frontend.service gc-analyzer-backend.service
        ;;
    logs)
        shift || true
        journalctl -u gc-analyzer-backend -u gc-analyzer-frontend -f "$@"
        ;;
    *)
        echo "Usage: gc-analyzerctl {start|stop|restart|status|logs}"
        exit 1
        ;;
esac
CONTROL
    chmod 0755 /usr/local/sbin/gc-analyzerctl

    systemctl daemon-reload
    systemctl enable gc-analyzer-backend.service gc-analyzer-frontend.service

    if [[ "$START_SERVICES" -eq 1 ]]; then
        log_info "Starting GC Analyzer services..."
        systemctl restart gc-analyzer-backend.service gc-analyzer-frontend.service
        log_ok "GC Analyzer services started."
    fi
fi

log_ok "============================================================"
log_ok " GC Analyzer Installation Complete!"
log_ok " Dashboard URL: http://<SERVER_IP>:3000"
log_ok " Backend API:   http://127.0.0.1:8083"
log_ok " Control tool:  gc-analyzerctl {start|stop|restart|status|logs}"
if [[ -f "$CONFIG_ROOT/bootstrap-credentials.json" ]]; then
    log_info " Initial credentials saved in: $CONFIG_ROOT/bootstrap-credentials.json"
fi
log_ok "============================================================"
