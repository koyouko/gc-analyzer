#!/usr/bin/env bash
set -euo pipefail
set -E
umask 027

EXPECTED_RHEL_VERSION="8.10"
EXPECTED_ARCH="x86_64"
MIN_FREE_KB=${GC_ANALYZER_MIN_FREE_KB:-2097152}
SERVICE_USER="gc-analyzer"
SERVICE_GROUP="gc-analyzer"
APP_ROOT="/opt/gc-analyzer"
STATE_ROOT="/var/lib/gc-analyzer"
CONFIG_ROOT="/etc/gc-analyzer"
CLUSTERS_ROOT="$CONFIG_ROOT/clusters"
ENV_FILE="$CONFIG_ROOT/gc-analyzer.env"
GC_USERS_FILE="$CONFIG_ROOT/users.json"
SESSION_SECRET_FILE="$CONFIG_ROOT/.session_secret"
GC_DB="$STATE_ROOT/gc_history.db"
NODE_ROOT="$APP_ROOT/runtime/node"
PYTHON_ROOT="$APP_ROOT/.venv"
CURRENT_STAGE="startup"
CONTAINER_TEST=0
BUNDLE_ROOT=""
BACKEND_WAS_ACTIVE=0
FRONTEND_WAS_ACTIVE=0
ROLLBACK_ARMED=0
ROLLBACK_ROOT=""
ROLLBACK_PARENT=""
BACKUP_COMPLETE=0
MOVED_PATHS=()
SYSTEMD_UNIT_ROOT="/etc/systemd/system"

fail() {
    printf 'ERROR [%s]: %s\n' "$CURRENT_STAGE" "$1" >&2
    exit 1
}

on_error() {
    local line=$1
    local status=$2
    printf 'ERROR [%s]: command failed at line %s (status %s)\n' \
        "$CURRENT_STAGE" "$line" "$status" >&2
    exit "$status"
}
trap 'on_error "$LINENO" "$?"' ERR

require_command() {
    command -v "$1" >/dev/null 2>&1 \
        || fail "required command is unavailable: $1"
}

parse_arguments() {
    (($# <= 1)) || fail "unexpected argument: ${2:-}"
    if (($# == 1)); then
        [[ ${1:-} == "--container-test" ]] \
            || fail "unexpected argument: $1"
        CONTAINER_TEST=1
    fi
}

preflight() {
    local free_kb
    CURRENT_STAGE="preflight"
    [[ "$EUID" -eq 0 ]] || fail "installer must run as root"
    [[ -r /etc/os-release ]] || fail "/etc/os-release is unavailable"
    # shellcheck disable=SC1091
    . /etc/os-release
    [[ "$ID" == "rhel" ]] || fail "operating system must be RHEL"
    [[ "$VERSION_ID" == "8.10" ]] \
        || fail "RHEL version must be exactly $EXPECTED_RHEL_VERSION (found $VERSION_ID)"
    [[ "$(uname -m)" == "x86_64" ]] \
        || fail "architecture must be exactly $EXPECTED_ARCH"

    for command in awk cut df dnf find grep id mkdir mktemp readlink \
        sed sha256sum sort stat tr uname wc; do
        require_command "$command"
    done
    [[ "$MIN_FREE_KB" =~ ^[0-9]+$ ]] \
        || fail "GC_ANALYZER_MIN_FREE_KB must be a positive integer"
    free_kb=$(df -Pk / | awk 'NR == 2 {print $4}')
    [[ "$free_kb" =~ ^[0-9]+$ ]] || fail "unable to determine free disk space"
    ((free_kb >= MIN_FREE_KB)) \
        || fail "at least $MIN_FREE_KB KiB free space is required"
}

validate_relative_path() {
    local path=$1
    [[ -n "$path" ]] || fail "manifest contains an empty path"
    [[ "$path" != /* ]] || fail "manifest contains an absolute path: $path"
    [[ "$path" != "." && "$path" != ".." ]] \
        || fail "manifest contains an unsafe path: $path"
    case "/$path/" in
        */../*|*/./*) fail "manifest contains an unsafe path: $path" ;;
    esac
    [[ "$path" != *$'\n'* && "$path" != *$'\r'* && "$path" != *$'\t'* ]] \
        || fail "manifest contains ambiguous path text"
}

verify_manifest_records() {
    local entry_type mode path extra link_path target
    while IFS=$'\t' read -r entry_type mode path extra || [[ -n "$entry_type" ]]; do
        [[ -z "${extra:-}" ]] || fail "MANIFEST.paths has an invalid record"
        case "$entry_type" in file|directory|symlink) ;; *)
            fail "MANIFEST.paths has an invalid type: $entry_type" ;;
        esac
        [[ "$mode" =~ ^0[0-7]{3}$ ]] \
            || fail "MANIFEST.paths has an invalid mode: $mode"
        validate_relative_path "$path"
    done < MANIFEST.paths

    while IFS=$'\t' read -r link_path target extra || [[ -n "$link_path" ]]; do
        [[ -z "${extra:-}" ]] || fail "MANIFEST.symlinks has an invalid record"
        validate_relative_path "$link_path"
        [[ -n "$target" && "$target" != /* ]] \
            || fail "MANIFEST.symlinks has an unsafe target"
        [[ "$target" != *$'\n'* && "$target" != *$'\r'* && "$target" != *$'\t'* ]] \
            || fail "MANIFEST.symlinks has ambiguous target text"
    done < MANIFEST.symlinks
}

verify_shell_inventory() {
    local temporary path relative entry_type mode target canonical_root resolved_target
    temporary=$(mktemp -d)
    trap 'rm -rf -- "$temporary"' RETURN
    canonical_root=$(readlink -f -- "$BUNDLE_ROOT") \
        || fail "unable to resolve canonical bundle root: $BUNDLE_ROOT"
    [[ -d "$canonical_root" && ! -L "$canonical_root" ]] \
        || fail "bundle root must be a real directory: $BUNDLE_ROOT"
    canonical_root=${canonical_root%/}

    while IFS= read -r -d '' path; do
        relative=${path#./}
        validate_relative_path "$relative"
        if [[ -L "$path" ]]; then
            entry_type="symlink"
            mode="0777"
            target=$(readlink -- "$path")
            [[ -n "$target" && "$target" != /* ]] \
                || fail "payload symlink has an absolute or empty target: $relative"
            [[ "$target" != *$'\n'* && "$target" != *$'\r'* && "$target" != *$'\t'* ]] \
                || fail "payload contains an ambiguous symlink target"
            if ! resolved_target=$(readlink -f -- "$path"); then
                fail "payload symlink is broken or cyclic: $relative -> $target"
            fi
            [[ -e "$resolved_target" ]] \
                || fail "payload symlink is broken: $relative -> $target"
            case "$resolved_target" in
                "$canonical_root"/*) ;;
                *) fail "payload symlink escapes bundle root: $relative -> $target" ;;
            esac
            printf '%s\t%s\n' "$relative" "$target" \
                >> "$temporary/MANIFEST.symlinks.unsorted"
        elif [[ -f "$path" ]]; then
            entry_type="file"
            mode=$(stat -c '%a' -- "$path")
            mode="0000$mode"
            mode=${mode: -4}
        elif [[ -d "$path" ]]; then
            entry_type="directory"
            mode=$(stat -c '%a' -- "$path")
            mode="0000$mode"
            mode=${mode: -4}
        else
            fail "payload contains unsupported entry type: $relative"
        fi
        printf '%s\t%s\t%s\n' "$entry_type" "$mode" "$relative" \
            >> "$temporary/MANIFEST.paths.unsorted"
    done < <(
        find . -mindepth 1 \
            ! -path './MANIFEST.paths' \
            ! -path './MANIFEST.symlinks' \
            ! -path './MANIFEST.sha256' -print0 | LC_ALL=C sort -z
    )

    cp "$temporary/MANIFEST.paths.unsorted" "$temporary/MANIFEST.paths"
    if [[ -f "$temporary/MANIFEST.symlinks.unsorted" ]]; then
        cp "$temporary/MANIFEST.symlinks.unsorted" \
            "$temporary/MANIFEST.symlinks"
    else
        : > "$temporary/MANIFEST.symlinks"
    fi
    [[ "$(sha256sum MANIFEST.paths | awk '{print $1}')" == \
        "$(sha256sum "$temporary/MANIFEST.paths" | awk '{print $1}')" ]] \
        || fail "MANIFEST.paths does not match the extracted payload"
    [[ "$(sha256sum MANIFEST.symlinks | awk '{print $1}')" == \
        "$(sha256sum "$temporary/MANIFEST.symlinks" | awk '{print $1}')" ]] \
        || fail "MANIFEST.symlinks does not match the extracted payload"

    rm -rf -- "$temporary"
    trap - RETURN
}

verify_bundle_before_install() {
    CURRENT_STAGE="pre-install bundle verification"
    cd "$BUNDLE_ROOT"
    for path in MANIFEST.sha256 MANIFEST.paths MANIFEST.symlinks inventory.py \
        app rpms node-runtime python-wheels npm-cache verify-offline.sh; do
        [[ -e "$path" || -L "$path" ]] || fail "bundle entry is missing: $path"
    done
    [[ -f MANIFEST.sha256 && ! -L MANIFEST.sha256 ]] \
        || fail "MANIFEST.sha256 must be a regular file"
    [[ -f MANIFEST.paths && ! -L MANIFEST.paths ]] \
        || fail "MANIFEST.paths must be a regular file"
    [[ -f MANIFEST.symlinks && ! -L MANIFEST.symlinks ]] \
        || fail "MANIFEST.symlinks must be a regular file"
    sha256sum -c MANIFEST.sha256
    verify_manifest_records
    verify_shell_inventory
}

install_local_rpms() {
    local rpms dnf_options candidate
    local compatible_rpms=()
    CURRENT_STAGE="local RPM installation"
    shopt -s nullglob
    rpms=("$BUNDLE_ROOT"/rpms/*.rpm)
    shopt -u nullglob
    ((${#rpms[@]} > 0)) || fail "bundle contains no RPM files"
    # UBI uses the equivalent single-binary coreutils provider. Keep that
    # installed provider instead of erasing OS packages to install the split one.
    if rpm -q coreutils-single >/dev/null 2>&1; then
        for candidate in "${rpms[@]}"; do
            case "${candidate##*/}" in coreutils-[0-9]*.rpm) continue ;; esac
            compatible_rpms+=("$candidate")
        done
        rpms=("${compatible_rpms[@]}")
    fi
    dnf_options=(-y '--disablerepo=*' '--disableplugin=*' --setopt=install_weak_deps=False)
    dnf "${dnf_options[@]}" install "${rpms[@]}"
}

verify_python_inventory() {
    CURRENT_STAGE="post-RPM bundle verification"
    require_command python3.12
    python3.12 "$BUNDLE_ROOT/inventory.py" --verify "$BUNDLE_ROOT"
}

ensure_service_account() {
    CURRENT_STAGE="service account setup"
    for command in groupadd useradd getent install ln mv runuser; do
        require_command "$command"
    done
    if ! getent group "$SERVICE_GROUP" >/dev/null; then
        groupadd --system gc-analyzer
    fi
    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        useradd --system --gid "$SERVICE_GROUP" --home-dir "$STATE_ROOT" \
            --create-home --shell /sbin/nologin gc-analyzer
    fi
    [[ "$(id -gn "$SERVICE_USER")" == "$SERVICE_GROUP" ]] \
        || fail "existing gc-analyzer user has an unexpected primary group"
}

move_if_destination_absent() {
    local source=$1
    local destination=$2
    if [[ -e "$source" || -L "$source" ]]; then
        if [[ ! -e "$destination" && ! -L "$destination" ]]; then
            mkdir -p "$(dirname "$destination")"
            mv -- "$source" "$destination"
        else
            printf 'Preserving both legacy and current state: %s, %s\n' \
                "$source" "$destination" >&2
        fi
    fi
}

migrate_legacy_state() {
    local legacy name
    CURRENT_STAGE="legacy state migration"
    mkdir -p "$STATE_ROOT" "$CONFIG_ROOT"
    if [[ -d "$APP_ROOT" && ! -L "$APP_ROOT" ]]; then
        shopt -s nullglob
        for legacy in \
            "$APP_ROOT"/*.db \
            "$APP_ROOT"/*.db-wal \
            "$APP_ROOT"/*.db-shm \
            "$APP_ROOT"/*.db-journal \
            "$APP_ROOT"/*.sqlite \
            "$APP_ROOT"/*.sqlite-wal \
            "$APP_ROOT"/*.sqlite-shm \
            "$APP_ROOT"/*.sqlite-journal \
            "$APP_ROOT"/*.sqlite3 \
            "$APP_ROOT"/*.sqlite3-wal \
            "$APP_ROOT"/*.sqlite3-shm \
            "$APP_ROOT"/*.sqlite3-journal; do
            name=${legacy##*/}
            move_if_destination_absent "$legacy" "$STATE_ROOT/$name"
        done
        shopt -u nullglob
        move_if_destination_absent "$APP_ROOT/users.json" "$GC_USERS_FILE"
        move_if_destination_absent "$APP_ROOT/.session_secret" "$SESSION_SECRET_FILE"
        move_if_destination_absent "$APP_ROOT/.env" "$ENV_FILE"
        move_if_destination_absent "$APP_ROOT/config.json" "$CONFIG_ROOT/config.json"
        move_if_destination_absent "$APP_ROOT/prometheus.json" "$STATE_ROOT/prometheus.json"
        move_if_destination_absent "$APP_ROOT/clusters" "$CLUSTERS_ROOT"
    fi
}

stop_existing_services() {
    if [[ "$CONTAINER_TEST" == "0" ]] && command -v systemctl >/dev/null 2>&1; then
        systemctl stop gc-analyzer-frontend.service gc-analyzer-backend.service \
            >/dev/null 2>&1 || true
    fi
}

capture_service_state() {
    BACKEND_WAS_ACTIVE=0
    FRONTEND_WAS_ACTIVE=0
    if [[ "$CONTAINER_TEST" == "0" ]] && command -v systemctl >/dev/null 2>&1; then
        if systemctl is-active --quiet gc-analyzer-backend.service; then
            BACKEND_WAS_ACTIVE=1
        fi
        if systemctl is-active --quiet gc-analyzer-frontend.service; then
            FRONTEND_WAS_ACTIVE=1
        fi
    fi
}

application_payload_paths() {
    REPLACE_PATHS=(
        "$APP_ROOT/frontend"
        "$APP_ROOT/gcanalyzer"
        "$APP_ROOT/seed"
        "$APP_ROOT/web"
        "$APP_ROOT/runtime"
        "$APP_ROOT/.venv"
        "$APP_ROOT/requirements.txt"
        "$APP_ROOT/requirements-offline.txt"
        "$APP_ROOT/manage-app.sh"
        "$APP_ROOT/README.md"
        "$APP_ROOT/architecture_and_user_guide.html"
        "$SYSTEMD_UNIT_ROOT/gc-analyzer-backend.service"
        "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service"
    )
}

backup_application_payload() {
    local path
    application_payload_paths
    ROLLBACK_PARENT=${ROLLBACK_PARENT:-"$(dirname "$APP_ROOT")"}
    ROLLBACK_ROOT=$(mktemp -d "$ROLLBACK_PARENT/.gc-analyzer-rollback.XXXXXX")
    BACKUP_COMPLETE=0
    MOVED_PATHS=()
    ROLLBACK_ARMED=1
    for path in "${REPLACE_PATHS[@]}"; do
        if [[ -e "$path" || -L "$path" ]]; then
            mv -- "$path" "$ROLLBACK_ROOT/${path##*/}"
            MOVED_PATHS+=("$path")
        fi
    done
    BACKUP_COMPLETE=1
}

restore_previously_active_services() {
    if [[ "$BACKEND_WAS_ACTIVE" == "1" ]]; then
        systemctl start gc-analyzer-backend.service
    fi
    if [[ "$FRONTEND_WAS_ACTIVE" == "1" ]]; then
        systemctl start gc-analyzer-frontend.service
    fi
}

rollback_failed_install() {
    local path backup
    if [[ "$ROLLBACK_ARMED" == "1" ]]; then
        application_payload_paths
        if [[ "$BACKUP_COMPLETE" == "1" ]]; then
            if [[ "$CONTAINER_TEST" == "0" ]]; then
                systemctl stop gc-analyzer-frontend.service \
                    gc-analyzer-backend.service >/dev/null 2>&1 || true
            fi
            rm -rf -- "${REPLACE_PATHS[@]}"
        fi
        for path in "${MOVED_PATHS[@]}"; do
            backup="$ROLLBACK_ROOT/${path##*/}"
            if [[ -e "$backup" || -L "$backup" ]]; then
                if ! mv -- "$backup" "$path"; then
                    printf 'Rollback needs manual recovery; backup retained: %s\n' "$ROLLBACK_ROOT" >&2
                    return 1
                fi
            fi
        done
        rm -rf -- "$ROLLBACK_ROOT"
        if [[ "$CONTAINER_TEST" == "0" ]]; then
            systemctl daemon-reload || true
        fi
        ROLLBACK_ARMED=0
        ROLLBACK_ROOT=""
        BACKUP_COMPLETE=0
        MOVED_PATHS=()
    fi
    restore_previously_active_services
}

installation_exit() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        rollback_failed_install || true
    fi
    exit "$status"
}

complete_application_replacement() {
    if [[ "$ROLLBACK_ARMED" == "1" ]]; then
        rm -rf -- "$ROLLBACK_ROOT"
        ROLLBACK_ARMED=0
        ROLLBACK_ROOT=""
        BACKUP_COMPLETE=0
        MOVED_PATHS=()
    fi
    trap - EXIT
}

replace_application_payload() {
    CURRENT_STAGE="application payload installation"
    mkdir -p "$APP_ROOT"
    cp -a "$BUNDLE_ROOT/app/." "$APP_ROOT/"
    mkdir -p "$NODE_ROOT"
    cp -a "$BUNDLE_ROOT/node-runtime/." "$NODE_ROOT/"
    install -m 0750 "$BUNDLE_ROOT/verify-offline.sh" \
        "$APP_ROOT/runtime/verify-offline.sh"
}

ensure_persistent_link() {
    local link_path=$1
    local target=$2
    if [[ -L "$link_path" ]]; then
        [[ "$(readlink -- "$link_path")" == "$target" ]] \
            || fail "persistent link points somewhere unexpected: $link_path"
    elif [[ -e "$link_path" ]]; then
        fail "persistent app path conflicts with preserved state: $link_path"
    else
        ln -s -- "$target" "$link_path"
    fi
}

generate_session_secret() {
    local temporary_secret
    if [[ ! -e "$SESSION_SECRET_FILE" && ! -L "$SESSION_SECRET_FILE" ]]; then
        temporary_secret=$(mktemp "$CONFIG_ROOT/.session-secret.XXXXXX")
        python3.12 -c 'import secrets; print(secrets.token_hex(32))' \
            > "$temporary_secret"
        chmod 0640 "$temporary_secret"
        chown root:"$SERVICE_GROUP" "$temporary_secret"
        mv -- "$temporary_secret" "$SESSION_SECRET_FILE"
    fi
}

prepare_persistent_state() {
    CURRENT_STAGE="persistent state setup"
    mkdir -p "$STATE_ROOT" "$CONFIG_ROOT" "$CLUSTERS_ROOT"
    [[ -e "$ENV_FILE" ]] || install -m 0640 -o root -g "$SERVICE_GROUP" \
        /dev/null "$ENV_FILE"
    if [[ ! -e "$CONFIG_ROOT/config.json" ]]; then
        printf '{}\n' > "$CONFIG_ROOT/config.json"
    fi
    generate_session_secret
    ensure_persistent_link "$APP_ROOT/users.json" "$GC_USERS_FILE"
    ensure_persistent_link "$APP_ROOT/.session_secret" "$SESSION_SECRET_FILE"
    ensure_persistent_link "$APP_ROOT/clusters" "$CLUSTERS_ROOT"
    ensure_persistent_link "$APP_ROOT/config.json" "$CONFIG_ROOT/config.json"

    chown root:"$SERVICE_GROUP" "$CONFIG_ROOT"
    chmod 0750 "$CONFIG_ROOT"
    chown "$SERVICE_USER:$SERVICE_GROUP" "$STATE_ROOT"
    chmod 0750 "$STATE_ROOT"
    chown -R "$SERVICE_USER:$SERVICE_GROUP" "$CLUSTERS_ROOT"
    find "$CLUSTERS_ROOT" -type d -exec chmod 0750 {} +
    find "$CLUSTERS_ROOT" -type f -exec chmod 0640 {} +
    chmod 0750 "$CLUSTERS_ROOT"
    chown root:"$SERVICE_GROUP" "$ENV_FILE" "$SESSION_SECRET_FILE"
    chmod 0640 "$ENV_FILE" "$SESSION_SECRET_FILE"
    chown "$SERVICE_USER:$SERVICE_GROUP" "$CONFIG_ROOT/config.json"
    chmod 0640 "$CONFIG_ROOT/config.json"
    if [[ -e "$GC_USERS_FILE" ]]; then
        chown "$SERVICE_USER:$SERVICE_GROUP" "$GC_USERS_FILE"
        chmod 0600 "$GC_USERS_FILE"
    fi
    find "$STATE_ROOT" -maxdepth 1 -type f -exec chown "$SERVICE_USER:$SERVICE_GROUP" {} +
}

install_python_environment() {
    CURRENT_STAGE="offline Python environment"
    python3.12 -m venv "$PYTHON_ROOT"
    "$PYTHON_ROOT/bin/python" -m pip install --no-index \
        --find-links="$BUNDLE_ROOT/python-wheels" --only-binary=:all: \
        -r "$APP_ROOT/requirements-offline.txt"
    "$PYTHON_ROOT/bin/python" -m pip check
    "$PYTHON_ROOT/bin/python" -c \
        'import fastapi, uvicorn, paramiko, yaml, sklearn, sqlite3'
}

initialize_users_file() {
    CURRENT_STAGE="initial user database setup"
    if [[ ! -e "$GC_USERS_FILE" && ! -L "$GC_USERS_FILE" ]]; then
        (
            cd "$APP_ROOT"
            GC_USERS_FILE="$GC_USERS_FILE" "$PYTHON_ROOT/bin/python" - <<'PY'
import json
import os
from pathlib import Path
import secrets
from gcanalyzer import auth

credentials = {}
generated = {}
for user, variable in (
    ("admin", "GC_ADMIN_PASSWORD"),
    ("readonly", "GC_READONLY_PASSWORD"),
):
    password = os.environ.get(variable)
    if password is None:
        password = secrets.token_urlsafe(24)
        generated[user] = password
    credentials[user] = password
os.environ["GC_ADMIN_PASSWORD"] = credentials["admin"]
os.environ["GC_READONLY_PASSWORD"] = credentials["readonly"]
if generated:
    path = Path(os.environ["GC_USERS_FILE"]).with_name("bootstrap-credentials.json")
    with path.open("x") as output:
        os.chmod(path, 0o600)
        json.dump(generated, output, indent=2)
        output.write("\n")
    print(f"Generated initial passwords saved for root only: {path}")
auth.load_users()
PY
        )
        chown "$SERVICE_USER:$SERVICE_GROUP" "$GC_USERS_FILE"
        chmod 0600 "$GC_USERS_FILE"
    fi
}

install_frontend() {
    local npm_cli="$NODE_ROOT/lib/node_modules/npm/bin/npm-cli.js"
    CURRENT_STAGE="offline frontend build"
    [[ "$("$NODE_ROOT/bin/node" --version)" == "v22.22.3" ]] \
        || fail "bundled Node version is not v22.22.3"
    chown -R "$SERVICE_USER:$SERVICE_GROUP" "$APP_ROOT/web"
    (
        cd "$APP_ROOT/web"
        runuser -u gc-analyzer -- env \
            HOME="$STATE_ROOT" PATH="$NODE_ROOT/bin:/usr/bin:/bin" \
            "$NODE_ROOT/bin/node" "$npm_cli" ci --offline \
            --cache "$BUNDLE_ROOT/npm-cache" --no-audit
        runuser -u gc-analyzer -- env \
            HOME="$STATE_ROOT" PATH="$NODE_ROOT/bin:/usr/bin:/bin" \
            BACKEND_URL=http://127.0.0.1:8083 \
            "$NODE_ROOT/bin/node" "$npm_cli" run build
    )
    [[ -s "$APP_ROOT/web/.next/BUILD_ID" ]] \
        || fail "frontend production build did not create .next/BUILD_ID"
}

apply_permissions() {
    CURRENT_STAGE="permissions"
    chown -R root:"$SERVICE_GROUP" "$APP_ROOT"
    chmod -R u=rwX,g=rX,o= "$APP_ROOT"
    chmod 0750 "$APP_ROOT"
    if [[ -d "$APP_ROOT/web/.next/cache" ]]; then
        chown -R "$SERVICE_USER:$SERVICE_GROUP" "$APP_ROOT/web/.next/cache"
    fi
    chown "$SERVICE_USER:$SERVICE_GROUP" "$STATE_ROOT" "$CLUSTERS_ROOT"
}

write_service_files() {
    CURRENT_STAGE="systemd service installation"
    require_command systemctl
    mkdir -p "$SYSTEMD_UNIT_ROOT"
    cat > "$SYSTEMD_UNIT_ROOT/gc-analyzer-backend.service" <<'UNIT'
[Unit]
Description=GC Analyzer backend
After=network.target

[Service]
Type=simple
User=gc-analyzer
Group=gc-analyzer
WorkingDirectory=/opt/gc-analyzer
EnvironmentFile=-/etc/gc-analyzer/gc-analyzer.env
Environment=GC_DB=/var/lib/gc-analyzer/gc_history.db
Environment=GC_USERS_FILE=/etc/gc-analyzer/users.json
Environment=GC_PROMETHEUS_CONFIG=/var/lib/gc-analyzer/prometheus.json
Environment=GC_SCHED_ENABLED=1
ExecStart=/opt/gc-analyzer/.venv/bin/python -m gcanalyzer.app --host 127.0.0.1 --port 8083 --db /var/lib/gc-analyzer/gc_history.db
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT
    cat > "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service" <<'UNIT'
[Unit]
Description=GC Analyzer frontend
After=network.target gc-analyzer-backend.service
Requires=gc-analyzer-backend.service

[Service]
Type=simple
User=gc-analyzer
Group=gc-analyzer
WorkingDirectory=/opt/gc-analyzer/web
Environment=HOME=/var/lib/gc-analyzer
Environment=NODE_ENV=production
Environment=BACKEND_URL=http://127.0.0.1:8083
ExecStart=/opt/gc-analyzer/runtime/node/bin/node /opt/gc-analyzer/web/node_modules/next/dist/bin/next start -H 0.0.0.0 -p 3000
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

    cat > /usr/local/sbin/gc-analyzerctl <<'CONTROL'
#!/usr/bin/env bash
set -euo pipefail
BACKEND_WAS_ACTIVE=0
FRONTEND_WAS_ACTIVE=0

restore_after_verify() {
    if [[ "$BACKEND_WAS_ACTIVE" == "1" ]]; then
        systemctl start gc-analyzer-backend.service
    fi
    if [[ "$FRONTEND_WAS_ACTIVE" == "1" ]]; then
        systemctl start gc-analyzer-frontend.service
    fi
}

run_verifier() {
    if systemctl is-active --quiet gc-analyzer-backend.service; then
        BACKEND_WAS_ACTIVE=1
    fi
    if systemctl is-active --quiet gc-analyzer-frontend.service; then
        FRONTEND_WAS_ACTIVE=1
    fi
    systemctl stop gc-analyzer-frontend.service gc-analyzer-backend.service
    trap restore_after_verify EXIT
    /opt/gc-analyzer/runtime/verify-offline.sh
    trap - EXIT
    restore_after_verify
}

case "${1:-status}" in
    start) systemctl start gc-analyzer-backend.service gc-analyzer-frontend.service ;;
    stop) systemctl stop gc-analyzer-frontend.service gc-analyzer-backend.service ;;
    restart) systemctl restart gc-analyzer-backend.service gc-analyzer-frontend.service ;;
    status) systemctl status gc-analyzer-backend.service gc-analyzer-frontend.service ;;
    logs) journalctl -u gc-analyzer-backend.service -u gc-analyzer-frontend.service "${@:2}" ;;
    verify) run_verifier ;;
    *) printf 'usage: %s {start|stop|restart|status|logs|verify}\n' "$0" >&2; exit 2 ;;
esac
CONTROL
    chmod 0644 "$SYSTEMD_UNIT_ROOT/gc-analyzer-backend.service" \
        "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service"
    chmod 0750 /usr/local/sbin/gc-analyzerctl
}

validate_service_files() {
    CURRENT_STAGE="systemd service validation"
    require_command systemd-analyze
    systemd-analyze verify \
        "$SYSTEMD_UNIT_ROOT/gc-analyzer-backend.service" \
        "$SYSTEMD_UNIT_ROOT/gc-analyzer-frontend.service"
}

verify_installation() {
    CURRENT_STAGE="installed application verification"
    "$BUNDLE_ROOT/verify-offline.sh"
}

enable_services() {
    CURRENT_STAGE="systemd activation"
    systemctl daemon-reload
    systemctl enable gc-analyzer-backend.service gc-analyzer-frontend.service
    systemctl start gc-analyzer-backend.service gc-analyzer-frontend.service
}

main() {
    parse_arguments "$@"
    BUNDLE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
    preflight
    verify_bundle_before_install
    install_local_rpms
    verify_python_inventory
    ensure_service_account
    capture_service_state
    stop_existing_services
    trap installation_exit EXIT
    migrate_legacy_state
    backup_application_payload
    replace_application_payload
    prepare_persistent_state
    install_python_environment
    initialize_users_file
    install_frontend
    apply_permissions
    verify_installation
    write_service_files
    validate_service_files
    if [[ "$CONTAINER_TEST" == "1" ]]; then
        complete_application_replacement
        printf 'Container-test installation and verification completed successfully.\n'
    else
        enable_services
        complete_application_replacement
        printf 'GC Analyzer installed. Frontend: http://0.0.0.0:3000\n'
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
