#!/usr/bin/env bash
set -euo pipefail
set -E
umask 077

APP_ROOT=${GC_ANALYZER_APP_ROOT:-/opt/gc-analyzer}
STATE_ROOT=${GC_ANALYZER_STATE_ROOT:-/var/lib/gc-analyzer}
SERVICE_USER="gc-analyzer"
PYTHON_BIN="$APP_ROOT/.venv/bin/python"
NODE_ROOT="$APP_ROOT/runtime/node"
NODE_BIN="$NODE_ROOT/bin/node"
NPM_CLI="$NODE_ROOT/lib/node_modules/npm/bin/npm-cli.js"
EXPECTED_NODE_VERSION="v22.22.3"
EXPECTED_NPM_MAJOR="10"
BACKEND_HOST=${GC_VERIFY_BACKEND_HOST:-127.0.0.1}
BACKEND_PORT=${GC_VERIFY_BACKEND_PORT:-8083}
FRONTEND_HOST=${GC_VERIFY_FRONTEND_HOST:-127.0.0.1}
FRONTEND_PORT=${GC_VERIFY_FRONTEND_PORT:-3000}
BACKEND_URL="http://${BACKEND_HOST}:${BACKEND_PORT}"
FRONTEND_URL="http://${FRONTEND_HOST}:${FRONTEND_PORT}/"
FRONTEND_API_URL="http://${FRONTEND_HOST}:${FRONTEND_PORT}/api/health"
TEMP_ROOT=""
BACKEND_PID=""
FRONTEND_PID=""
BACKEND_LOG=""
FRONTEND_LOG=""

show_recent_logs() {
    local log
    for log in "$BACKEND_LOG" "$FRONTEND_LOG"; do
        if [[ -n "$log" && -f "$log" ]]; then
            printf '\nRecent output from %s:\n' "$log" >&2
            tail -n 60 "$log" >&2 || true
        fi
    done
}

fail() {
    printf 'ERROR: %s\n' "$1" >&2
    show_recent_logs
    exit 1
}

terminate_process() {
    local pid=${1:-}
    local attempt
    [[ -n "$pid" ]] || return 0
    if kill -0 "$pid" >/dev/null 2>&1; then
        kill -TERM -- "-$pid" >/dev/null 2>&1 || true
        for attempt in {1..20}; do
            kill -0 "$pid" >/dev/null 2>&1 || return 0
            sleep 0.25
        done
        kill -KILL -- "-$pid" >/dev/null 2>&1 || true
        wait "$pid" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    local status=$?
    trap - EXIT
    terminate_process "$FRONTEND_PID"
    terminate_process "$BACKEND_PID"
    [[ -z "$TEMP_ROOT" ]] || rm -rf -- "$TEMP_ROOT"
    exit "$status"
}
trap cleanup EXIT

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command is unavailable: $1"
}

assert_port_available() {
    local host=$1
    local port=$2
    local label=$3
    if (exec 9<>"/dev/tcp/$host/$port") 2>/dev/null; then
        exec 9>&- || true
        fail "$label address is already in use: $host:$port"
    fi
}

require_alive() {
    local pid=$1
    local label=$2
    kill -0 "$pid" >/dev/null 2>&1 || fail "$label process exited unexpectedly"
}

wait_for_http_200() {
    local url=$1
    local label=$2
    local attempt status
    for attempt in {1..80}; do
        require_alive "$BACKEND_PID" "backend"
        require_alive "$FRONTEND_PID" "frontend"
        status=$(curl --silent --show-error --max-time 2 \
            --output /dev/null --write-out '%{http_code}' "$url" 2>/dev/null || true)
        if [[ "$status" == "200" ]]; then
            return 0
        fi
        sleep 0.25
    done
    fail "$label did not return HTTP 200: $url"
}

verify_runtimes() {
    local npm_version
    [[ "$EUID" -eq 0 ]] || fail "verification must run as root"
    require_command curl
    require_command kill
    require_command mktemp
    require_command runuser
    require_command setsid
    require_command tail
    [[ -x "$PYTHON_BIN" ]] || fail "installed Python is missing: $PYTHON_BIN"
    [[ -x "$NODE_BIN" ]] || fail "installed Node is missing: $NODE_BIN"
    [[ -f "$NPM_CLI" ]] || fail "installed npm CLI is missing: $NPM_CLI"
    [[ -s "$APP_ROOT/web/.next/BUILD_ID" ]] \
        || fail "frontend build marker is missing: web/.next/BUILD_ID"

    "$PYTHON_BIN" -c \
        'import sys; assert sys.version_info[:2] == (3, 12); import fastapi, uvicorn, paramiko, yaml, sklearn, sqlite3'
    [[ "$("$NODE_BIN" --version)" == "$EXPECTED_NODE_VERSION" ]] \
        || fail "Node version must be exactly $EXPECTED_NODE_VERSION"
    npm_version=$("$NODE_BIN" "$NPM_CLI" --version)
    [[ "${npm_version%%.*}" == "$EXPECTED_NPM_MAJOR" ]] \
        || fail "npm major version must be exactly $EXPECTED_NPM_MAJOR"
    runuser -u gc-analyzer -- env \
        HOME="$STATE_ROOT" PATH="$NODE_ROOT/bin:/usr/bin:/bin" \
        "$NODE_BIN" "$NPM_CLI" ls --offline --omit=dev --all \
        --prefix "$APP_ROOT/web" >/dev/null
}

prepare_temporary_state() {
    TEMP_ROOT=$(mktemp -d /var/tmp/gc-analyzer-verify.XXXXXXXX)
    BACKEND_LOG="$TEMP_ROOT/backend.log"
    FRONTEND_LOG="$TEMP_ROOT/frontend.log"
    mkdir -p "$TEMP_ROOT/home" "$TEMP_ROOT/config"
    chown -R "$SERVICE_USER:$SERVICE_USER" "$TEMP_ROOT"
}

start_backend() {
    local session_secret=$1
    (
        cd "$APP_ROOT"
        exec setsid runuser -u gc-analyzer -- env \
            HOME="$TEMP_ROOT/home" \
            GC_DB="$TEMP_ROOT/verify.db" \
            GC_USERS_FILE="$TEMP_ROOT/users.json" \
            GC_CONFIG_DIR="$TEMP_ROOT/config" \
            GC_PROMETHEUS_CONFIG="$TEMP_ROOT/prometheus.json" \
            GC_SESSION_SECRET="$session_secret" \
            GC_SCHED_ENABLED="0" \
            "$PYTHON_BIN" -m gcanalyzer.app \
                --host "$BACKEND_HOST" --port "$BACKEND_PORT" \
                --db "$TEMP_ROOT/verify.db"
    ) >"$BACKEND_LOG" 2>&1 &
    BACKEND_PID=$!
    printf '%s\n' "$BACKEND_PID" > "$TEMP_ROOT/backend.pid"
}

start_frontend() {
    (
        cd "$APP_ROOT/web"
        exec setsid runuser -u gc-analyzer -- env \
            HOME="$TEMP_ROOT/home" \
            PATH="$NODE_ROOT/bin:/usr/bin:/bin" \
            NODE_ENV=production \
            BACKEND_URL="$BACKEND_URL" \
            "$NODE_BIN" "$APP_ROOT/web/node_modules/next/dist/bin/next" \
                start -H "$FRONTEND_HOST" -p "$FRONTEND_PORT"
    ) >"$FRONTEND_LOG" 2>&1 &
    FRONTEND_PID=$!
    printf '%s\n' "$FRONTEND_PID" > "$TEMP_ROOT/frontend.pid"
}

main() {
    local session_secret
    (($# == 0)) || fail "verify-offline.sh accepts no arguments"
    verify_runtimes
    assert_port_available "$BACKEND_HOST" "$BACKEND_PORT" "backend"
    assert_port_available "$FRONTEND_HOST" "$FRONTEND_PORT" "frontend"
    prepare_temporary_state
    session_secret=$("$PYTHON_BIN" -c 'import secrets; print(secrets.token_hex(32))')
    start_backend "$session_secret"
    start_frontend
    wait_for_http_200 "$BACKEND_URL/api/health" "backend health endpoint"
    wait_for_http_200 "$FRONTEND_URL" "frontend root"
    wait_for_http_200 "$FRONTEND_API_URL" "frontend proxy health endpoint"
    wait_for_http_200 "${FRONTEND_URL}assets/prometheus-settings.js" "Prometheus settings asset"
    curl --fail --silent --show-error --max-time 5 "$FRONTEND_URL" \
        | grep '/assets/prometheus-settings.js' >/dev/null \
        || fail "frontend does not serve the current dashboard"
    require_alive "$BACKEND_PID" "backend"
    require_alive "$FRONTEND_PID" "frontend"
    printf 'Offline installation verified: Python, Node, npm, backend, frontend, and proxy are healthy.\n'
}

main "$@"
