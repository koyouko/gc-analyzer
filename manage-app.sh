#!/usr/bin/env bash
# BSP Kafka GC Analyzer - single application control script.
#
# Usage:
#   ./manage-app.sh start [port] [db_path] [--open]
#   ./manage-app.sh deploy [port] [db_path] [--open]
#   ./manage-app.sh stop [port]
#   ./manage-app.sh restart [port] [db_path] [--open]
#   ./manage-app.sh status [port]
#   ./manage-app.sh logs [port]
#   ./manage-app.sh seed [db_path]
#   ./manage-app.sh open [port]
#   ./manage-app.sh setup
#
# Environment:
#   PYTHON=python3.14   Force interpreter for .venv creation.
#   GC_HOST=0.0.0.0     Bind host, defaults to 127.0.0.1.
#   GC_PORT=8083        Default port.
#   GC_DB=gc_live.db    Default SQLite DB path.

set -euo pipefail
cd "$(dirname "$0")"

load_env() {
  if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
  fi
}

pick_python() {
  if [ -n "${PYTHON:-}" ] && command -v "$PYTHON" >/dev/null 2>&1; then
    echo "$PYTHON"
    return 0
  fi

  local candidate version
  for candidate in python3.14 python3.13 python3.12 python3.11 python3.10 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
      version="$("$candidate" -c 'import sys; print(sys.version_info[0] * 100 + sys.version_info[1])' 2>/dev/null || echo 0)"
      if [ "$version" -ge 310 ]; then
        echo "$candidate"
        return 0
      fi
    fi
  done
  return 1
}

usage() {
  cat <<'EOF'
BSP Kafka GC Analyzer control script

Usage:
  ./manage-app.sh start [port] [db_path] [--open]
  ./manage-app.sh deploy [port] [db_path] [--open]
  ./manage-app.sh stop [port]
  ./manage-app.sh restart [port] [db_path] [--open]
  ./manage-app.sh status [port]
  ./manage-app.sh logs [port]
  ./manage-app.sh seed [db_path]
  ./manage-app.sh open [port]
  ./manage-app.sh setup

Environment:
  PYTHON=python3.14   Force interpreter for .venv creation.
  GC_HOST=0.0.0.0     Bind host, defaults to 127.0.0.1.
  GC_PORT=8083        Default port.
  GC_DB=gc_live.db    Default SQLite DB path.
EOF
}

load_env

CMD="${1:-help}"
PORT="${2:-${GC_PORT:-8083}}"
DB_PATH="${3:-${GC_DB:-}}"
HOST="${GC_HOST:-127.0.0.1}"
PID_FILE="gcanalyzer-${PORT}.pid"
LOG_FILE="gcanalyzer-${PORT}.log"
OPEN_AFTER=0

for arg in "$@"; do
  if [ "$arg" = "--open" ]; then
    OPEN_AFTER=1
  fi
done
if [ "${DB_PATH:-}" = "--open" ]; then
  DB_PATH=""
fi

ensure_venv() {
  local py version
  py="$(pick_python || true)"
  if [ -z "$py" ]; then
    echo "ERROR: No Python 3.10+ found."
    echo "Install Python 3.10+ or run with PYTHON=python3.14 ./manage-app.sh setup"
    return 1
  fi

  if [ -d .venv ]; then
    version="$(.venv/bin/python -c 'import sys; print(sys.version_info[0] * 100 + sys.version_info[1])' 2>/dev/null || echo 0)"
    if [ "$version" -lt 310 ]; then
      echo "Existing .venv is older than Python 3.10; recreating it."
      rm -rf .venv
    fi
  fi

  if [ ! -d .venv ]; then
    echo "Creating virtual environment with $("$py" --version 2>&1)..."
    "$py" -m venv .venv
  fi

  echo "Installing/updating Python dependencies..."
  .venv/bin/python -m pip install --quiet --upgrade pip
  .venv/bin/python -m pip install --quiet -r requirements.txt
}

python_bin() {
  if [ -x .venv/bin/python ]; then
    echo ".venv/bin/python"
  else
    pick_python
  fi
}

seed_demo_history() {
  ensure_venv
  local py db has_data
  py="$(python_bin)"
  db="${1:-${DB_PATH:-${GC_DB:-gc_history.db}}}"
  export GC_DB="$db"

  has_data="$("$py" - <<'PY'
import os
import sqlite3

db = os.environ.get("GC_DB", "gc_history.db")
try:
    count = sqlite3.connect(db).execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
except Exception:
    count = 0
print("1" if count > 0 else "0")
PY
)"

  if [ "$has_data" = "1" ]; then
    echo "Demo/history database already has metric rows: $db"
    return 0
  fi

  echo "Seeding 30 days of demo history into $db..."
  "$py" -m seed.seed_history
}

get_pid() {
  if [ -f "$PID_FILE" ]; then
    cat "$PID_FILE"
  fi
}

is_running() {
  local pid
  pid="$(get_pid || true)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  return 1
}

dashboard_url() {
  echo "http://127.0.0.1:${PORT}"
}

open_app() {
  local url
  url="$(dashboard_url)"
  if command -v open >/dev/null 2>&1; then
    open "$url" >/dev/null 2>&1 || true
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$url" >/dev/null 2>&1 || true
  fi
  echo "Dashboard URL: $url"
}

wait_for_health() {
  local pid="$1"
  local py url i
  py="$(python_bin)"
  url="$(dashboard_url)/api/health"

  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    if "$py" - "$url" >/dev/null 2>&1 <<'PY'; then
import sys
import urllib.request

url = sys.argv[1]
with urllib.request.urlopen(url, timeout=1.0) as response:
    if response.status != 200:
        raise SystemExit(1)
PY
      return 0
    fi
    sleep 0.5
  done
  return 1
}

start_app() {
  if is_running; then
    echo "GC Analyzer is already running on port $PORT (PID: $(get_pid))."
    echo "Dashboard URL: $(dashboard_url)"
    if [ "$OPEN_AFTER" = "1" ]; then
      open_app >/dev/null
    fi
    return 0
  fi

  seed_demo_history "${DB_PATH:-${GC_DB:-gc_history.db}}"

  local py new_pid
  py="$(python_bin)"

  echo "Starting GC Analyzer on ${HOST}:${PORT}..."
  if [ -n "${DB_PATH:-}" ]; then
    export GC_DB="$DB_PATH"
    nohup "$py" -m gcanalyzer.app --host "$HOST" --port "$PORT" --db "$DB_PATH" > "$LOG_FILE" 2>&1 &
  else
    nohup "$py" -m gcanalyzer.app --host "$HOST" --port "$PORT" > "$LOG_FILE" 2>&1 &
  fi
  new_pid=$!
  echo "$new_pid" > "$PID_FILE"

  if ! wait_for_health "$new_pid"; then
    echo "ERROR: GC Analyzer failed to start. Recent logs:"
    if [ -f "$LOG_FILE" ]; then
      tail -n 30 "$LOG_FILE" || true
    else
      echo "Log file was not created: $LOG_FILE"
    fi
    rm -f "$PID_FILE"
    return 1
  fi

  echo "GC Analyzer started (PID: $new_pid)."
  echo "Log file: $LOG_FILE"
  echo "Dashboard URL: $(dashboard_url)"
  if [ "$OPEN_AFTER" = "1" ]; then
    open_app >/dev/null
  fi
}

stop_app() {
  if ! is_running; then
    echo "GC Analyzer is stopped on port $PORT."
    rm -f "$PID_FILE"
    return 0
  fi

  local pid
  pid="$(get_pid)"
  echo "Stopping GC Analyzer on port $PORT (PID: $pid)..."
  kill "$pid" 2>/dev/null || true

  local i
  for i in 1 2 3 4 5; do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done

  if kill -0 "$pid" 2>/dev/null; then
    echo "Process did not exit gracefully; sending SIGKILL."
    kill -9 "$pid" 2>/dev/null || true
  fi

  rm -f "$PID_FILE"
  echo "GC Analyzer stopped."
}

status_app() {
  if is_running; then
    echo "GC Analyzer: RUNNING"
    echo "PID: $(get_pid)"
    echo "Port: $PORT"
    echo "Log file: $LOG_FILE"
    echo "Dashboard URL: $(dashboard_url)"
  else
    echo "GC Analyzer: STOPPED on port $PORT"
  fi
}

logs_app() {
  if [ ! -f "$LOG_FILE" ]; then
    echo "Log file not found: $LOG_FILE"
    return 1
  fi
  echo "Tailing $LOG_FILE"
  tail -n 80 -f "$LOG_FILE"
}

deploy_app() {
  stop_app
  start_app
}

case "$CMD" in
  start)
    start_app
    ;;
  deploy)
    deploy_app
    ;;
  stop)
    stop_app
    ;;
  restart)
    stop_app
    start_app
    ;;
  status)
    status_app
    ;;
  logs)
    logs_app
    ;;
  seed)
    seed_demo_history "${2:-${GC_DB:-gc_history.db}}"
    ;;
  open)
    open_app
    ;;
  setup)
    ensure_venv
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    echo "Unknown command: $CMD"
    usage
    exit 1
    ;;
esac
