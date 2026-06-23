#!/bin/bash
# BSP Kafka GC Analyzer — Application Control Script.
#
# Usage:
#   ./manage-app.sh start [port] [db_path]
#   ./manage-app.sh stop [port]
#   ./manage-app.sh status [port]
#   ./manage-app.sh restart [port] [db_path]
#   ./manage-app.sh logs [port]
#
# Examples:
#   ./manage-app.sh start 8083
#   ./manage-app.sh start 8090 gc_live.db
#   ./manage-app.sh status 8083
#   ./manage-app.sh stop 8090

set -e
cd "$(dirname "$0")"

load_env() {
  if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
  fi
}

CMD="${1}"
PORT="${2:-8083}"
DB_PATH="${3}"

PID_FILE="gcanalyzer-${PORT}.pid"
LOG_FILE="gcanalyzer-${PORT}.log"

# Resolve Python interpreter
if [ -d .venv ]; then
  PY=".venv/bin/python"
else
  PY="$(command -v python3 || command -v python || true)"
fi

if [ -z "$PY" ]; then
  echo "ERROR: Python interpreter not found. Please run ./start-local.command once first to set up the environment."
  exit 1
fi

get_pid() {
  if [ -f "$PID_FILE" ]; then
    cat "$PID_FILE"
  else
    echo ""
  fi
}

is_running() {
  local pid
  pid="$(get_pid)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "1"
  else
    echo "0"
  fi
}

start_app() {
  if [ "$(is_running)" = "1" ]; then
    local pid
    pid="$(get_pid)"
    echo "GC Analyzer is ALREADY running on port $PORT (PID: $pid)."
    return 0
  fi

  load_env

  echo "Starting GC Analyzer on port $PORT..."
  
  # Construct execution arguments
  local args=("--port" "$PORT")
  if [ -n "$DB_PATH" ]; then
    args+=("--db" "$DB_PATH")
  fi

  # Run in background and redirect logs
  nohup "$PY" -m gcanalyzer.app "${args[@]}" > "$LOG_FILE" 2>&1 &
  local new_pid=$!
  
  # Save PID
  echo "$new_pid" > "$PID_FILE"
  
  # Quick verification check
  sleep 1.5
  if kill -0 "$new_pid" 2>/dev/null; then
    echo "GC Analyzer started successfully (PID: $new_pid)."
    echo "Uvicorn logs: $LOG_FILE"
    echo "Dashboard URL: http://127.0.0.1:$PORT"
  else
    echo "ERROR: Process failed to start. Check logs at $LOG_FILE for details:"
    tail -n 10 "$LOG_FILE"
    rm -f "$PID_FILE"
    return 1
  fi
}

stop_app() {
  if [ "$(is_running)" = "0" ]; then
    echo "No active GC Analyzer found running on port $PORT."
    rm -f "$PID_FILE"
    return 0
  fi

  local pid
  pid="$(get_pid)"
  echo "Stopping GC Analyzer (PID: $pid) on port $PORT..."
  kill "$pid"

  # Graceful wait
  for i in {1..5}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done

  # Force kill if still active
  if kill -0 "$pid" 2>/dev/null; then
    echo "Process did not exit gracefully, sending SIGKILL..."
    kill -9 "$pid"
  fi

  echo "GC Analyzer stopped."
  rm -f "$PID_FILE"
}

check_status() {
  if [ "$(is_running)" = "1" ]; then
    local pid
    pid="$(get_pid)"
    echo "GC Analyzer: RUNNING on port $PORT (PID: $pid)."
    echo "Dashboard:   http://127.0.0.1:$PORT"
  else
    echo "GC Analyzer: STOPPED on port $PORT."
  fi
}

tail_logs() {
  if [ ! -f "$LOG_FILE" ]; then
    echo "Log file $LOG_FILE not found."
    return 1
  fi
  echo "Tailing logs for port $PORT ($LOG_FILE):"
  tail -n 50 -f "$LOG_FILE"
}

case "$CMD" in
  start)
    start_app
    ;;
  stop)
    stop_app
    ;;
  status)
    check_status
    ;;
  restart)
    stop_app
    start_app
    ;;
  logs)
    tail_logs
    ;;
  *)
    echo "Usage: $0 {start|stop|status|restart|logs} [port] [db_path]"
    exit 1
    ;;
esac
