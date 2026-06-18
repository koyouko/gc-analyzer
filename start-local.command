#!/bin/bash
# BSP Kafka GC Analyzer — macOS launcher.
# Double-click this file in Finder, OR run:  ./start-local.command [port]
# It creates an isolated Python virtualenv (avoids macOS "externally-managed"
# pip errors), installs deps, seeds demo data once, then serves the dashboard.

set -e
cd "$(dirname "$0")"
PORT="${1:-8083}"

echo "=================================================="
echo " BSP Kafka GC Analyzer — starting on port $PORT"
echo "=================================================="

# 1) Find Python 3
PY="$(command -v python3 || command -v python || true)"
if [ -z "$PY" ]; then
  echo "ERROR: Python 3 not found. Install it from https://www.python.org/downloads/ then retry."
  read -r -p "Press Enter to close..."; exit 1
fi
echo "Using Python: $("$PY" --version 2>&1)"

# 2) Virtualenv (keeps your system Python untouched)
if [ ! -d .venv ]; then
  echo "Creating virtual environment (.venv)..."
  "$PY" -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# 3) Dependencies
echo "Installing dependencies..."
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r requirements.txt

# 4) Seed demo history only if the store has no data yet
NEED_SEED=1
if python - <<'PYCHECK' 2>/dev/null
import sqlite3, os, sys
db = os.environ.get("GC_DB", "gc_history.db")
try:
    n = sqlite3.connect(db).execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
except Exception:
    n = 0
sys.exit(0 if n > 0 else 1)
PYCHECK
then
  NEED_SEED=0
fi
if [ "$NEED_SEED" = "1" ]; then
  echo "Seeding 30 days of demo history (first run only)..."
  python -m seed.seed_history
fi

# 5) Open the browser shortly after the server comes up
( sleep 2; open "http://127.0.0.1:$PORT" >/dev/null 2>&1 || true ) &

echo ""
echo "Dashboard:  http://127.0.0.1:$PORT"
echo "Press Ctrl+C to stop."
echo ""
exec python -m gcanalyzer.app --port "$PORT"
