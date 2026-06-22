#!/bin/bash
# BSP Kafka GC Analyzer — macOS launcher.
# Double-click in Finder, OR run:  ./start-local.command [port]
# Picks the NEWEST Python 3.10+ on your machine (so it won't default to an old
# python3), builds an isolated virtualenv, installs deps, seeds demo data once,
# then serves the dashboard.
#
# Force a specific interpreter:   PYTHON=python3.14 ./start-local.command
# Use a different port:           ./start-local.command 9000

set -e
cd "$(dirname "$0")"
PORT="${1:-8083}"

echo "=================================================="
echo " BSP Kafka GC Analyzer — starting on port $PORT"
echo "=================================================="

# 1) Pick the newest Python >= 3.10 (don't just trust `python3`).
pick_python() {
  if [ -n "$PYTHON" ] && command -v "$PYTHON" >/dev/null 2>&1; then echo "$PYTHON"; return 0; fi
  for c in python3.14 python3.13 python3.12 python3.11 python3.10 python3 python; do
    if command -v "$c" >/dev/null 2>&1; then
      v="$("$c" -c 'import sys;print(sys.version_info[0]*100+sys.version_info[1])' 2>/dev/null || echo 0)"
      if [ "$v" -ge 310 ]; then echo "$c"; return 0; fi
    fi
  done
  return 1
}
PY="$(pick_python || true)"
if [ -z "$PY" ]; then
  echo "ERROR: No Python 3.10+ found. Detected interpreters:"
  for c in python3.14 python3.13 python3.12 python3.11 python3.10 python3; do
    command -v "$c" >/dev/null 2>&1 && echo "  - $c  ($($c --version 2>&1))"
  done
  echo "Install 3.10+ or force one, e.g.:  PYTHON=python3.14 ./start-local.command"
  read -r -p "Press Enter to close..."; exit 1
fi
echo "Using Python: $("$PY" --version 2>&1)   ($PY)"

# 2) Virtualenv. Recreate if an existing one is older than 3.10.
if [ -d .venv ]; then
  VV="$(.venv/bin/python -c 'import sys;print(sys.version_info[0]*100+sys.version_info[1])' 2>/dev/null || echo 0)"
  if [ "$VV" -lt 310 ]; then
    echo "Existing .venv is Python < 3.10 — recreating it..."
    rm -rf .venv
  fi
fi
if [ ! -d .venv ]; then
  echo "Creating virtual environment (.venv)..."
  "$PY" -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
echo "venv Python: $(python --version 2>&1)"

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
