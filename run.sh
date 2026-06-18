#!/usr/bin/env bash
# Kafka Fleet GC Analyzer launcher.
#   ./run.sh            -> seed demo history (if missing) + serve http://127.0.0.1:8000
#   ./run.sh 0.0.0.0 9000
set -euo pipefail
cd "$(dirname "$0")"

HOST="${1:-127.0.0.1}"
PORT="${2:-8000}"

python3 -m pip install -q -r requirements.txt || true

# Seed unless the history store already has metric rows.
HAS_DATA="$(python3 - <<'PY'
import sqlite3, os
p = os.environ.get("GC_DB", "gc_history.db")
try:
    n = sqlite3.connect(p).execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
    print(1 if n > 0 else 0)
except Exception:
    print(0)
PY
)"
if [[ "$HAS_DATA" != "1" ]]; then
  echo "Seeding 30 days of demo history (one-time)…"
  python3 -m seed.seed_history
fi

echo "Dashboard: http://$HOST:$PORT"
python3 -m gcanalyzer.app --host "$HOST" --port "$PORT"
