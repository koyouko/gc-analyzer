#!/usr/bin/env bash
# Compatibility launcher. The single control script is manage-app.sh.
# Old usage still works:
#   ./run.sh
#   ./run.sh 0.0.0.0 9000

set -euo pipefail
cd "$(dirname "$0")"

export GC_HOST="${1:-${GC_HOST:-127.0.0.1}}"
PORT="${2:-${GC_PORT:-8000}}"

exec ./manage-app.sh start "$PORT"
