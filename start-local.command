#!/usr/bin/env bash
# macOS double-click launcher. The single control script is manage-app.sh.
# Usage:
#   ./start-local.command [port]

set -euo pipefail
cd "$(dirname "$0")"

PORT="${1:-${GC_PORT:-8083}}"

exec ./manage-app.sh start "$PORT" --open
