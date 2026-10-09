#!/usr/bin/env bash
# Owner-side hourly fire for admin Kanban last-day done deliver.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
exec python3 "$ROOT/scripts/admin-kanban-hourly-deliver/deliver.py" "$@"
