#!/usr/bin/env bash
set -euo pipefail

min_mib="${LASTDB_SMOKE_MIN_AVAILABLE_MIB:-4096}"
warn_only=0

usage() {
  cat <<'EOF'
Usage: smoke-memory-preflight.sh [--min-mib N] [--warn]

Checks whether enough memory is available to run the LastDB Mini real-data
smoke canary alongside the live primary lastdbd. By default it requires
4096 MiB available and exits non-zero when the host is below that floor.

Environment:
  LASTDB_SMOKE_MIN_AVAILABLE_MIB  Override the default minimum.
  LASTDB_SMOKE_AVAILABLE_KIB      Test override for detected available memory.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --min-mib)
      if [ "$#" -lt 2 ]; then
        echo "missing value for --min-mib" >&2
        exit 64
      fi
      min_mib="$2"
      shift 2
      ;;
    --warn)
      warn_only=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

case "$min_mib" in
  ''|*[!0-9]*)
    echo "LASTDB_SMOKE_MIN_AVAILABLE_MIB/--min-mib must be an integer MiB value" >&2
    exit 64
    ;;
esac

available_kib_from_vm_stat() {
  local pagesize
  pagesize="$(vm_stat 2>/dev/null | awk '/page size of/ { gsub("\\.", "", $8); print $8; exit }')"
  if [ -z "$pagesize" ]; then
    pagesize="$(pagesize 2>/dev/null || true)"
  fi
  if [ -z "$pagesize" ]; then
    return 1
  fi

  vm_stat 2>/dev/null | awk -v pagesize="$pagesize" '
    /Pages free:/ { gsub("\\.", "", $3); free=$3 }
    /Pages inactive:/ { gsub("\\.", "", $3); inactive=$3 }
    /Pages speculative:/ { gsub("\\.", "", $3); speculative=$3 }
    END {
      pages = free + inactive + speculative
      if (pages <= 0) exit 1
      printf "%.0f\n", pages * pagesize / 1024
    }
  '
}

available_kib_from_meminfo() {
  awk '/^MemAvailable:/ { print $2; found=1; exit } END { if (!found) exit 1 }' /proc/meminfo 2>/dev/null
}

available_kib="${LASTDB_SMOKE_AVAILABLE_KIB:-}"
if [ -z "$available_kib" ]; then
  available_kib="$(available_kib_from_meminfo || available_kib_from_vm_stat || true)"
fi

case "$available_kib" in
  ''|*[!0-9]*)
    echo "LASTDB_SMOKE_MEMORY_PRECHECK=unknown reason=available-memory-unavailable min_mib=$min_mib" >&2
    if [ "$warn_only" -eq 1 ]; then
      exit 0
    fi
    exit 69
    ;;
esac

available_mib=$((available_kib / 1024))

if [ "$available_mib" -lt "$min_mib" ]; then
  echo "LASTDB_SMOKE_MEMORY_PRECHECK=insufficient available_mib=$available_mib min_mib=$min_mib"
  if [ "$warn_only" -eq 1 ]; then
    exit 0
  fi
  exit 75
fi

echo "LASTDB_SMOKE_MEMORY_PRECHECK=ok available_mib=$available_mib min_mib=$min_mib"
