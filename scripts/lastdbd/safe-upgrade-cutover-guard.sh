#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  safe-upgrade-cutover-guard.sh install-sidebin --candidate PATH [options]
  safe-upgrade-cutover-guard.sh assert-sessions --sessions PATH --since EPOCH [options]

Subcommands:
  install-sidebin
    Serializes a LastDB sidebin cutover against a launchd KeepAlive supervisor:
    disable + bootout the LaunchAgent first, swap binaries while it is unloaded,
    then bootstrap/enable/kickstart exactly one supervised instance. This helper
    intentionally never falls back to a direct nohup lastdbd start.

  assert-sessions
    Checks sessions.jsonl for the cutover window and fails unless exactly one
    boot record exists and last_boot_error.txt has no database-lock failure.

Options:
  --candidate PATH        Candidate lastdbd binary for install-sidebin
  --candidate-cli PATH    Candidate lastdb CLI (required; defaults to the
                          sibling "lastdb" beside --candidate). install-sidebin
                          refuses to cut over unless this CLI is present and
                          its --version agrees with the candidate lastdbd —
                          promotion is a matched lastdb+lastdbd pair, never
                          the daemon alone.
  --sidebin-dir DIR       Destination dir (default: ~/.lastdb/bin-with-upload-cap)
  --label LABEL           LaunchAgent label (default: com.tomtang.lastdbd-primary-506)
  --legacy-watchdog-label LABEL
                         Legacy watchdog label to retire before cutover
                         (default: com.folddb.watchdog; use "none" to skip)
  --plist PATH            LaunchAgent plist path
  --primary-home PATH     Primary LastDB home (default: ~/.lastdb)
  --version VERSION       Version string used in backup names
  --backup-retention N    Backup files to retain per binary (default: 5)
  --min-free-mib MIB      Required free disk after planned copies (default: 512)
  --launchctl PATH        launchctl path or test stub
  --sessions PATH         sessions.jsonl path for assert-sessions
  --since EPOCH           Inclusive start_ts lower bound
  --until EPOCH           Inclusive start_ts upper bound (default: now)
  --last-boot-error PATH  last_boot_error.txt path (default: dirname(sessions)/last_boot_error.txt)
EOF
}

die() {
  echo "safe-upgrade-cutover-guard: $*" >&2
  exit 64
}

warn() {
  echo "safe-upgrade-cutover-guard: WARN: $*" >&2
}

cmd="${1:-}"
[ -n "$cmd" ] || { usage >&2; exit 64; }
shift

label="${LASTDB_LAUNCHD_LABEL:-com.tomtang.lastdbd-primary-506}"
legacy_watchdog_label="${LASTDB_LEGACY_WATCHDOG_LABEL:-com.folddb.watchdog}"
primary_home="${LASTDB_HOME:-$HOME/.lastdb}"
sidebin_dir="${LASTDB_SIDEBIN_DIR:-$HOME/.lastdb/bin-with-upload-cap}"
plist="${LASTDB_LAUNCHD_PLIST:-}"
candidate=""
candidate_cli=""
version=""
backup_retention="${LASTDB_UPGRADE_BACKUP_RETENTION:-5}"
min_free_mib="${LASTDB_UPGRADE_MIN_FREE_MIB:-512}"
launchctl_bin="${LAUNCHCTL_BIN:-launchctl}"
sessions=""
since=""
until="$(date +%s)"
last_boot_error=""
restart_intent_path=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --candidate) [ "$#" -ge 2 ] || die "missing value for --candidate"; candidate="$2"; shift 2 ;;
    --candidate-cli) [ "$#" -ge 2 ] || die "missing value for --candidate-cli"; candidate_cli="$2"; shift 2 ;;
    --sidebin-dir) [ "$#" -ge 2 ] || die "missing value for --sidebin-dir"; sidebin_dir="$2"; shift 2 ;;
    --label) [ "$#" -ge 2 ] || die "missing value for --label"; label="$2"; shift 2 ;;
    --legacy-watchdog-label) [ "$#" -ge 2 ] || die "missing value for --legacy-watchdog-label"; legacy_watchdog_label="$2"; shift 2 ;;
    --plist) [ "$#" -ge 2 ] || die "missing value for --plist"; plist="$2"; shift 2 ;;
    --primary-home) [ "$#" -ge 2 ] || die "missing value for --primary-home"; primary_home="$2"; shift 2 ;;
    --version) [ "$#" -ge 2 ] || die "missing value for --version"; version="$2"; shift 2 ;;
    --backup-retention) [ "$#" -ge 2 ] || die "missing value for --backup-retention"; backup_retention="$2"; shift 2 ;;
    --min-free-mib) [ "$#" -ge 2 ] || die "missing value for --min-free-mib"; min_free_mib="$2"; shift 2 ;;
    --launchctl) [ "$#" -ge 2 ] || die "missing value for --launchctl"; launchctl_bin="$2"; shift 2 ;;
    --sessions) [ "$#" -ge 2 ] || die "missing value for --sessions"; sessions="$2"; shift 2 ;;
    --since) [ "$#" -ge 2 ] || die "missing value for --since"; since="$2"; shift 2 ;;
    --until) [ "$#" -ge 2 ] || die "missing value for --until"; until="$2"; shift 2 ;;
    --last-boot-error) [ "$#" -ge 2 ] || die "missing value for --last-boot-error"; last_boot_error="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

target_domain() {
  printf 'gui/%s' "$(id -u)"
}

target_service() {
  printf '%s/%s' "$(target_domain)" "$label"
}

write_restart_intent() {
  local cause="$1"
  local current_session="$primary_home/current-session.json"
  if [ ! -s "$current_session" ]; then
    echo "CUTOVER_RESTART_INTENT=not-needed reason=no-current-session path=$current_session"
    return 0
  fi
  local previous_pid
  previous_pid="$(jq -r 'if (.pid | type) == "number" and .pid > 0 then .pid else empty end' "$current_session")"
  [ -n "$previous_pid" ] || die "current session has no valid pid: $current_session"
  restart_intent_path="$primary_home/restart-intent.json"
  local intent_tmp="${restart_intent_path}.tmp.$$"
  mkdir -p "$primary_home"
  (
    umask 077
    printf '{"cause":"%s","previous_pid":%s,"created_at":%s}\n' \
      "$cause" "$previous_pid" "$(date +%s)" >"$intent_tmp"
    mv -f "$intent_tmp" "$restart_intent_path"
  )
  echo "CUTOVER_RESTART_INTENT=ok cause=$cause previous_pid=$previous_pid path=$restart_intent_path"
}

retire_legacy_watchdog() {
  [ -n "$legacy_watchdog_label" ] || return 0
  [ "$legacy_watchdog_label" = "none" ] && return 0
  legacy_service="$(target_domain)/$legacy_watchdog_label"
  echo "CUTOVER_LEGACY_WATCHDOG=disable service=$legacy_service"
  run_launchctl disable "$legacy_service" 2>/dev/null || true
  echo "CUTOVER_LEGACY_WATCHDOG=bootout service=$legacy_service"
  run_launchctl bootout "$legacy_service" 2>/dev/null || true
}

run_launchctl() {
  "$launchctl_bin" "$@"
}

mtime_epoch() {
  stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null || echo 0
}

require_positive_integer() {
  name="$1"
  value="$2"
  case "$value" in
    ''|*[!0-9]*) die "$name must be a positive integer: $value" ;;
  esac
  [ "$value" -gt 0 ] || die "$name must be greater than zero: $value"
}

require_nonnegative_integer() {
  name="$1"
  value="$2"
  case "$value" in
    ''|*[!0-9]*) die "$name must be a non-negative integer: $value" ;;
  esac
}

du_kib() {
  [ -e "$1" ] || {
    echo 0
    return 0
  }
  du -sk "$1" | awk '{print $1}'
}

available_kib_for() {
  dir="$1"
  if [ -n "${LASTDB_UPGRADE_AVAILABLE_KIB:-}" ]; then
    require_nonnegative_integer LASTDB_UPGRADE_AVAILABLE_KIB "$LASTDB_UPGRADE_AVAILABLE_KIB"
    echo "$LASTDB_UPGRADE_AVAILABLE_KIB"
    return 0
  fi
  df -Pk "$dir" | awk 'NR == 2 {print $4}'
}

planned_copy_kib() {
  total=0
  total=$(( total + $(du_kib "$candidate") ))
  if [ -x "$sidebin_dir/lastdbd" ]; then
    total=$(( total + $(du_kib "$sidebin_dir/lastdbd") ))
  fi
  if [ -x "$candidate_cli" ]; then
    total=$(( total + $(du_kib "$candidate_cli") ))
    if [ -x "$sidebin_dir/lastdb" ]; then
      total=$(( total + $(du_kib "$sidebin_dir/lastdb") ))
    fi
  fi
  echo "$total"
}

assert_disk_headroom() {
  require_nonnegative_integer "--min-free-mib" "$min_free_mib"
  available_kib="$(available_kib_for "$sidebin_dir")"
  planned_kib="$(planned_copy_kib)"
  min_free_kib=$(( min_free_mib * 1024 ))
  required_kib=$(( planned_kib + min_free_kib ))
  available_mib=$(( available_kib / 1024 ))
  planned_mib=$(( (planned_kib + 1023) / 1024 ))
  required_mib=$(( (required_kib + 1023) / 1024 ))
  if [ "$available_kib" -lt "$required_kib" ]; then
    echo "CUTOVER_DISK_HEADROOM=insufficient path=$sidebin_dir available_mib=$available_mib planned_copy_mib=$planned_mib min_free_mib=$min_free_mib required_mib=$required_mib" >&2
    exit 66
  fi
  echo "CUTOVER_DISK_HEADROOM=ok path=$sidebin_dir available_mib=$available_mib planned_copy_mib=$planned_mib min_free_mib=$min_free_mib required_mib=$required_mib"
}

prune_backup_files() {
  binary_name="$1"
  keep="$2"
  [ -d "$sidebin_dir" ] || return 0
  find "$sidebin_dir" -type f -name "${binary_name}.bak-pre-*" -print \
    | while IFS= read -r path; do
        printf '%s\t%s\n' "$(mtime_epoch "$path")" "$path"
      done \
    | sort -rn \
    | awk -v keep="$keep" 'NR > keep { sub(/^[^\t]*\t/, ""); print }' \
    | while IFS= read -r old_backup; do
        [ -n "$old_backup" ] || continue
        rm -f "$old_backup"
        echo "CUTOVER_BACKUP_PRUNED path=$old_backup"
      done
  echo "CUTOVER_BACKUP_RETENTION=ok name=$binary_name keep=$keep"
}

reserve_backup_slot() {
  binary_name="$1"
  keep_before=$(( backup_retention - 1 ))
  prune_backup_files "$binary_name" "$keep_before"
}

install_sidebin() {
  [ -x "$candidate" ] || die "candidate is required and must be executable: ${candidate:-}"
  if [ -z "$candidate_cli" ]; then
    candidate_cli="$(dirname "$candidate")/lastdb"
  fi
  [ -x "$candidate_cli" ] || die "candidate lastdb CLI is required and must be executable (paired-bundle promotion): ${candidate_cli:-}"
  require_positive_integer "--backup-retention" "$backup_retention"
  if [ -z "$plist" ]; then
    plist="$HOME/Library/LaunchAgents/${label}.plist"
  fi
  [ -f "$plist" ] || die "LaunchAgent plist is required before restarting supervisor: $plist"
  if [ -z "$version" ]; then
    version="$("$candidate" --version 2>/dev/null | awk '{print $NF}' || true)"
  fi
  [ -n "$version" ] || version="unknown"

  cli_version="$("$candidate_cli" --version 2>/dev/null | awk '{print $NF}' || true)"
  [ -n "$cli_version" ] || die "candidate lastdb CLI did not report a --version: $candidate_cli"
  if [ "$version" != "unknown" ] && [ "$cli_version" != "$version" ]; then
    die "paired-bundle version skew: candidate lastdbd=$version candidate lastdb=$cli_version (refusing cutover; promote a matched lastdb+lastdbd bundle)"
  fi

  mkdir -p "$sidebin_dir"
  reserve_backup_slot lastdbd
  if [ -x "$candidate_cli" ]; then
    reserve_backup_slot lastdb
  fi
  assert_disk_headroom
  lock="$sidebin_dir/.cutover.lock"
  if [ -f "$lock" ]; then
    age=$(( $(date +%s) - $(mtime_epoch "$lock") ))
    [ "$age" -ge 600 ] || die "cutover lock present ($lock, age ${age}s)"
    warn "removing stale cutover lock ($lock, age ${age}s)"
    rm -f "$lock"
  fi
  printf '%s %s %s\n' "$$" "$version" "$(date -u +%Y%m%dT%H%M%SZ)" >"$lock"
  trap 'rm -f "$lock"; [ -z "${restart_intent_path:-}" ] || rm -f "$restart_intent_path"' EXIT

  service="$(target_service)"
  domain="$(target_domain)"
  write_restart_intent upgrade
  retire_legacy_watchdog
  echo "CUTOVER_SUPERVISOR=disable service=$service"
  run_launchctl disable "$service" 2>/dev/null || warn "launchctl disable failed for $service"
  echo "CUTOVER_SUPERVISOR=bootout service=$service"
  run_launchctl bootout "$service" 2>/dev/null || true

  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  if [ -x "$sidebin_dir/lastdbd" ]; then
    cp -a "$sidebin_dir/lastdbd" "$sidebin_dir/lastdbd.bak-pre-${version}-${ts}"
  fi
  cp -a "$candidate" "$sidebin_dir/lastdbd.new"
  chmod +x "$sidebin_dir/lastdbd.new"
  mv -f "$sidebin_dir/lastdbd.new" "$sidebin_dir/lastdbd"

  if [ -x "$sidebin_dir/lastdb" ]; then
    cp -a "$sidebin_dir/lastdb" "$sidebin_dir/lastdb.bak-pre-${version}-${ts}" 2>/dev/null || true
  fi
  cp -a "$candidate_cli" "$sidebin_dir/lastdb.new"
  chmod +x "$sidebin_dir/lastdb.new"
  mv -f "$sidebin_dir/lastdb.new" "$sidebin_dir/lastdb"
  prune_backup_files lastdbd "$backup_retention"
  prune_backup_files lastdb "$backup_retention"
  echo "CUTOVER_INSTALL=ok dest=$sidebin_dir/lastdbd"

  echo "CUTOVER_SUPERVISOR=bootstrap domain=$domain plist=$plist"
  if ! run_launchctl bootstrap "$domain" "$plist" 2>/dev/null; then
    run_launchctl print "$service" >/dev/null 2>&1 || die "launchctl bootstrap failed and service is not loaded: $service"
  fi
  echo "CUTOVER_SUPERVISOR=enable service=$service"
  run_launchctl enable "$service" 2>/dev/null || warn "launchctl enable failed for $service"
  echo "CUTOVER_SUPERVISOR=kickstart service=$service"
  run_launchctl kickstart -k "$service"

  rm -f "$lock"
  trap - EXIT
  echo "CUTOVER_GUARD=ok service=$service primary_home=$primary_home"
}

assert_sessions() {
  [ -n "$sessions" ] || die "--sessions is required"
  [ -n "$since" ] || die "--since is required"
  [ -f "$sessions" ] || die "sessions file does not exist: $sessions"
  if [ -z "$last_boot_error" ]; then
    last_boot_error="$(dirname "$sessions")/last_boot_error.txt"
  fi

  count="$(jq -r --argjson since "$since" --argjson until "$until" '
    select(type == "object")
    | select((.start_ts // 0) >= $since and (.start_ts // 0) <= $until)
    | .pid
  ' "$sessions" | wc -l | tr -d '[:space:]')"

  if [ "$count" -ne 1 ]; then
    echo "CUTOVER_SESSION_ASSERT=failed expected=1 actual=$count sessions=$sessions since=$since until=$until" >&2
    exit 65
  fi

  if [ -s "$last_boot_error" ] && grep -Eiq 'database is locked|lock-wait|another process' "$last_boot_error"; then
    echo "CUTOVER_SESSION_ASSERT=failed last_boot_error=$last_boot_error" >&2
    exit 65
  fi

  echo "CUTOVER_SESSION_ASSERT=ok boots=$count sessions=$sessions since=$since until=$until"
}

case "$cmd" in
  install-sidebin) install_sidebin ;;
  assert-sessions) assert_sessions ;;
  *) die "unknown subcommand: $cmd" ;;
esac
