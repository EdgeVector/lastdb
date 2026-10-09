#!/usr/bin/env bash
set -euo pipefail

name="bob"
label=""
home_dir=""
dsn_locator="${LASTDBD_SECONDARY_OBS_SENTRY_DSN_LOCATOR:-lastsecrets://obs-sentry-dsn-rust}"
environment=""
install_id=""
lastdbd_bin="${LASTDBD_BIN:-}"
lastsecrets_bin="${LASTSECRETS_BIN:-}"
plist_dir="${LASTDBD_LAUNCH_AGENT_DIR:-$HOME/Library/LaunchAgents}"
support_dir="${LASTDBD_LAUNCH_SUPPORT_DIR:-$HOME/.config/lastdb/launchd}"
log_dir="${LASTDBD_LAUNCH_LOG_DIR:-$HOME/Library/Logs/lastdb}"
load=1
print_paths=0

usage() {
  cat <<'EOF'
Usage: install-secondary-launch-agent.sh [options]

Installs a secret-free LaunchAgent for a secondary LastDB Mini node. The
generated wrapper resolves OBS_SENTRY_DSN from a lastsecrets:// locator at
process start, then execs lastdbd with OBS_SENTRY_ENVIRONMENT set to the node
name (bob by default).

Options:
  --name NAME              Secondary node name (default: bob)
  --label LABEL            LaunchAgent label (default: com.edgevector.lastdbd-NAME)
  --home PATH              LastDB home (default: ~/.lastdb-NAME)
  --dsn-locator LOCATOR    LastSecrets locator (default: lastsecrets://obs-sentry-dsn-rust)
  --environment NAME       OBS_SENTRY_ENVIRONMENT (default: --name)
  --install-id ID          OBS_SENTRY_INSTALL_ID (default: --name)
  --bin PATH               lastdbd binary (default: command -v lastdbd, then /opt/homebrew/bin/lastdbd)
  --lastsecrets-bin PATH   lastsecrets binary (default: command -v lastsecrets, then ~/.bun/bin/lastsecrets)
  --plist-dir DIR          LaunchAgents directory (default: ~/Library/LaunchAgents)
  --support-dir DIR        Wrapper directory (default: ~/.config/lastdb/launchd)
  --log-dir DIR            Log directory (default: ~/Library/Logs/lastdb)
  --no-load                Write files but do not bootstrap/kickstart launchd
  --print-paths            Print generated plist and wrapper paths
EOF
}

die() {
  echo "install-secondary-launch-agent: $*" >&2
  exit 64
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --name)
      [ "$#" -ge 2 ] || die "missing value for --name"
      name="$2"
      shift 2
      ;;
    --label)
      [ "$#" -ge 2 ] || die "missing value for --label"
      label="$2"
      shift 2
      ;;
    --home)
      [ "$#" -ge 2 ] || die "missing value for --home"
      home_dir="$2"
      shift 2
      ;;
    --dsn-locator)
      [ "$#" -ge 2 ] || die "missing value for --dsn-locator"
      dsn_locator="$2"
      shift 2
      ;;
    --environment)
      [ "$#" -ge 2 ] || die "missing value for --environment"
      environment="$2"
      shift 2
      ;;
    --install-id)
      [ "$#" -ge 2 ] || die "missing value for --install-id"
      install_id="$2"
      shift 2
      ;;
    --bin)
      [ "$#" -ge 2 ] || die "missing value for --bin"
      lastdbd_bin="$2"
      shift 2
      ;;
    --lastsecrets-bin)
      [ "$#" -ge 2 ] || die "missing value for --lastsecrets-bin"
      lastsecrets_bin="$2"
      shift 2
      ;;
    --plist-dir)
      [ "$#" -ge 2 ] || die "missing value for --plist-dir"
      plist_dir="$2"
      shift 2
      ;;
    --support-dir)
      [ "$#" -ge 2 ] || die "missing value for --support-dir"
      support_dir="$2"
      shift 2
      ;;
    --log-dir)
      [ "$#" -ge 2 ] || die "missing value for --log-dir"
      log_dir="$2"
      shift 2
      ;;
    --no-load)
      load=0
      shift
      ;;
    --print-paths)
      print_paths=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "$name" in
  ''|*[!A-Za-z0-9._-]*)
    die "--name must contain only letters, numbers, '.', '_', or '-'"
    ;;
esac

if [ -z "$label" ]; then
  label="com.edgevector.lastdbd-$name"
fi
case "$label" in
  ''|*[!A-Za-z0-9._-]*)
    die "--label must contain only letters, numbers, '.', '_', or '-'"
    ;;
esac

case "$dsn_locator" in
  lastsecrets://?*) ;;
  *) die "--dsn-locator must be a lastsecrets:// locator" ;;
esac

if [ -z "$home_dir" ]; then
  home_dir="$HOME/.lastdb-$name"
fi
if [ -z "$environment" ]; then
  environment="$name"
fi
if [ -z "$install_id" ]; then
  install_id="$name"
fi
if [ -z "$lastdbd_bin" ]; then
  lastdbd_bin="$(command -v lastdbd 2>/dev/null || true)"
fi
if [ -z "$lastdbd_bin" ]; then
  lastdbd_bin="/opt/homebrew/bin/lastdbd"
fi
if [ -z "$lastsecrets_bin" ]; then
  lastsecrets_bin="$(command -v lastsecrets 2>/dev/null || true)"
fi
if [ -z "$lastsecrets_bin" ]; then
  lastsecrets_bin="$HOME/.bun/bin/lastsecrets"
fi

case "$home_dir" in
  "$HOME/.lastdb"|"$HOME/.folddb")
    die "refusing to configure a secondary node on the primary home: $home_dir"
    ;;
esac

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

xml_escape() {
  printf '%s' "$1" \
    | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g'
}

mkdir -p "$home_dir" "$plist_dir" "$support_dir" "$log_dir"

wrapper="$support_dir/$label-env.sh"
plist="$plist_dir/$label.plist"
stdout_log="$log_dir/lastdbd-$name.log"
stderr_log="$log_dir/lastdbd-$name.err.log"
dsn_slug="${dsn_locator#lastsecrets://}"

cat > "$wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail

dsn="\$($(shell_quote "$lastsecrets_bin") get $(shell_quote "$dsn_slug"))"
if [ -z "\$dsn" ]; then
  echo "lastsecrets locator $(shell_quote "$dsn_locator") returned an empty OBS_SENTRY_DSN" >&2
  exit 70
fi

export OBS_SENTRY_DSN="\$dsn"
export OBS_SENTRY_ENVIRONMENT=$(shell_quote "$environment")
export OBS_SENTRY_INSTALL_ID=$(shell_quote "$install_id")
export LASTDB_HOME=$(shell_quote "$home_dir")
export FOLDDB_HOME="\$LASTDB_HOME"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:\$PATH"

exec $(shell_quote "$lastdbd_bin") --data-dir "\$LASTDB_HOME"
EOF
chmod 700 "$wrapper"

cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$(xml_escape "$label")</string>
  <key>ProgramArguments</key>
  <array>
    <string>$(xml_escape "$wrapper")</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <!-- A graceful stop drains persist lanes, stops sync, and flushes. The
       launchd default gave the primary 5 s before SIGKILL on 2026-09-24,
       which cut the drain short. Keep this above the daemon's worst case. -->
  <key>ExitTimeOut</key>
  <integer>150</integer>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOME</key>
    <string>$(xml_escape "$HOME")</string>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key>
  <string>$(xml_escape "$stdout_log")</string>
  <key>StandardErrorPath</key>
  <string>$(xml_escape "$stderr_log")</string>
</dict>
</plist>
EOF
chmod 644 "$plist"

if command -v plutil >/dev/null 2>&1; then
  plutil -lint "$plist" >/dev/null
fi

if [ "$print_paths" -eq 1 ]; then
  printf 'label=%s\nplist=%s\nwrapper=%s\nhome=%s\n' "$label" "$plist" "$wrapper" "$home_dir"
fi

if [ "$load" -eq 1 ]; then
  if ! command -v launchctl >/dev/null 2>&1; then
    die "launchctl is required unless --no-load is passed"
  fi
  uid="$(id -u)"
  launchctl bootout "gui/$uid/$label" 2>/dev/null || true
  launchctl bootstrap "gui/$uid" "$plist"
  launchctl enable "gui/$uid/$label" 2>/dev/null || true
  launchctl kickstart -k "gui/$uid/$label"
fi
