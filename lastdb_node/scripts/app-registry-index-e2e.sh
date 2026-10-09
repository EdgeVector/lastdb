#!/usr/bin/env bash
# Hermetic end-to-end for the static signed app index:
#   keygen → write index → sign → verify → tamper is refused → resolve picks
#   the row proved with the named node build → install checks out that commit
#   → upgrade is a no-op on the same commit and reinstalls on a new one.
#
# Needs a built `lastdb` (LASTDB_BIN, default target/debug/lastdb). Touches
# nothing outside a temp directory: the trust key is a throwaway key passed
# with --trust-key, so the pinned release key is never involved.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LASTDB_BIN="${LASTDB_BIN:-$ROOT/target/debug/lastdb}"
[ -x "$LASTDB_BIN" ] || { echo "lastdb binary not found at $LASTDB_BIN (cargo build -p lastdb_node --bin lastdb)" >&2; exit 2; }

work="$(mktemp -d "${TMPDIR:-/tmp}/app-registry-index-e2e.XXXXXX")"
trap 'rm -rf "$work"' EXIT
export HOME="$work/home"
mkdir -p "$HOME" "$work/idx"
export LASTDB_HOME="$work/lastdb-home"
mkdir -p "$LASTDB_HOME/data"

step() { printf '\n== %s ==\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

step "keygen"
"$LASTDB_BIN" app dev-init --key-file "$work/sign.key" >"$work/keygen.out"
pub="$(awk '/dev_pubkey:/ {print $2}' "$work/keygen.out")"
[ -n "$pub" ] || fail "no pubkey printed"

step "demo app repo with two commits"
repo="$work/demo.git"
git init --quiet -b main "$work/demo-src"
git -C "$work/demo-src" -c user.name=t -c user.email=t@example.com commit --quiet --allow-empty -m one
first="$(git -C "$work/demo-src" rev-parse HEAD)"
echo two >"$work/demo-src/two.txt"
git -C "$work/demo-src" add two.txt
git -C "$work/demo-src" -c user.name=t -c user.email=t@example.com commit --quiet -m two
second="$(git -C "$work/demo-src" rev-parse HEAD)"
git clone --quiet --bare "$work/demo-src" "$repo"

step "index with two proved pairs for two node builds"
cat >"$work/idx/stable.json" <<EOF
{
  "index_version": 1,
  "channel": "stable",
  "generated_at": "2026-09-20T00:00:00Z",
  "apps": [
    {
      "app_id": "demo",
      "source": "$repo",
      "description": "e2e demo",
      "compat": [
        {"app_version": "1.0.0", "sha": "$first", "lastdb_version": "0.23.3-100-gaaaaaaaaa", "proved_at": "2026-09-19T00:00:00Z", "proof_run": "e2e-1"},
        {"app_version": "1.1.0", "sha": "$second", "lastdb_version": "0.23.3-200-gbbbbbbbbb", "proved_at": "2026-09-20T00:00:00Z", "proof_run": "e2e-2"}
      ]
    }
  ]
}
EOF

step "sign + verify"
"$LASTDB_BIN" app index sign --index "$work/idx/stable.json" --key-file "$work/sign.key"
"$LASTDB_BIN" app index verify --index "$work/idx/stable.json" --trust-key "$pub"

step "tamper is refused"
cp "$work/idx/stable.json" "$work/idx/stable.json.orig"
sed -i.bak 's/1\.1\.0/9.9.9/' "$work/idx/stable.json"
if "$LASTDB_BIN" app index verify --index "$work/idx/stable.json" --trust-key "$pub" 2>/dev/null; then
  fail "tampered index verified"
fi
if "$LASTDB_BIN" app resolve demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.23.3-100-gaaaaaaaaa 2>/dev/null; then
  fail "tampered index resolved"
fi
cp "$work/idx/stable.json.orig" "$work/idx/stable.json"

step "wrong trust key is refused"
"$LASTDB_BIN" app dev-init --key-file "$work/other.key" >"$work/other.out"
other_pub="$(awk '/dev_pubkey:/ {print $2}' "$work/other.out")"
if "$LASTDB_BIN" app index verify --index "$work/idx/stable.json" --trust-key "$other_pub" 2>/dev/null; then
  fail "index verified under the wrong key"
fi

step "resolve picks the exact proved pair"
r1="$("$LASTDB_BIN" app resolve demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.23.3-100-gaaaaaaaaa --json)"
[ "$(printf '%s' "$r1" | jq -r .sha)" = "$first" ] || fail "resolve for build 100 picked $(printf '%s' "$r1" | jq -r .sha)"
r2="$("$LASTDB_BIN" app resolve demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.23.3-200-gbbbbbbbbb --json)"
[ "$(printf '%s' "$r2" | jq -r .sha)" = "$second" ] || fail "resolve for build 200 picked $(printf '%s' "$r2" | jq -r .sha)"
if "$LASTDB_BIN" app resolve demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.24.0-1-gccccccccc 2>"$work/none.err"; then
  fail "unproved node build resolved"
fi
grep -q 'brew upgrade lastdb' "$work/none.err" || fail "unproved build error lacks the remedy"

step "install checks out the proved commit"
"$LASTDB_BIN" app install demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.23.3-100-gaaaaaaaaa --dir "$work/install/demo" >/dev/null
[ "$(git -C "$work/install/demo/source" rev-parse HEAD)" = "$first" ] || fail "install HEAD is not the proved commit"
[ "$(jq -r .proof_run "$work/install/demo/lastdb-app-install.json")" = "e2e-1" ] || fail "receipt lacks proof_run"

step "upgrade: same pair is a no-op, new pair reinstalls"
u1="$("$LASTDB_BIN" app upgrade demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.23.3-100-gaaaaaaaaa --dir "$work/install/demo" --json)"
[ "$(printf '%s' "$u1" | jq -r '.[0].upgraded')" = "false" ] || fail "same pair upgraded"
u2="$("$LASTDB_BIN" app upgrade demo --index "$work/idx" --trust-key "$pub" --lastdb-version 0.23.3-200-gbbbbbbbbb --dir "$work/install/demo" --json)"
[ "$(printf '%s' "$u2" | jq -r '.[0].upgraded')" = "true" ] || fail "new pair did not upgrade"
[ "$(git -C "$work/install/demo/source" rev-parse HEAD)" = "$second" ] || fail "upgrade HEAD is not the new proved commit"

step "list and info read the signed index anonymously"
"$LASTDB_BIN" app list --index "$work/idx" --trust-key "$pub" | grep -q '^demo' || fail "list lacks demo"
"$LASTDB_BIN" app info demo --index "$work/idx" --trust-key "$pub" | jq -e '.compat | length == 2' >/dev/null || fail "info lacks two rows"

echo
echo "PASS app-registry-index-e2e"
