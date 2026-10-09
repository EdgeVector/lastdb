#!/usr/bin/env bash
#
# Create the paired LastDB Mini release bundle.
#
# The tarball remains binary-only (`lastdb` + `lastdbd`) for Homebrew and the
# fresh-install shape gate. The sidecar manifest records the exact source OID,
# tarball hash, and per-binary versions/hashes so promotion tooling can prove it
# is consuming a matched CLI/daemon pair.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  package-lastdb-bundle.sh --target TRIPLE --bin-dir DIR --out-dir DIR [options]

Options:
  --target TRIPLE          Release target triple, e.g. aarch64-apple-darwin.
  --bin-dir DIR            Directory containing executable lastdb and lastdbd.
  --out-dir DIR            Output directory for lastdb-TRIPLE.tar.gz and manifest.
  --git-oid OID            Source commit OID. Defaults to GITHUB_SHA or git HEAD.
  --expect-version X.Y.Z   Require both binaries to report this version.
  --help | -h              Show this help.
EOF
}

fail() {
  echo "package-lastdb-bundle: $*" >&2
  exit 64
}

target=""
bin_dir=""
out_dir=""
git_oid="${GITHUB_SHA:-}"
expect_version=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --target) [ "$#" -ge 2 ] || fail "missing value for --target"; target="$2"; shift 2 ;;
    --bin-dir) [ "$#" -ge 2 ] || fail "missing value for --bin-dir"; bin_dir="$2"; shift 2 ;;
    --out-dir) [ "$#" -ge 2 ] || fail "missing value for --out-dir"; out_dir="$2"; shift 2 ;;
    --git-oid) [ "$#" -ge 2 ] || fail "missing value for --git-oid"; git_oid="$2"; shift 2 ;;
    --expect-version) [ "$#" -ge 2 ] || fail "missing value for --expect-version"; expect_version="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) fail "unknown argument: $1" ;;
  esac
done

[ -n "$target" ] || fail "--target is required"
[ -n "$bin_dir" ] || fail "--bin-dir is required"
[ -n "$out_dir" ] || fail "--out-dir is required"

for tool in tar python3; do
  command -v "$tool" >/dev/null 2>&1 || fail "missing required tool: $tool"
done
if ! command -v shasum >/dev/null 2>&1 && ! command -v sha256sum >/dev/null 2>&1; then
  fail "missing required tool: shasum or sha256sum"
fi

if [ -z "$git_oid" ]; then
  git_oid="$(git rev-parse HEAD 2>/dev/null || true)"
fi
[ -n "$git_oid" ] || fail "could not determine source git OID"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

version_of() {
  "$1" --version 2>/dev/null | awk '{print $NF}'
}

for bin in lastdb lastdbd; do
  [ -x "$bin_dir/$bin" ] || fail "required executable missing: $bin_dir/$bin"
done

lastdb_version="$(version_of "$bin_dir/lastdb")"
lastdbd_version="$(version_of "$bin_dir/lastdbd")"
[ -n "$lastdb_version" ] || fail "lastdb did not report a version"
[ -n "$lastdbd_version" ] || fail "lastdbd did not report a version"
[ "$lastdb_version" = "$lastdbd_version" ] || \
  fail "paired-bundle version skew: lastdb=$lastdb_version lastdbd=$lastdbd_version"
if [ -n "$expect_version" ] && [ "$lastdb_version" != "$expect_version" ]; then
  fail "version mismatch: expected $expect_version, got $lastdb_version"
fi

mkdir -p "$out_dir"
stage="$(mktemp -d "${TMPDIR:-/tmp}/lastdb-bundle.XXXXXX")"
trap 'rm -rf "$stage"' EXIT
cp "$bin_dir/lastdb" "$stage/lastdb"
cp "$bin_dir/lastdbd" "$stage/lastdbd"

tarball="$out_dir/lastdb-${target}.tar.gz"
manifest="$out_dir/lastdb-${target}.manifest.json"
# BSD tar otherwise adds hidden AppleDouble sidecars for extended attributes.
# The release archive contains the executable pair and no metadata files.
COPYFILE_DISABLE=1 tar -czf "$tarball" -C "$stage" .

lastdb_sha="$(sha256_file "$stage/lastdb")"
lastdbd_sha="$(sha256_file "$stage/lastdbd")"
tarball_sha="$(sha256_file "$tarball")"

python3 - "$manifest" "$target" "$git_oid" "$tarball" "$tarball_sha" \
  "$lastdb_version" "$lastdb_sha" "$lastdbd_version" "$lastdbd_sha" <<'PY'
import json
import os
import sys

(
    manifest_path,
    target,
    git_oid,
    tarball_path,
    tarball_sha,
    lastdb_version,
    lastdb_sha,
    lastdbd_version,
    lastdbd_sha,
) = sys.argv[1:]

payload = {
    "schema": "lastdb.mini.bundle.v1",
    "target": target,
    "source_git_oid": git_oid,
    "artifact": {
        "file": os.path.basename(tarball_path),
        "sha256": tarball_sha,
    },
    "binaries": [
        {
            "name": "lastdb",
            "version": lastdb_version,
            "sha256": lastdb_sha,
        },
        {
            "name": "lastdbd",
            "version": lastdbd_version,
            "sha256": lastdbd_sha,
        },
    ],
}

with open(manifest_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY

echo "LASTDB_BUNDLE=ok target=$target tarball=$tarball manifest=$manifest version=$lastdb_version git_oid=$git_oid"
