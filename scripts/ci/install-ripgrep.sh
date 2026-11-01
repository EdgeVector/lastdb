#!/usr/bin/env bash
# Install ripgrep 14.1.0 without apt.
#
# Run 41351158807, mini lane, 2026-11-01: the apt step took 13s.
# The deb download took 1s. apt spent the rest on the package
# database and the needrestart scan.
set -euo pipefail

if command -v rg >/dev/null 2>&1; then
  echo "ripgrep already on PATH: $(command -v rg)"
  rg --version | head -n 1
  exit 0
fi

if [[ "$(uname -s)" != "Linux" || "$(uname -m)" != "x86_64" ]]; then
  echo "install-ripgrep: pinned binary is Linux x86_64 only (have $(uname -s) $(uname -m))" >&2
  exit 1
fi

if [[ -z "${GITHUB_PATH:-}" ]]; then
  echo "install-ripgrep: GITHUB_PATH is unset" >&2
  exit 1
fi

version=14.1.0
name="ripgrep-${version}-x86_64-unknown-linux-musl"
# Published file ripgrep-14.1.0-x86_64-unknown-linux-musl.tar.gz.sha256
expect="f84757b07f425fe5cf11d87df6644691c644a5cd2348a2c670894272999d3ba7"
url="https://github.com/BurntSushi/ripgrep/releases/download/${version}/${name}.tar.gz"

dest="${RUNNER_TEMP:-/tmp}/ripgrep-${version}"
mkdir -p "$dest"
archive="$dest/${name}.tar.gz"
curl -fsSL --retry 3 --retry-delay 2 -o "$archive" "$url"
printf '%s  %s\n' "$expect" "$archive" | sha256sum -c -
tar -xzf "$archive" -C "$dest" "${name}/rg"

bindir="$dest/bin"
mkdir -p "$bindir"
mv "$dest/${name}/rg" "$bindir/rg"
chmod 755 "$bindir/rg"
printf '%s\n' "$bindir" >>"$GITHUB_PATH"
"$bindir/rg" --version
