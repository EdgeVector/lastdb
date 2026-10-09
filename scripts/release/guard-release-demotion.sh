#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage:
  VERSION_TAG=vX.Y.Z guard-release-demotion.sh

Refuses to publish a final release tag if that exact version is already marked
as a prerelease on the public tap release. A hand-demoted final release uses the
existing prerelease flag as the persistent per-version veto marker.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

: "${VERSION_TAG:?VERSION_TAG must be set to the pushed tag (for example v0.20.3)}"

if [[ "$VERSION_TAG" == *-* ]]; then
  echo "Release demotion guard: $VERSION_TAG is already a prerelease tag; skipping final-release veto check."
  exit 0
fi

REPOS="${RELEASE_DEMOTION_GUARD_REPOS:-EdgeVector/homebrew-lastdb}"

for repo in $REPOS; do
  err_file="$(mktemp)"
  if json="$(gh release view "$VERSION_TAG" --repo "$repo" --json isPrerelease 2>"$err_file")"; then
    if [[ "$(jq -r '.isPrerelease // false' <<<"$json")" == "true" ]]; then
      echo "::error::Release $repo@$VERSION_TAG is already marked prerelease. Refusing to publish a final-tag release or stable feed because that prerelease flag is the demotion veto marker."
      rm -f "$err_file"
      exit 1
    fi
    echo "Release demotion guard: $repo@$VERSION_TAG is not demoted."
  else
    err="$(cat "$err_file")"
    if grep -Eiq 'not found|could not resolve to a release|no release found|HTTP 404' <<<"$err"; then
      echo "Release demotion guard: $repo@$VERSION_TAG does not exist yet."
    else
      echo "::error::Could not inspect $repo@$VERSION_TAG for demotion veto."
      if [[ -n "$err" ]]; then
        echo "$err" >&2
      fi
      rm -f "$err_file"
      exit 1
    fi
  fi
  rm -f "$err_file"
done

echo "Release demotion guard: no prerelease demotion veto found for $VERSION_TAG."
