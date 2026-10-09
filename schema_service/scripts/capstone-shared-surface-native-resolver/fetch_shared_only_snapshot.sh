#!/bin/sh
set -eu

default_locator="lastsecrets://schema-service-prod-api-key"
CDPATH=
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
env_json="${FOLD_ENVIRONMENTS_JSON:-$repo_root/folddb_profile/environments.json}"
env_name="${SCHEMA_SERVICE_ENV:-prod}"

locator="${SCHEMA_SERVICE_PROD_API_KEY_LOCATOR:-$default_locator}"
out="${SCHEMA_SERVICE_SHARED_ONLY_OUT:-/tmp/schema-service-shared-only.json}"
lastsecrets_bin="${LASTSECRETS_BIN:-lastsecrets}"
curl_bin="${CURL_BIN:-/usr/bin/curl}"

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

resolve_default_url() {
  python3 - "$env_json" "$env_name" <<'PY'
import json
import sys

path, env_name = sys.argv[1:3]
with open(path, encoding="utf-8") as fh:
    environments = json.load(fh)["environments"]
base = environments[env_name]["schema_service"].rstrip("/")
print(f"{base}/v1/snapshot/shared-only")
PY
}

if [ -n "${SCHEMA_SERVICE_SHARED_ONLY_URL:-}" ]; then
  url="$SCHEMA_SERVICE_SHARED_ONLY_URL"
elif ! url="$(resolve_default_url)"; then
  die "could not resolve schema_service URL for env=$env_name from $env_json"
fi

usage() {
  cat <<'USAGE'
Usage: fetch_shared_only_snapshot.sh [options]

Fetch the prod Schema Service shared-only snapshot using an API key read from
LastSecrets at point of use. The secret value is never printed and is passed to
curl through stdin config, not as a process argument.

Options:
  --locator lastsecrets://slug   API-key locator
  --url URL                      shared-only snapshot URL
  --out PATH                     output path for the JSON response
  --lastsecrets-bin PATH         lastsecrets executable
  --curl-bin PATH                curl executable
  -h, --help                     show this help

Environment defaults:
  SCHEMA_SERVICE_PROD_API_KEY_LOCATOR=lastsecrets://schema-service-prod-api-key
  SCHEMA_SERVICE_ENV=prod
  FOLD_ENVIRONMENTS_JSON=<repo>/folddb_profile/environments.json
  SCHEMA_SERVICE_SHARED_ONLY_URL=<optional explicit override>
  SCHEMA_SERVICE_SHARED_ONLY_OUT=/tmp/schema-service-shared-only.json
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --locator)
      [ "$#" -ge 2 ] || die "missing value for --locator"
      locator="$2"
      shift 2
      ;;
    --url)
      [ "$#" -ge 2 ] || die "missing value for --url"
      url="$2"
      shift 2
      ;;
    --out)
      [ "$#" -ge 2 ] || die "missing value for --out"
      out="$2"
      shift 2
      ;;
    --lastsecrets-bin)
      [ "$#" -ge 2 ] || die "missing value for --lastsecrets-bin"
      lastsecrets_bin="$2"
      shift 2
      ;;
    --curl-bin)
      [ "$#" -ge 2 ] || die "missing value for --curl-bin"
      curl_bin="$2"
      shift 2
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

case "$locator" in
  lastsecrets://?*) ;;
  lastsecrets://) die "empty LastSecrets locator for Schema Service prod API key" ;;
  *) die "Schema Service prod API key must use a lastsecrets:// locator, got: $locator" ;;
esac

slug="${locator#lastsecrets://}"

if ! command -v "$lastsecrets_bin" >/dev/null 2>&1; then
  die "lastsecrets command unavailable; cannot resolve $locator"
fi

if ! command -v "$curl_bin" >/dev/null 2>&1; then
  die "curl command unavailable: $curl_bin"
fi

if ! api_key="$("$lastsecrets_bin" get "$slug" 2>/dev/null)"; then
  die "missing Schema Service prod API-key locator $locator; provision with: lastsecrets put $slug --value-stdin"
fi

if [ -z "$api_key" ]; then
  unset api_key
  die "Schema Service prod API-key locator $locator returned an empty value"
fi

http_code="$(
  printf 'header = "X-API-Key: %s"\n' "$api_key" \
    | "$curl_bin" --config - -sS -o "$out" -w '%{http_code}' "$url"
)" || {
  unset api_key
  die "curl failed while fetching Schema Service shared-only snapshot"
}
unset api_key

case "$http_code" in
  2??)
    printf 'Fetched Schema Service shared-only snapshot to %s (HTTP %s) using %s\n' "$out" "$http_code" "$locator"
    ;;
  401|403)
    die "Schema Service rejected the API key resolved from $locator (HTTP $http_code)"
    ;;
  *)
    die "Schema Service shared-only snapshot fetch returned HTTP $http_code; response saved to $out"
    ;;
esac
