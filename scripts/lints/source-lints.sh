#!/usr/bin/env bash
# Run the cheap source lints over every product crate. ci-required runs this
# script, so a new crate or HTTP client cannot skip a lint that only a manual
# workflow ran. Add a new workspace crate to CRATES below.
#
# usage: bash scripts/lints/source-lints.sh
set -euo pipefail

cd "$(dirname "$0")/../.."

CRATES=(
  fold_db/crates
  schema_service/crates
  lastdb_node
  cloud_backup_protocol
  mutation_log
  mutation_log_mem
  mutation_log_sync
  database_cutover
  app_identity_crypto
)
scopes=()
for crate in "${CRATES[@]}"; do
  test -d "$crate" || { echo "ERROR: source-lints crate path is missing: $crate" >&2; exit 1; }
  scopes+=("--scope=$crate")
done

failed=0
run() {
  echo "== $*"
  if ! bash "$@"; then
    failed=1
  fi
}

run scripts/lints/lint-tracing-egress.sh "${scopes[@]}" --strict
run scripts/lints/lint-redaction.sh "${scopes[@]}"
run scripts/lints/lint-byte-slice.sh "${scopes[@]}"
run scripts/lints/lint-spawn-instrument.sh "${scopes[@]}"
run scripts/lints/lint-no-hardcoded-urls.sh
run scripts/lints/lint-workspace-edition-inheritance.sh

if [ "$failed" -ne 0 ]; then
  echo "ERROR: one or more source lints failed" >&2
  exit 1
fi
echo "source lints: ok"
