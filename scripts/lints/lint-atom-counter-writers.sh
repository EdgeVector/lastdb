#!/usr/bin/env bash
# lint-atom-counter-writers.sh
#
# Writer boundary of the atom counter (docs: design of the counter lifecycle).
# Only the planner (db_operations/atom_store/counter/) writes tip records,
# conflict references, atom bodies and atom counters inside a write scope.
# This lint finds every other file that writes through a raw store handle and
# compares the set with scripts/lints/atom-counter-writers.allow. A new writer
# fails here until someone routes it through the planner or adds it to the
# list with a reason. A list entry that no longer matches also fails.
#
# Exit 0 on a clean tree; 1 otherwise.

set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
allow="$root/scripts/lints/atom-counter-writers.allow"
cd "$root"

if ! command -v rg >/dev/null 2>&1; then
  echo "lint-atom-counter-writers: ripgrep (rg) not found in PATH" >&2
  exit 1
fi
[[ -f "$allow" ]] || { echo "lint-atom-counter-writers: missing $allow" >&2; exit 1; }

dirs=(fold_db/crates/core/src lastdb_node/src lastdb_host lastdb_uds)
tmp="$(mktemp -d "${TMPDIR:-/tmp}/atom-counter-lint.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

# rule raw: a write through `.raw()` or `main_store`.
rg -l -U --glob '*.rs' \
  -e '(\.raw\(\)|main_store)\s*(\.inner\(\))?\s*\.(put_item|batch_put_items|batch_delete_keys|delete_item|batch_mutate|batch_put|put|delete|batch_delete)\(' \
  "${dirs[@]}" | sort -u > "$tmp/raw.found" || true

# rule sync: a file under sync/ or a node bin that names a tip, body or counter
# key family and also writes raw.
key_tokens='hash_range_record_key|history_event_key|body_key\(|body_storage_key\(|counter_key\(|PerKeyRecord|AtomCounter\b|"mk:|atom_key_codec|MK_PREFIX|anchored\("conflict"'
write_tokens='batch_mutate\(|batch_put\(|batch_put_items\(|put_item\(|batch_delete_keys\(|delete_item\(|\.batch_delete\(|restore_batch_put|KvMutation::(put|delete)|\.put\(|\.delete\('
: > "$tmp/sync.found"
while IFS= read -r f; do
  if rg -q -e "$write_tokens" "$f"; then echo "$f" >> "$tmp/sync.found"; fi
done < <(rg -l -e "$key_tokens" --glob '*.rs' fold_db/crates/core/src/sync lastdb_node/src | sort -u)
sort -u -o "$tmp/sync.found" "$tmp/sync.found"

# rule restore: a generic row restore.
rg -l --glob '*.rs' --glob '!**/storage/**' -e 'restore_batch_put\(' "${dirs[@]}" | sort -u > "$tmp/restore.found" || true

# rule counter: the counter record type or its key builder.
rg -l --glob '*.rs' -e '\bcounter_key\(|\bAtomCounter\b' "${dirs[@]}" | sort -u > "$tmp/counter.found" || true

failed=0
for rule in raw sync restore counter; do
  grep -E "^${rule}\|" "$allow" | cut -d'|' -f2 | sort -u > "$tmp/$rule.allowed"
  new="$(comm -13 "$tmp/$rule.allowed" "$tmp/$rule.found")"
  stale="$(comm -23 "$tmp/$rule.allowed" "$tmp/$rule.found")"
  if [[ -n "$new" ]]; then
    echo "lint-atom-counter-writers: rule '$rule' found a file that is not in the allow list:" >&2
    echo "$new" | sed 's/^/  /' >&2
    echo "  Route the write through the planner (counter::commit_tip_changes), or add the file to" >&2
    echo "  scripts/lints/atom-counter-writers.allow with the families it touches and a reason." >&2
    failed=1
  fi
  if [[ -n "$stale" ]]; then
    echo "lint-atom-counter-writers: rule '$rule' allow list names a file that no longer matches:" >&2
    echo "$stale" | sed 's/^/  /' >&2
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
echo "lint-atom-counter-writers: OK (raw=$(wc -l < "$tmp/raw.found" | tr -d ' ') sync=$(wc -l < "$tmp/sync.found" | tr -d ' ') restore=$(wc -l < "$tmp/restore.found" | tr -d ' ') counter=$(wc -l < "$tmp/counter.found" | tr -d ' '))"
