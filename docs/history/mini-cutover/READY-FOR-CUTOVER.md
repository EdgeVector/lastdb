READY-FOR-CUTOVER

# Mini cutover safety package (filled)

- Captured at: `2026-07-18T01:46:05Z`
- Source home (offline): `/Users/example/lastdb-cloudtest`
- Work dir: `/tmp/mini-cutover-safety-run2`
- Generator: `scripts/lastdbd/mini-cutover-safety-package.sh`
- Template: `docs/history/mini-cutover/CUTOVER_SAFETY_PACKAGE.md`

## 1. Durable backup procedure (primary — at flip only)

```bash
SRC="$HOME/.lastdb"
DEST="$HOME/lastdb-backup-pre-laststore-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$DEST"
cp -cR "$SRC/." "$DEST/" 2>/dev/null || cp -pR "$SRC/." "$DEST/"
du -sh "$SRC" "$DEST"
```

Do not use live primary as a migrator write target. Backup first, migrate the backup.

## 2. Fresh laststore home from this offline source

- Migrate work: `/tmp/mini-cutover-safety-run2/migrate`
- Laststore home: `/tmp/mini-cutover-safety-run2/migrate/laststore-home` (when migrate PASS)
- Migrate report: `/tmp/mini-cutover-safety-run2/migrate-report.json`

## 3. Verification results

| check | result | note |
| --- | --- | --- |
| `migrator_coverage` | PASS | main keys mapped/unmapped/total=6049 0 6049 |
| `domain_encrypt_tests` | PASS | cargo test -p fold_db --lib domain_encrypt |
| `migrate_unit_tests` | PASS | cargo test -p fold_db --lib migrate |
| `empty_laststore_smoke` | PASS | laststore-empty-home-smoke.sh |
| `refuse_primary_source` | PASS | migrate refuses ~/.lastdb (exit 65) |
| `package_template` | PASS | docs/history/mini-cutover/CUTOVER_SAFETY_PACKAGE.md |
| `rollback_docs` | PASS | rollback section in package template |
| `brain_kanban_throwaway` | SKIP | optional for READY; run against throwaway Mini home only at flip soak |

## 4. Rollback

1. Stop writers on the cutover home (ops/Tom — do not casually kill primary).
2. Set engine back to sled (default): unset `LASTDB_ENGINE` or `LASTDB_ENGINE=sled`.
3. Point Mini home at the sled backup from §1 (or original `~/.lastdb` if only config swapped).
4. Retain laststore tree until soak window ends; do not delete sled backup for 7–14 days.

## 5. Flip plan (NOT executed by this package)

Card: `mini-cutover-p4-primary-flip` — blocked until:

1. This report first line is `READY-FOR-CUTOVER`
2. Tom says **yes** once (open-decisions / explicit TOM-YES-CUTOVER)

Then: Situations notice → quiet writers → durable backup → migrate backup →
point Mini at new home or atomic swap → verify checklist → retain sled backup.

## 6. Human decision line

`NEEDS-DECISION mini-cutover-p4-primary-flip — READY-FOR-CUTOVER package attached — reply yes to flip primary (or no + reason)`
