# Mini cutover safety package (template)

**Status:** Phase 4 package template — fill via  
`bash scripts/lastdbd/mini-cutover-safety-package.sh --source <offline-or-backup-home>`

**Human gate:** Tom says **yes/no** once on a report whose first line is  
`READY-FOR-CUTOVER` (or `NOT-READY` with gaps). Primary flip is a **separate** card  
(`mini-cutover-p4-primary-flip`) and must not run until that yes.

---

## 1. Durable offline backup (primary — execute only at flip)

Never run these against a live writers-hot primary without a quiet window.

```bash
# Example durable full copy (APFS clone when available):
SRC="$HOME/.lastdb"
DEST="$HOME/lastdb-backup-pre-laststore-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$DEST"
if cp -cR "$SRC/." "$DEST/" 2>/dev/null; then echo "clone OK"; else cp -pR "$SRC/." "$DEST/"; fi
du -sh "$SRC" "$DEST"
# Optional: tar offline
# tar -C "$HOME" -czf "${DEST}.tgz" "$(basename "$DEST")"
```

Safety: destination must **not** be `~/.lastdb` itself. Keep this tree read-only for the rollback window (7–14 days after flip).

---

## 2. Build fresh Last Store home from backup (CoW / offline only)

```bash
# Prefer a non-primary offline copy as --source (never pass live primary
# into migrate tooling as a write target).
bash scripts/lastdbd/mini-cutover-migrate.sh \
  --source "$DEST" \
  --work /tmp/mini-cutover-safety-work \
  --report /tmp/mini-cutover-safety-work/migrate-report.json
# laststore root: /tmp/mini-cutover-safety-work/laststore-home
```

Migrator: logical export → encrypting Last Store; splits sled `main` by key prefix  
(`fold_db::mini_cutover`). Requires 100% key coverage (`ok: true`).

---

## 3. Verification checklist (throwaway home)

| Check | How | Pass |
|-------|-----|------|
| Migrator coverage | `migrate-report.json` → `ok` true, unmapped=0 | required |
| Sample key parity | Phase 2 CoW GREEN harness / migrate re-open samples | required |
| Domain encrypt | `cargo test -p fold_db --lib domain_encrypt -- --test-threads=1` | required (CI / local) |
| Empty laststore boot | `bash scripts/lastdbd/laststore-empty-home-smoke.sh` | required |
| Brain/kanban on throwaway Mini | Optional until flip: point a **throwaway** home only; never re-point primary silently | optional for READY if documented |
| Rollback dry-run | Commands in §4 reviewed | required |

Generator script records each row in the filled report.

---

## 4. Rollback (after a mistaken or unhealthy flip)

Primary still holds a sled tree backup from §1.

```bash
# Stop writers on the cutover home (do not kill primary casually — Tom/ops).
# Restore config default:
#   LASTDB_ENGINE=sled   # or omit; sled is factory default
# Point Mini home back at the pre-cutover sled tree (the §1 DEST, or
# the original ~/.lastdb if only config was swapped).
# Do NOT delete the laststore tree until soak window ends.
```

Exact paths for a live flip are filled in the READY report’s **Flip plan** section by the generator (still not executed by this package).

---

## 5. Situations notice (at flip time only)

```bash
# Example — run only when executing primary flip, not when packaging:
situations notice --title "Mini Last Store primary cutover starting" \
  --kind cutover --system lastdbd \
  --body "Primary home swap to laststore; sled backup retained at <path>."
```

---

## 6. Report verdict

Filled report path (host or work dir):

- Template-driven output: `docs/history/mini-cutover/READY-FOR-CUTOVER.md` (checked-in sample from CoW)  
- Or host-local: `$HOME/.last-stack/mini-cutover/READY-FOR-CUTOVER.md`

**First non-empty line must be exactly one of:**

```text
READY-FOR-CUTOVER
```

or

```text
NOT-READY
```

followed by why. Tom’s single yes/no decision attaches to the READY report.
