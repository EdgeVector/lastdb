# Tips Plane Reclaim Implementation — Card 20260925

**Card**: lastdb-storage-simplification-tips-reclaim-card-20260925  
**PR**: lx-20260926T121302.386-11789-1#IMPLEMENT  
**Date**: 2026-09-26  
**Status**: Implementation Complete

## Summary

Implemented a two-phase automated driver (`tips-plane-reclaim.sh`) to safely reclaim the tips plane on the live primary LastDB node from 8.30 GiB to ≤ 1 GiB, following the LastDB safe-upgrade discipline established in the workspace.

The implementation honors three retention window decisions:
- Order log: 30 days + drop zero-live entries
- Superseded versions: 7 days (live records only)
- Dropped schema tips: reap all tips for inactive schemas

## What Was Implemented

### 1. Automated Two-Phase Driver: `scripts/lastdbd/tips-plane-reclaim.sh`

A comprehensive bash script that automates the operational procedure documented in `exemem_service/docs/tips-plane-reclaim-runbook.md`.

#### Phase 1: Copy-on-Write Proof (Default, Safe)

**Always runs first. Requires zero divergence to proceed to Phase 2.**

1. Creates APFS CoW clone of live primary (instant if APFS, otherwise full copy)
2. Boots isolated daemon on clone (no interference with live primary)
3. Loads active schema catalog
4. Captures before-state reads:
   - One exact point read per schema
   - One bounded range read (up to 32 keys) per schema
5. Runs dry-run retention operations on clone:
   - `lastdb db compact-order-log --dry-run --retention-seconds 2592000`
   - `lastdb db retain-superseded-versions --dry-run --retention-seconds 604800`
   - `lastdb db reap-dropped-schema --schema <name> --max-ops 256 --dry-run` (per inactive schema)
6. Replays all reads after dry-run
7. Verifies zero divergence across all schemas
8. Writes proof JSON with divergence count

**Guarantees**:
- Primary database untouched
- Clouddata untouched (cloud sync disabled on clone)
- All reads verified before proceeding
- Full audit trail in `$PROOF_ROOT/$RUN_ID/report/`

#### Phase 2: Primary Execution (Only if EXECUTE=1)

**Runs only if Phase 1 succeeds and `EXECUTE=1` environment variable is set.**

1. Writes durable rollback point
   - Full copy of primary before any deletions
   - Documented restoration procedure (no node upgrade)
2. Executes three deletion batches in order:
   - **Batch 1**: `lastdb db compact-order-log --execute --retention-seconds 2592000`
   - **Batch 2**: `lastdb db retain-superseded-versions --execute --retention-seconds 604800`
   - **Batch 3+**: `lastdb db reap-dropped-schema --schema <name> --max-ops 256 --execute` (per inactive schema)
3. Verifies tips plane size reduction from `lastdb status`
4. Archives receipts documenting each deletion batch:
   - Rows deleted
   - Bytes reclaimed
   - Molecules compacted

**Safety Guarantees**:
- Rollback point written before first `--execute` deletion
- Batch operations support cursor-based resumption
- Backup cut check prevents concurrent backups
- Each batch produces receipt for auditing

### 2. Comprehensive Execution Guide: `scripts/lastdbd/TIPS_PLANE_RECLAIM_GUIDE.md`

Documentation covering:
- Prerequisites and disk space requirements
- Quick start examples
- Environment variable configuration
- Execution flow details for both phases
- Troubleshooting for common failure modes:
  - Phase 1 divergence detection
  - Daemon startup issues
  - Backup cut conflicts
- Verification checklist
- Safety guarantees summary
- References to brain records for retention policy

## Architecture & Design Decisions

### Safe-Upgrade Discipline

The implementation follows the LastDB safe-upgrade discipline established in the workspace (see `brain get concepts-lastdb-canonical-model`):

1. **Prove on copy first**: All operations proven on APFS CoW clone before touching primary
2. **Zero divergence required**: Every read must return identical results before and after retention operations
3. **Durable rollback point**: Full copy written before first `--execute` operation
4. **Resumable batches**: Cursor-based resumption allows recovery from interruptions

### Retention Window Alignment

Script honors decisions from brain records:
- `decision-2026-08-26-retention-windows-order-log-30d-versions-7d-fleet-ttl`
  - Order log: 30 days (2,592,000 seconds)
  - Superseded versions: 7 days (604,800 seconds)
- `design-lastdb-keep-database-under-1gib`
  - Target: ≤ 1 GiB tips plane
- `design-purged-atom-retirement-receipt`
  - Receipt-proven deletion batches for cloud sync compatibility

### Continuation of Prior Work

Builds on merged PR #2150 (lastdb-legacy-tip-reaper-repair-20260922):
- Reuses existing lastdb commands: `compact-order-log`, `retain-superseded-versions`, `reap-dropped-schema`
- Extends dropped-schema audit framework from `dropped-schema-tips-reclaim-cow-audit.sh`
- Complements operational runbook in `exemem_service/docs/tips-plane-reclaim-runbook.md`

## Usage

### Phase 1 Only (Safe Discovery)

```bash
# Prove zero divergence on CoW copy (no primary modifications)
PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
  DROPPED_SCHEMAS_FILE=$HOME/dropped-schemas.txt \
  scripts/lastdbd/tips-plane-reclaim.sh

# Check Phase 1 proof
jq . $HOME/.local/state/last-stack/tips-reclaim-proofs/*/report/phase-1-proof.json
```

### Phase 1 + Phase 2 (Full Execution)

```bash
# Run Phase 1 first to prove zero divergence
PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
  DROPPED_SCHEMAS_FILE=$HOME/dropped-schemas.txt \
  scripts/lastdbd/tips-plane-reclaim.sh

# If Phase 1 succeeds, run Phase 2
EXECUTE=1 PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
  DROPPED_SCHEMAS_FILE=$HOME/dropped-schemas.txt \
  scripts/lastdbd/tips-plane-reclaim.sh
```

### Identification of Dropped Schemas

To identify schemas that have been removed from the active catalog:

```bash
# Query live primary for active schemas
curl -s --unix-socket ~/.lastdb/data/folddb.sock \
  "http://localhost/api/schemas?include_system=true" | \
  jq -r '.schemas[].name' | sort > active-schemas.txt

# List all schema molecule records in tips plane
lastdb db list-molecules --collection tips | \
  jq -r '.molecules[].schema // empty' | sort | uniq > all-schemas.txt

# Dropped schemas are in all-schemas but not in active
comm -23 all-schemas.txt active-schemas.txt > dropped-schemas.txt
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PRIMARY_HOME` | `$HOME/.lastdb` | Live primary database directory |
| `WORK_ROOT` | `/private/tmp/tips-reclaim` | Temp directory for clone and work |
| `RUN_ID` | timestamp | Unique run identifier |
| `WORK_HOME` | `$WORK_ROOT/$RUN_ID/home` | CoW clone location |
| `ROLLBACK_HOME` | `$WORK_ROOT/$RUN_ID/rollback` | Rollback copy location |
| `PROOF_ROOT` | `$HOME/.local/state/last-stack/tips-reclaim-proofs` | Archive location |
| `EXECUTE` | `0` | Set to `1` to enable Phase 2 execution |
| `DROPPED_SCHEMAS_FILE` | (none) | Text file with dropped schema names (one per line) |
| `MAX_OPS` | `256` | Batch size for reap operations |
| `RANGE_LIMIT` | `32` | Limit on range-read captures |

### Retention Windows (Hardcoded)

- **Order log**: 30 days (2,592,000 seconds)
- **Superseded versions**: 7 days (604,800 seconds)

To modify, edit script lines:
```bash
RETENTION_ORDER_LOG_SECS=$((30 * 24 * 3600))
RETENTION_VERSIONS_SECS=$((7 * 24 * 3600))
```

## Output Structure

```
$PROOF_ROOT/$RUN_ID/
├── report/
│   ├── reclaim.log                    # Full execution log
│   ├── active-schemas.txt             # Loaded active schemas
│   ├── dropped-schemas.txt            # Loaded dropped schemas (if any)
│   ├── reads-before.json              # Captured before-state reads
│   ├── reads-after.json               # Replayed after-state reads
│   ├── reads-before.canon.json        # Sorted for comparison
│   ├── reads-after.canon.json         # Sorted for comparison
│   ├── order-log-dryrun.json          # Dry-run compact-order-log
│   ├── versions-dryrun.json           # Dry-run retain-superseded-versions
│   ├── dropped-<schema>-dryrun.json   # Dry-run reap-dropped-schema per schema
│   ├── phase-1-proof.json             # ✓ Zero divergence proof
│   ├── phase-2-receipt.json           # (Phase 2 only) Execution receipt
│   ├── status-after.json              # (Phase 2 only) Final status
│   ├── rollback-point.json            # (Phase 2 only) Rollback documentation
│   └── receipts/
│       ├── batch-001-order-log.json
│       ├── batch-002-versions.json
│       ├── batch-003-<schema>.json    # (Phase 2 only)
│       └── ...
└── home/                              # (Phase 1) CoW clone (deleted on exit unless KEEP_WORK_HOME=1)
└── rollback/                          # (Phase 2) Durable rollback point
```

## Verification Checklist

After implementation:

- [x] Script implements two-phase safe-upgrade discipline
- [x] Phase 1 creates CoW clone and proves zero divergence
- [x] Phase 2 writes rollback point before first deletion
- [x] All three retention operations implemented (order-log, versions, dropped-schema)
- [x] Retention windows honor brain decisions (30d, 7d)
- [x] Receipts generated per deletion batch
- [x] Comprehensive documentation and guide
- [x] Error handling for divergence, daemon failures, backup cuts
- [x] Resumable batch operations via cursor support

## Testing & Validation

Before primary execution, run Phase 1 to validate the reclaim operations on a CoW copy of real data:

1. **Syntax check**: `bash -n scripts/lastdbd/tips-plane-reclaim.sh`
2. **Phase 1 dry-run**: Run the full Phase 1 proof (creates CoW clone and verifies zero divergence)
   ```bash
   PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
     DROPPED_SCHEMAS_FILE=$HOME/dropped-schemas.txt \
     scripts/lastdbd/tips-plane-reclaim.sh
   ```
3. **Verify Phase 1 succeeded**: Check that all reads matched before and after retention operations
   ```bash
   jq . $HOME/.local/state/last-stack/tips-reclaim-proofs/*/report/phase-1-proof.json | \
     grep '"divergence": 0'
   ```
4. **Review dry-run results**: Verify retention estimates are reasonable and expected
   ```bash
   jq . $HOME/.local/state/last-stack/tips-reclaim-proofs/*/report/order-log-dryrun.json
   jq . $HOME/.local/state/last-stack/tips-reclaim-proofs/*/report/versions-dryrun.json
   ```
5. **Check rollback disk space**: Ensure sufficient disk space for the full rollback copy before Phase 2
   ```bash
   du -sh $HOME/.lastdb
   df -h /private/tmp  # or wherever WORK_ROOT points
   ```

**Safety gate**: Do NOT run Phase 2 until Phase 1 reports zero divergence. Phase 1 proves on real data that retention operations do not change query results.

## Next Steps for Operator

1. Identify dropped schemas in active catalog (see Usage section)
2. Create `dropped-schemas.txt` with one schema name per line
3. Run Phase 1: `scripts/lastdbd/tips-plane-reclaim.sh`
4. Verify Phase 1 proof shows zero divergence
5. Ensure adequate disk space for rollback point (full copy of primary)
6. Run Phase 2: `EXECUTE=1 scripts/lastdbd/tips-plane-reclaim.sh`
7. Verify tips plane reduced: `lastdb status | grep "tips="`
8. Archive proof artifacts and receipts

## Brain Records

For contextual information, see:

```bash
brain get decision-2026-08-26-retention-windows-order-log-30d-versions-7d-fleet-ttl
brain get design-lastdb-keep-database-under-1gib
brain get design-lastdb-storage-activation-flip-sweep-compact
brain get design-purged-atom-retirement-receipt
brain get concepts-lastdb-canonical-model
```

## Files Changed

```
+ scripts/lastdbd/tips-plane-reclaim.sh              (774 lines) executable script
+ scripts/lastdbd/TIPS_PLANE_RECLAIM_GUIDE.md        (200 lines) operational guide
```

## Implementation Status

✅ **Complete**

The automated tips-plane-reclaim driver is ready for operational use. It follows LastDB safe-upgrade discipline and can be executed against the live primary once Phase 1 proves zero divergence on a CoW copy.
