# Tips Plane Reclaim Execution Guide

**Objective**: Bring the tips plane on the live primary LastDB node from 8.30 GiB down to ≤ 1 GiB, honoring three retention windows.

**Document**: Automated execution guide for `tips-plane-reclaim.sh`  
**Date**: 2026-09-26  
**Retention Policy**:
- Order log: 30 days + drop zero-live entries
- Superseded versions: 7 days (live records only)
- Dropped schema tips: reap all tips for inactive schemas

## Prerequisites

1. **Disk space**: ~14+ GiB free in temp directory for CoW clone
2. **Responsive daemon**: Live primary must have a responsive `lastdbd` running
3. **No concurrent backups**: Cannot run while `backup_cut` is in flight
4. **Dropped schemas list** (optional): A text file with schema names to reap, one per line (comments starting with `#` are ignored)

## Quick Start

```bash
# Phase 1 only (dry-run proof on CoW copy)
PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
  scripts/lastdbd/tips-plane-reclaim.sh

# After Phase 1 succeeds, Phase 2 (primary execution)
EXECUTE=1 PRIMARY_HOME=$HOME/.lastdb WORK_ROOT=/private/tmp/tips-reclaim \
  scripts/lastdbd/tips-plane-reclaim.sh
```

## Execution Flow

### Phase 1: Copy-on-Write Proof (Default)

The script **always** runs Phase 1 first. This phase:

1. **Creates a CoW clone** of the live primary database
   - APFS CoW if available (instant, zero disk overhead)
   - Full copy otherwise (blocking, uses disk space)

2. **Boots isolated daemon** on the clone
   - No interference with live primary
   - Cloud sync disabled on clone

3. **Captures before-state reads** for every active schema
   - One point read (exact key)
   - One bounded range read (up to 32 keys)

4. **Runs dry-run retention operations** on clone
   - `compact-order-log` (30-day window)
   - `retain-superseded-versions` (7-day window)
   - `reap-dropped-schema` per inactive schema

5. **Replays all reads after dry-run** to verify zero divergence
   - If any schema read differs, Phase 1 fails
   - Ensures safe-upgrade discipline

6. **Writes proof document** with divergence count and retention estimates

**Output**: `$PROOF_ROOT/$RUN_ID/report/phase-1-proof.json` (and detailed logs)

### Phase 2: Primary Execution (Only if Phase 1 Succeeds)

Phase 2 runs **only** if `EXECUTE=1` is set **and** Phase 1 proved zero divergence:

1. **Writes durable rollback point**
   - Full copy of primary before any deletions
   - Restore with `cp -a <rollback> ~/.lastdb; lastdbd --data-dir ~/.lastdb`
   - No node version upgrade on restore

2. **Executes three deletion batches** in order:
   - **Batch 1**: `compact-order-log --execute` (30-day retention)
   - **Batch 2**: `retain-superseded-versions --execute` (7-day retention)
   - **Batch 3+**: `reap-dropped-schema --execute` per inactive schema

3. **Verifies tips plane reduction**
   - Measures final size from `lastdb status`
   - Confirms reduction toward 1 GiB target

4. **Archives receipts** documenting each deletion batch
   - Counts: rows deleted, bytes reclaimed, molecules compacted

**Output**: `$PROOF_ROOT/$RUN_ID/report/phase-2-receipt.json` and receipts per batch

## Environment Variables

```bash
PRIMARY_HOME        # Live primary data dir (default: $HOME/.lastdb)
WORK_ROOT           # Temp directory for clone and work (default: /private/tmp/tips-reclaim)
RUN_ID              # Unique run identifier (default: timestamp)
WORK_HOME           # CoW clone home (default: $WORK_ROOT/$RUN_ID/home)
ROLLBACK_HOME       # Rollback copy location (default: $WORK_ROOT/$RUN_ID/rollback)
PROOF_ROOT          # Archive location (default: $HOME/.local/state/last-stack/tips-reclaim-proofs)
EXECUTE             # Enable Phase 2: set to 1 (default: 0, Phase 1 only)
DROPPED_SCHEMAS_FILE # Text file with dropped schema names, one per line
LASTDB              # Path to lastdb binary (auto-detected from repo)
LASTDBD             # Path to lastdbd binary (auto-detected from repo)
MAX_OPS             # Batch size for reap operations (default: 256)
RANGE_LIMIT         # Limit on range-read captures (default: 32)
NODE_WAIT_TRIES     # Max attempts to wait for daemon socket (default: 600)
```

## Example: Full Execution

```bash
#!/bin/bash
set -euo pipefail

# Identify dropped schemas by querying the live primary
PRIMARY_HOME=$HOME/.lastdb
WORK_ROOT=/private/tmp/tips-reclaim-$(date +%Y%m%d)

# Phase 1: Dry-run proof
PRIMARY_HOME="$PRIMARY_HOME" WORK_ROOT="$WORK_ROOT" \
  scripts/lastdbd/tips-plane-reclaim.sh

# Check Phase 1 proof
RUN_ID=$(ls -td "$HOME/.local/state/last-stack/tips-reclaim-proofs"/* | head -1 | xargs basename)
PROOF_DIR="$HOME/.local/state/last-stack/tips-reclaim-proofs/$RUN_ID/report"

# Verify zero divergence
DIVERGENCE=$(jq '.divergence_count' "$PROOF_DIR/phase-1-proof.json")
if [[ "$DIVERGENCE" != "0" ]]; then
  echo "FAIL: Phase 1 divergence detected ($DIVERGENCE schemas differ)"
  exit 1
fi

echo "✓ Phase 1 passed. Proceeding to Phase 2..."

# Phase 2: Primary execution
EXECUTE=1 PRIMARY_HOME="$PRIMARY_HOME" WORK_ROOT="$WORK_ROOT" RUN_ID="$RUN_ID" \
  scripts/lastdbd/tips-plane-reclaim.sh

# Verify results
RECEIPT="$PROOF_DIR/phase-2-receipt.json"
TOTAL_DELETED=$(jq '.total_entries_deleted' "$RECEIPT")
echo "✓ Phase 2 complete. Total entries deleted: $TOTAL_DELETED"

# Check final tips size
lastdb status | grep "tips="
```

## Troubleshooting

### Phase 1 Divergence Detected
**Symptom**: `DIVERGENCE DETECTED: N schemas differ`

**Action**: Phase 1 failed safely. Do NOT proceed to Phase 2.
1. Investigate the mismatches in `$PROOF_DIR/point-read-report.json`
2. Check if concurrent writes affected schema state during proof
3. Try Phase 1 again when load is lower

### Daemon Did Not Become Ready
**Symptom**: `isolated daemon did not become ready at ...`

**Action**: 
1. Check `$REPORT_DIR/lastdbd.err` for startup errors
2. Verify socket path is not too deep (< 103 bytes)
3. Ensure sufficient disk space for clone

### Backup Cut In Flight
**Symptom**: `Order-log compaction skipped: a backup cut is held`

**Action**: Phase 2 was blocked by a concurrent backup.
1. Wait for backup to complete
2. Re-run Phase 2 with same `RUN_ID`

## Verification Checklist

After Phase 2 completes:

- [ ] Phase 1 proof shows zero divergence
- [ ] Rollback point exists and is documented
- [ ] All batch receipts created and archived
- [ ] Tips plane reduced toward 1 GiB target
  - Check: `lastdb status | grep "tips="`
- [ ] Proof artifacts archived in `$PROOF_ROOT/$RUN_ID`

## Safety Guarantees

✓ **Primary not touched** until Phase 1 proves zero divergence  
✓ **Rollback point** written before first `--execute` deletion  
✓ **All reads preserved** on active schemas (verified in Phase 1)  
✓ **Live atoms retained** (only stale/superseded records deleted)  
✓ **No concurrent operations** (backup cut check)  
✓ **Batch status tracking** (batch progress logged and detectable on interruption)  

## Related Documentation

- Main runbook: `exemem_service/docs/tips-plane-reclaim-runbook.md`
- Retention policy: `brain get decision-2026-08-26-retention-windows-order-log-30d-versions-7d-fleet-ttl`
- Safe-upgrade discipline: `brain get concepts-lastdb-canonical-model`
- Dropped schema reap script: `scripts/lastdbd/dropped-schema-tips-reclaim-cow-audit.sh`

## Support

For issues or questions:
```bash
# Check the operational runbook
brain get concepts-lastdb-canonical-model

# Review decision context
brain get decision-2026-08-26-retention-windows-order-log-30d-versions-7d-fleet-ttl
brain get design-lastdb-keep-database-under-1gib

# Inspect proof artifacts
jq . $PROOF_DIR/phase-1-proof.json
jq . $PROOF_DIR/phase-2-receipt.json
cat $PROOF_DIR/reclaim.log
```
