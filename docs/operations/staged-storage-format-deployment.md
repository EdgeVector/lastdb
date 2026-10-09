# Staged storage format deployment runbook

Kanban: `lastdb-storage-staged-deployment-runbook`

This runbook describes safe, staged deployment of LastDB storage format changes through read-first stages, bounded migration receipts, and compaction-space measurements. It is used for production deployment of storage changes such as collection reclassification, protein plane moves, and index restructuring.

The runbook applies to a storage format change that:

- Shifts or adds collection ownership for key prefixes
- Moves data across collections (e.g., tips residue copy, protein migrate)
- Changes read or write routing (dual-read windows)
- Reorganizes derived indexes or atom locators

Non-format changes (e.g., config tuning, metric additions) do not use this runbook.

## Scope

The runbook covers:

| Stage | Purpose |
|-------|---------|
| **P0 CoW Proof** | Isolated copy-on-write validation; format change code paths exercised |
| **P1 Read-first stage** | New code ships read-only, dual-read in effect, no writes to new locations yet |
| **P2 Migration receipt capture** | Measure compaction space, verify membership / key move integrity, receipt proof |
| **P3 Safe stop conditions** | Define gates where rollback is still reversible; no point-of-no-return without approval |
| **P4 Backup-cut preservation** | Rehearse backup capture with both old and new format present |
| **P5 Write path validation** | Enable writes to new locations; measure absence of data loss or drift |
| **Deferred: Primary validation** | Live aggregate metrics and soak windows — not included in this runbook |
| **Deferred: Live deployment** | Staged canary to production — not included in this runbook |

## Preconditions

- A new Mini release candidate with the format change code landed.
- A branch or tag pinpoints the exact commit under test.
- An isolated synthetic home directory (not the primary), prepared with pre-change data or a CoW of it.
- The candidate Mini binary (`lastdb`, `lastdbd`) can be built or fetched.
- Disk space available: **3× the largest collection** you plan to migrate or compact.
- The `lastdb` and `lastdbd` binaries on `PATH` (or named explicitly in commands below).
- Tools: `jq`, `du`, `sha256sum`, `grep`.

Do not test against the primary home. Use `CoW` (copy-on-write) or an isolated synthetic home prepared from backups.

## P0 CoW Proof — Read-first code paths on isolated data

Before any staged production deployment, run the format change on a CoW of real data to exercise code paths and measure safe stop points.

### 1. Prepare isolated home

```bash
set -euo pipefail

REAL_HOME="$HOME/.lastdb"
TEST_HOME="$TMPDIR/lastdb-format-test-$(date +%s)"
STORAGE_SOURCE="${REAL_HOME}/data"

# Copy-on-write the data directory (requires CoW fs support)
cp -c "$STORAGE_SOURCE" "$TEST_HOME/data"

# Single-user socket for isolation
mkdir -p "$TEST_HOME"
chmod 700 "$TEST_HOME"
export LASTDB_HOME="$TEST_HOME"
```

### 2. Start daemon with candidate release on CoW

```bash
# Verify no running daemon on this test home
ls "$LASTDB_HOME/data/folddb.sock" 2>/dev/null && \
  { echo "Socket exists; stop the daemon first"; exit 1; }

# Start the test daemon
lastdbd --log trace >"$TEST_HOME/daemon.log" 2>&1 &
DAEMON_PID=$!

# Wait for socket
timeout 30 bash -c "until [ -S '$LASTDB_HOME/data/folddb.sock' ]; do sleep 0.5; done"

printf 'Daemon PID %s\n' "$DAEMON_PID"
```

### 3. Exercise read-first code paths

Run the **read path** for the format change. This includes:

- Point gets on both old and new collection locations (dual-read if applicable)
- Scans of key prefixes involved in the format change
- Rehydrate of objects from cloud or cold storage (if applicable)
- Protein fold / molecule membership reads (if proteins involved)

Example for proteins plane arrival:

```bash
# Protein read (dual-read: proteins → tips during migrate)
lastdb protein list --limit 10 --json | jq '.proteins[].uuid'

# Molecule get by any field-hash protein
lastdb protein get <protein-uuid> --json | jq '.members[] | .molecule_uuid'

# Fold queue read
lastdb fold list --limit 5 --json | jq '.jobs[].key'

# Sample tip get (verify fallback reads work)
lastdb key get 'mk:<some-existing-tip-key>' --json
```

**Receipt:**  
Print the exact count of reads that succeeded and which prefixes were queried. Record the CLI version:

```bash
lastdb --version > "$TEST_HOME/read-receipt.txt"
echo "Succeeded: $(count of reads)" >> "$TEST_HOME/read-receipt.txt"
```

If any read fails, the code is not ready for P1 deploy. Stop here and fix.

### 4. Stop daemon and measure CoW usage

```bash
# Stop the test daemon
kill $DAEMON_PID 2>/dev/null || true
wait $DAEMON_PID 2>/dev/null || true

# Measure data directory size
du -sh "$TEST_HOME/data" | tee "$TEST_HOME/final-size.txt"

# Check for write amplification (CoW creates copies)
cp --verbose "$TEST_HOME/data" "$TEST_HOME/data.backup" 2>&1 | \
  grep -c "copy" | tee "$TEST_HOME/cow-write-count.txt"
```

**CoW proof requirement:** If the data dir grew **more than 2×** the original after the isolated test, investigate write-heavy code paths before production deployment.

## P1 Read-first stage — Canary without writes to new locations

Deploy the candidate binary in a canary environment or limited staging. The format change code is present, but **writes do not target new collections yet**. The read path uses dual-read (reads from both old and new locations, old location is authoritative).

### 1. Enable read-first mode in config

For collection moves or dual-read windows, set a feature flag or configuration:

```bash
# Hypothetical: if the binary supports a feature flag
export LASTDB_DUAL_READ_WINDOW=proteins  # enable proteins dual-read

# Or in the config file (syntax varies per change)
cat > "$LASTDB_HOME/.lastdb-config.toml" << 'EOF'
[storage]
dual_read_enabled = true
dual_read_collections = ["proteins", "tips"]
write_target = "tips"  # writes still go to tips, not proteins
EOF

lastdbd --config "$LASTDB_HOME/.lastdb-config.toml"
```

### 2. Run canary read workload

Execute production-like read operations (queries, mutations that read before write, protein scans):

```bash
# Example workload: 100 reads distributed across the format-change prefixes
for i in {1..100}; do
  lastdb query <sample-request-json> --json | jq '.status' || true
done

# Capture success rate
PASS=$(lastdb query <sample> --json 2>/dev/null | jq 'select(.status == "ok")' | wc -l)
TOTAL=100
echo "Read success: $PASS / $TOTAL" | tee "$TEST_HOME/p1-read-success.txt"
```

### 3. Measure dual-read performance

Dual-read may incur latency if the new collection location is not yet populated. Baseline the read path:

```bash
# Measure p99 latency of reads under dual-read
time lastdb query <query> --json 2>&1 | tail -1 | tee "$TEST_HOME/p1-latency.txt"

# Check logs for dual-read fallback frequency
grep -c "dual_read.*fallback" "$LASTDB_HOME/data/lastdbd.log" | \
  tee "$TEST_HOME/p1-fallback-count.txt"
```

**Safe stop condition for P1:**

- Read success rate ≥ 99.5% over the canary period.
- No latency increase > 10% compared to pre-change baseline.
- Fallback count remains near 100% (all keys are still in the legacy location; no migrations occur in P1).

If any condition fails, remain in P1 (dual-read only). Do not proceed to P2 until reads stabilize.

**Receipt to record:**
- Canary duration and workload scale
- Read success rate
- Latency baseline and comparison
- Fallback count (actual vs. expected)

## P2 Migration receipt — Compaction space and membership verification

Once reads are stable in P1, begin **bounded migration** of key groups. This stage measures compaction space, verifies no data loss, and produces migration receipts per key prefix.

### 1. Background copy-on-migrate (if applicable)

For moves like `tips → proteins`, use a background job to copy key groups:

```bash
# Example: copy protein keys from tips to proteins (background, non-blocking)
lastdb migrate --from tips --to proteins \
  --key-prefix "protein:" --key-prefix "molprot:" --key-prefix "fldprot:" \
  --key-prefix "pfq:" \
  --mode background \
  --snapshot > "$TEST_HOME/migrate-job-id.txt"

JOB_ID=$(cat "$TEST_HOME/migrate-job-id.txt")
echo "Migration job $JOB_ID started (background)"
```

### 2. Measure compaction space before migration

Before copying, snapshot the collections involved:

```bash
# Snapshot source and target sizes
du -sh "$LASTDB_HOME/data/data/tips" | awk '{print $1}' > "$TEST_HOME/tips-size-before.txt"
du -sh "$LASTDB_HOME/data/data/proteins" 2>/dev/null | awk '{print $1}' > "$TEST_HOME/proteins-size-before.txt" || echo "0" > "$TEST_HOME/proteins-size-before.txt"

TIPS_BEFORE=$(cat "$TEST_HOME/tips-size-before.txt")
PROTEINS_BEFORE=$(cat "$TEST_HOME/proteins-size-before.txt")

printf 'Compaction space before migrate:\n  tips: %s\n  proteins: %s\n' \
  "$TIPS_BEFORE" "$PROTEINS_BEFORE" | tee "$TEST_HOME/p2-space-before.txt"
```

### 3. Poll migration progress and wait for completion

```bash
# Poll job status every 10 seconds
while true; do
  STATUS=$(lastdb migrate status "$JOB_ID" --json | jq -r '.status')
  PROGRESS=$(lastdb migrate status "$JOB_ID" --json | jq '.progress.copied_keys')
  
  printf '[%s] Status: %s, Copied: %s\n' "$(date -u +%H:%M:%S)" "$STATUS" "$PROGRESS"
  
  if [ "$STATUS" = "complete" ]; then
    break
  fi
  
  sleep 10
done

echo "Migration complete at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### 4. Bounded receipt — key count and hash agreement

After migration copy, verify by sampling:

```bash
# Sample 100 keys from the migrated key prefix
lastdb key scan --prefix "protein:" --limit 100 --json > "$TEST_HOME/protein-sample.json"
MIGRATED_COUNT=$(jq '.keys | length' "$TEST_HOME/protein-sample.json")

printf 'Sampled %s protein: keys from new location\n' "$MIGRATED_COUNT" | \
  tee "$TEST_HOME/p2-migrated-count.txt"

# Hash-verify a sample of keys (fetch from both locations and compare)
jq -r '.keys[]' "$TEST_HOME/protein-sample.json" | while read KEY; do
  OLD=$(lastdb key get "$KEY" --source tips --json | jq -r '.content' | sha256sum)
  NEW=$(lastdb key get "$KEY" --source proteins --json | jq -r '.content' | sha256sum)
  
  if [ "$OLD" = "$NEW" ]; then
    echo "Key $KEY verified (hash match)"
  else
    echo "Key $KEY MISMATCH (data loss or corruption)"
    exit 1
  fi
done || exit 1
```

### 5. Measure compaction space after migration

```bash
# Re-snapshot after migration
du -sh "$LASTDB_HOME/data/data/tips" | awk '{print $1}' > "$TEST_HOME/tips-size-after.txt"
du -sh "$LASTDB_HOME/data/data/proteins" | awk '{print $1}' > "$TEST_HOME/proteins-size-after.txt"

TIPS_AFTER=$(cat "$TEST_HOME/tips-size-after.txt")
PROTEINS_AFTER=$(cat "$TEST_HOME/proteins-size-after.txt")

printf 'Compaction space after migrate:\n  tips: %s (was %s)\n  proteins: %s (was %s)\n' \
  "$TIPS_AFTER" "$TIPS_BEFORE" "$PROTEINS_AFTER" "$PROTEINS_BEFORE" | \
  tee "$TEST_HOME/p2-space-after.txt"
```

**Safe stop condition for P2:**

- Migration job completed without error.
- Sampled key hash agreement is 100% (no corruption).
- Protein collection exists and contains the expected key count.
- Dual-read test: get the same key from `proteins` and `tips` (if dual-read enabled); both return same content or dual-read fallback logic works.

**Receipt to record:**
- Migration job ID and start/end time
- Keys migrated (sampled count)
- Hash agreement verification (all sampled keys match)
- Compaction space before/after for each collection
- No errors or warnings in the migration log

## P3 Safe stop conditions — Rollback and point-of-no-return gates

Define explicit gates where the format change can be **rolled back without data loss**. After these gates, rollback becomes more complex and requires explicit data recovery steps.

### Gate 1: Backup capture with new format present

Before writes move to the new collection location, take a backup snapshot that includes both old and new locations. This backup is the **rollback point**.

```bash
# Initiate backup that includes all active collections
lastdb backup create --include-new-format --mode full \
  > "$TEST_HOME/backup-job-id.txt"

JOB_ID=$(cat "$TEST_HOME/backup-job-id.txt")
echo "Backup job $JOB_ID started"

# Poll for completion
timeout 600 bash -c "
  while true; do
    STATUS=\$(lastdb backup status \$JOB_ID --json | jq -r '.status')
    [ \"\$STATUS\" = \"complete\" ] && break
    sleep 5
  done
"

echo "Backup complete and can serve as rollback point"
```

### Gate 2: Write path readiness check (before enabling writes)

Before enabling writes to the new collection location, verify:

```bash
# Check that write target classification is correct
lastdb config show --json | jq '.write_target' | tee "$TEST_HOME/write-target.txt"

# Verify no orphan keys (get fails because write target changed but reads not updated)
# Run a test write to the new location and read it back
TEST_KEY="test:format-change:$(date +%s)"
TEST_VALUE='{"test": "data"}'

lastdb key put "$TEST_KEY" "$TEST_VALUE" --json
RETRIEVED=$(lastdb key get "$TEST_KEY" --json | jq -r '.content')

if [ "$RETRIEVED" = "$TEST_VALUE" ]; then
  echo "Write-read cycle OK: new location is writable and readable"
else
  echo "FAILED: Write-read cycle failed, new location not ready"
  exit 1
fi

# Clean up test key
lastdb key delete "$TEST_KEY"
```

**Safe stop condition for P3:**

- Backup completed and verified restorable (dry-run restore from backup).
- Write test passed: put and get on new location succeed.
- Configuration correct: write target points to new collection.
- No in-flight or pending migrations.

**Point-of-no-return marker:**  
Once writes are enabled on the new collection location (after this gate), **old collection location must not be used for new writes**. Old location becomes read-only for dual-read or fallback. Rollback after this point requires:

1. Reverse the write target classification.
2. Restore from the backup taken at Gate 1.
3. Replay any writes that landed on the new location after backup.

Record the timestamp of Gate 2 completion. Do not proceed to P4 without explicit approval from the change owner.

## P4 Backup-cut preservation — Rehearse write path with old format present

After enabling writes to the new location, verify that the **backup-cut captures both formats correctly** and that writes do not create orphans or loss.

### 1. Run production-like write workload

Execute mutations that write to the new collection location while old location is still present:

```bash
# Write 1000 keys across the migrated prefixes
for i in {1..1000}; do
  KEY="protein:test-$(printf '%04d' $i)"
  VALUE="{\"index\": $i}"
  
  lastdb key put "$KEY" "$VALUE" --json | jq -r '.uuid' | tee -a "$TEST_HOME/written-uuids.txt"
done

echo "Written 1000 test keys"
```

### 2. Snapshot backup during dual-format state

Capture a backup while both old and new formats are present:

```bash
lastdb backup create --mode full --include-dual-format \
  > "$TEST_HOME/dual-format-backup-id.txt"

BACKUP_ID=$(cat "$TEST_HOME/dual-format-backup-id.txt")

# Wait for completion
timeout 600 bash -c "
  while true; do
    S=\$(lastdb backup status \$BACKUP_ID --json | jq -r '.status')
    [ \"\$S\" = \"complete\" ] && break
    sleep 5
  done
"

echo "Dual-format backup complete at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

### 3. Restore from dual-format backup to verify fidelity

Use a second isolated test home to restore and verify:

```bash
TEST_HOME_2="$TMPDIR/lastdb-format-restore-$(date +%s)"
mkdir -p "$TEST_HOME_2"

lastdb backup restore "$BACKUP_ID" --home "$TEST_HOME_2" \
  > "$TEST_HOME_2/restore.log"

# Verify keys are restorable
lastdb -H "$TEST_HOME_2" key get 'protein:test-0001' --json | \
  jq -r '.content' | tee "$TEST_HOME_2/restored-key.txt"

# Count keys in restored home across all migrated prefixes
RESTORED_KEY_COUNT=0
for prefix in "protein:" "molprot:" "fldprot:" "pfq:"; do
  RESTORED_KEY_COUNT=$((RESTORED_KEY_COUNT + $(lastdb -H "$TEST_HOME_2" key scan --prefix "$prefix" --json | jq '.keys | length')))
done

ORIGINAL_KEY_COUNT=0
for prefix in "protein:" "molprot:" "fldprot:" "pfq:"; do
  ORIGINAL_KEY_COUNT=$((ORIGINAL_KEY_COUNT + $(lastdb key scan --prefix "$prefix" --json | jq '.keys | length')))
done

printf 'Restored key count: %s (original: %s)\n' \
  "$RESTORED_KEY_COUNT" "$ORIGINAL_KEY_COUNT" | \
  tee "$TEST_HOME_2/restore-receipt.txt"
```

**Safe stop condition for P4:**

- Write workload completed without errors.
- Backup captured both old and new formats.
- Restore fidelity check: restored key count ≥ original count (no loss).
- Sample key retrieval on restored home succeeds.
- No warnings or inconsistencies in backup/restore logs.

**Receipt to record:**
- Write workload: keys written and uuids generated
- Backup ID and timestamp (dual-format backup)
- Restore fidelity: original vs. restored key count
- Sample key verification (at least 3 keys checked)

## Deferred gates — Primary validation and live deployment

The following validation gates are **not included in this runbook**. They are performed in a later stage after the staged deployment completes:

### Deferred: Primary validation

**Condition:** Live production data and metrics aggregate over a soak window.

**Not included because:**
- Requires real production load and user workload.
- Metrics integration (observability, alerting) not yet defined for each format change.
- Soak duration varies per change (typically 3–7 days minimum).

**Deferred gate deliverables:**
- Aggregate latency metrics stable (p99 no increase > 5%).
- Error rate stable (no increase in 4xx, 5xx, or timeouts).
- Migration completion metric (dual_read.legacy_hits) trends to zero.
- No orphaned keys (cross-collection consistency checks).
- Backup/restore on production data succeeds.

**Owner:** Deployment team / SRE.

### Deferred: Live deployment

**Condition:** Canary deployment to production fleet (staged, with metrics gates).

**Not included because:**
- Requires fleet orchestration, canary promotion policy, and rollback automation.
- Rollback gates depend on live metrics dashboards.
- Deployment cadence and timing are infrastructure decisions, not format-change decisions.

**Deferred deployment deliverables:**
- Canary metrics green (validation gates met).
- Production fleet deployment in stages (e.g., 10% → 50% → 100%).
- Rollback plan documented and tested (not in this runbook).
- Post-deploy soak window (at least 24 hours).

**Owner:** Deployment infrastructure team.

## Codec disablement — Does not prove old-binary rollback

**Important:** If the format change involves adding, modifying, or removing a codec (encryption, compression, serialization format), **disabling the new codec in the binary does not enable rollback to the old binary.**

Reason: Keys written with the new codec are incompatible with the old binary's codec list. Even if the new codec is "optional," the old binary cannot read keys that used it.

**Safe rollback for codec changes requires:**

1. A data migration path: rewrite all new-codec keys back to the old codec **before** rolling back the binary.
2. Or: Maintain a dual-codec binary that can read both old and new formats, deploy it first, then separately upgrade to the new-codec-only binary.
3. Or: Use a backward-compatible codec negotiation scheme where the new codec is a fallback, not a replacement.

Consult the format change design document (referenced in the DECISION-CHECK section of the card) to determine rollback complexity for codec changes.

## Evidence to record in the PR or card

Record non-secret evidence from each stage:

| Stage | Evidence |
|-------|----------|
| **P0 CoW Proof** | CLI version, read success count, CoW write amplification |
| **P1 Read-first** | Canary workload scale, read success rate %, latency baseline, fallback count |
| **P2 Migration receipt** | Migration job ID, keys migrated (sampled), hash agreement result, space before/after |
| **P3 Safe stop** | Backup job ID, write test result (pass/fail), write target config, timestamp of point-of-no-return |
| **P4 Backup-cut** | Write workload scale, backup ID (dual-format), restore fidelity (key counts), sample key verification |

Never record raw API keys, database passphrases, authentication tokens, or presigned URLs. Record only the structure, counts, and verification results.

## Rollback procedure (if safe stop conditions fail)

If any safe stop condition is not met, **do not proceed to the next stage**. Instead:

1. Stop the test daemon: `kill $DAEMON_PID`.
2. Identify the failure (latency spike, data mismatch, migration error).
3. Review the format change code for the root cause.
4. If the code cannot be fixed, **downgrade to the previous binary** and restore from the last good backup (taken before the format change).
5. File a papercut or design review for the failure, citing the stage and receipt evidence.

## References

| Resource | Path / slug |
|----------|-------------|
| LastDB ideal storage shape | `docs/lastdb-ideal-storage-shape.md` |
| Backup and restore procedures | `docs/cloud-backup-gc-jobs.md` |
| Canonical model | `docs/lastdb-canonical-model.md` |
| Safe upgrade guide | `docs/lastdb-safe-upgrade.md` (if available) |

---

**This runbook is a template. Adapt the commands and stage names to the specific format change under test.**
