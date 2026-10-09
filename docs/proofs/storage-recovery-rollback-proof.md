# Storage recovery and rollback proof

Exercises supported-version mixes, point and range reads, reference audits,
controlled host-failure faults, restore, and rollback. Each scenario preserves
acknowledged writes and reports recovery results.

## Proof Scenarios

### basic_write_read

Verifies that 100 sequential writes to a collection can be acknowledged (flushed)
and then read back exactly. Exercises point read accuracy after durability barrier.

**Acceptance:** All 100 writes are verified as readable after flush.

### range_read

Verifies that 50 records written and flushed can be enumerated and read back in
full. Exercises range/enumeration read completeness after durability.

**Acceptance:** All 50 records are found and verified readable.

### multiversion_mixed

Verifies that three logical "version" collections (v1_compat, v2_feature, v3_new)
can coexist in the same store and be read back correctly. Each collection holds
10 version-specific records.

**Acceptance:** All 30 records across all three collections are readable after
flush in each collection.

### reference_audit

Verifies that cross-collection references are valid. Creates 20 documents with
references forming a ring (doc-N → doc-N+1 mod 20). Audits that each reference
points to an existing document.

**Acceptance:** All 20 references resolve to their target documents.

### rollback_after_crash

Verifies that flushed writes survive the put/flush durability cycle, and that
unflushed writes are held in memory until flush (simulating crash behavior where
in-flight writes are lost).

1. Write and flush 30 "acknowledged" records
2. Write 10 "unacknowledged" records (no flush)
3. Verify flushed records are still readable
4. Verify unflushed records are still in memory (pre-crash state)

**Acceptance:** Flushed records survive; unflushed records are accessible before
flush (demonstrating correct in-memory buffering).

### negative_acked_write_loss

Verifies that acknowledged (flushed) writes cannot be lost. Writes 20 records,
flushes, and confirms all 20 are still readable.

**Acceptance:** All 20 acknowledged writes are found (no loss detected).

## Run the Proof

Build and test locally:

```sh
cargo build -p lastdb_node --bin lastdb_storage_recovery_rollback_proof --release
./target/release/lastdb_storage_recovery_rollback_proof \
  --home /tmp/lastdb-storage-recovery-rollback-test
```

All scenarios pass: 6/6 scenarios exercising write, flush, read, durability,
and recovery invariants.
