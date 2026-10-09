# Order-log retention CoW proof

This harness runs the order-log retention proof on a disposable clone. It does
not run a daemon on the primary home or use the primary socket.

Script:

```text
scripts/lastdbd/order-log-retention-cow-proof.sh
```

The harness does these actions:

1. It clones `PRIMARY_HOME` into `WORK_HOME`.
2. It moves the inherited `cloud_sync.json` into the durable paused state.
3. It starts `lastdbd --data-dir WORK_HOME` on the clone socket.
4. It calls `POST /api/sync/cloud-off` on that socket.
5. It reuses one stable validation schema and creates three fresh records.
6. It records a range-prefix order read under one hash and an exact point read.
7. It runs `lastdb db compact-order-log` in dry-run mode.
8. With `EXECUTE_COMPACTION=1`, it runs the logical reclaim.
9. It physically compacts `tips`, `field_update_order_log`, and
   `field_update_order_count`.
10. It proves the fresh order read and point read did not change.
11. It stops the clone daemon and removes the clone by default.

The order check uses a range under one hash. It does not use a schema scan.

## Fixture test

Run the fast test before a real-data job:

```bash
bash scripts/lastdbd/order-log-retention-cow-proof-test.sh
```

The test uses mock binaries and a disposable source directory. It proves that
the harness rejects `WORK_HOME=PRIMARY_HOME`, uses a unique socket, confirms
Cloud Sync is off, preserves read order, and removes the clone.

## Real-data job

Build the current branch binaries:

```bash
cargo build -p lastdb_node --bin lastdb --bin lastdbd
```

Use a short work root because macOS limits Unix socket path length:

```bash
PRIMARY_HOME="$HOME/.lastdb" \
WORK_ROOT=/private/tmp/olr \
EXECUTE_COMPACTION=1 \
LASTDB=target/debug/lastdb \
LASTDBD=target/debug/lastdbd \
scripts/lastdbd/order-log-retention-cow-proof.sh
```

Run this as a dedicated long job. Do not run it inside the pickup timebox. Do
not use `KEEP_WORK_HOME=1` unless an operator needs the failed clone for an
inspection.

## Output

The harness writes a detailed report under:

```text
~/.local/state/last-stack/order-log-retention-cow-proof/runs/<run-id>/report/
```

It writes the validation predicate file here:

```text
~/.local/state/last-stack/proofs/lastdb-order-log-retention-cow-proof-20260826.md
```

The first line is `PASS` only after logical and physical compaction finish.
The dry-run path writes `DRY_RUN`.

CAUTION: Never set `WORK_HOME` to `~/.lastdb`, `~/.folddb`, or a child of the
source home. The harness rejects these paths.
