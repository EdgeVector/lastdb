# Candidate measurement commands

Run these commands only after the candidate release executable is ready.
Record its SHA256 and source identity before the first command.
Use that executable for both the off and on cohorts.
Each command creates a new private synthetic home.
The ordinary full-path cap is 10,000.

## Preload off

```sh
python3 docs/proofs/schema-preload-product-20261017/measure.py \
  --binary /Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016/target/release/lastdbd \
  --variant after-off \
  --receipt-dir docs/proofs/schema-preload-product-20261017/after-off-main-256-candidate5 \
  --source-commit aa64f61-plus-candidate5-source-json \
  --env LASTDB_SCHEMA_PRELOAD_SCHEMAS= \
  --env LASTDB_SCHEMA_KEY_BYTES=134217728
```

## Preload on

```sh
python3 docs/proofs/schema-preload-product-20261017/measure.py \
  --binary /Users/tomtang/.fkanban/worktrees/lastdb-kanban-layer2-batch-storage-20261016/target/release/lastdbd \
  --variant after-on \
  --receipt-dir docs/proofs/schema-preload-product-20261017/after-on-main-256-candidate5 \
  --source-commit aa64f61-plus-candidate5-source-json \
  --env LASTDB_SCHEMA_PRELOAD_SCHEMAS=SchemaPreloadProduct \
  --env LASTDB_SCHEMA_KEY_BYTES=134217728
```

For 10,000 records, add `--records-per-hash 2500 --hashes 4 --reads-only`.
For 20,000 records, add `--records-per-hash 5000 --hashes 4 --reads-only`.
Use a different receipt directory for each cohort.
The driver preserves the exact executable hash in each input receipt.

## Separate key-budget refusal

Use the same 256-record request sequence with this policy:

```sh
--variant after-on-small-key-budget \
--receipt-dir docs/proofs/schema-preload-product-20261017/after-on-small-key-budget-256-candidate5 \
--env LASTDB_SCHEMA_PRELOAD_SCHEMAS=SchemaPreloadProduct \
--env LASTDB_SCHEMA_KEY_BYTES=4096
```

The small key budget remains separate from the 10,000-path cap.
The native status must report the actual domain state and refusal reason.
Repeated query observations must show whether the engine starts another complete key load.
Correct disk fallback values remain necessary.
Do not infer a successful complete key load from a correct read alone.
Do not combine these costs with normal-budget query times.

## Evidence limits

The first exact request, first range request, and concurrent first requests follow separate process starts.
Each first request can start a new complete key load in the selected schema.
Repeated requests in one process show memory key selection after a successful load.
The large reference cohorts can end at the separate capped-preview resource limit.
Preserve that result and the valid earlier query measurements.
Do not change the later request shape to conceal a refusal.
