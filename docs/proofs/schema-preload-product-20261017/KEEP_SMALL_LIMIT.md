# Existing limit for the storage meters

The extra 20,000-record run completes its data writes and its explicit flush. The normal stop then fails on the storage meters.
The node exits with code 1. No forced stop occurs.

The error is:

```text
shutdown: keep-small flush failed; live-budget meters may reopen stale
persist keep-small: Invalid operation: complete native batch exceeds 128 MiB
```

This limit also exists in the before engine, `aa64f61`. The current change does not change the 128 MiB guard or the meter format.
The guard remains active. This work does not change the storage meters.

| Evidence | Value |
| --- | --- |
| Program | Candidate 2, SHA256 `d7fe765461a2e8ab4ba8677ee0cb8407ea0caa87100fdc881d97710ce079dcee` |
| Generated records | 20,000; four hashes; 5,000 records per hash |
| Declared fields | `bucket`, `id`, `title`, `body`, `score` |
| Body size | 1,024 bytes |
| Range size | 54 API bytes; OPE on disk |
| Write batch | 64 records, followed by an explicit flush |
| Final accepted and completed numbers | 844 and 844 |
| Writer failure before stop | `false` |
| Core warm paths before stop | 0 |
| Process memory before stop | 1,650,769,920 bytes |
| Complete owned scratch root | 8,547,648 KiB; this includes the program, logs, and receipts |

The matched main cohort declares four fields: `bucket`, `id`, `title`, and `body`.
The extra field raises the declared field paths from 80,000 to 100,000 for the same record count.
The main cohort stops normally. The extra five-field cohort does not enter its first query, so it gives no evidence of publication during Loading.

The source path explains the limit. These paths start at `fold_db/crates/core/src/`:

- `db_operations/atom_store/keep_small/persist.rs:136` copies the complete meter state.
- `db_operations/keep_small.rs:64–77` puts all the tip sources and key sizes for one schema into one stored record.
- `db_operations/atom_store/keep_small/persist.rs:205–225` serializes the records and sends one native write batch.
- `storage/disk_pack/transaction.rs:245–267` rejects a complete native batch above 128 MiB.
- `fold_db_core/fold_db/lifecycle.rs:148–154` records the error and refuses a clean stop.

The persistence, schema split, and meter export files are unchanged from `aa64f61`. The native guard function is also unchanged.
`extra-loading-20000-candidate2/keep-small-source-audit.json` retains their hashes and the baseline identity.

A batch split between schemas alone does not bound a single large schema record.
A future fix needs bounded records and a final commit that cannot mark a partial meter snapshot as complete.
It also needs to restore dirty state after a failed write. The current path clears that state before the write.

`extra-loading-20000-candidate2/` retains the request receipts, input recipe, node log, size record, and exact cleanup records.
All data comes from a fresh generated home. This case does not use the primary or its archive.
