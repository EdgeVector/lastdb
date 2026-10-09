# BoardCards secondary field fetch spike

Date: 2026-10-01. Branch: `kanban/boardcards-parallel-field-fetch-spike-20261001`.

## Change

The current-head query reads secondary field slots in groups of at most four. The prior path reads one field at a time.
The default is one, so production keeps serial reads until an operator sets `LASTDB_QUERY_SECONDARY_CONCURRENCY` above one.
The variable selects 1 through 8. Invalid values and zero also select one; values above eight select eight.
An individual query can set `secondary_concurrency` to override the daemon value for that request.
The per-query value is also bounded to 1 through 8. Omit it to use the daemon default.
History queries keep the serial path.
The query still fetches atom bodies in one batch after the slot reads. The response format does not change.

## Small comparison

The test creates a temporary database with 20 rows and 21 fields. It queries one page of 20 rows.
It alternates concurrency levels on the same database. Each mode has 28 timed reads after four warm reads.
Each read checks every field count and checks one value per field.

| Concurrent field slot reads | Median query time | 95th percentile |
| ---: | ---: | ---: |
| 1 | 41.456 ms | 55.620 ms |
| 2 | 34.816 ms | 45.516 ms |
| 4 | 33.135 ms | 53.809 ms |
| 8 | 35.616 ms | 53.645 ms |

Four concurrent fields cut the median by 8.321 ms, or 20.1%, in this fixture. Two fields gave the lowest observed 95th percentile.
The sample is small, and the test runs on a development build with local storage. These numbers do not predict production throughput.

## Interpretation

Parallelism helps the field slot phase. It does not remove slot work, atom body reads, response bytes, or field metadata.
The code still holds the same final result in memory. It also holds pending results for up to four fields at once.
The change does not alter stored atoms or shared atom references. A single summary atom remains a separate design option.
That option could remove field metadata and read work, but it would change write behavior and the data model.

The test does not measure many concurrent board users, write contention, or a shared production database.
Use a bounded development-node test before a production decision. Compare query latency, throughput, memory, and `lastdb ops` work.
A broad query test did not finish before the host reached 0.5 GiB of free swap. The test stopped without a pass result.

## Reproduce

Run the opt-in test with:

```sh
cargo test -p fold_db --test parallel_secondary_spike -- --ignored --nocapture --test-threads=1
```

The test uses a temporary database. It does not read or restart the primary LastDB node.

## Production flag and trial

Set `secondary_concurrency: 4` on one query to opt in without a daemon reload.
Set `secondary_concurrency: 1` for its serial control. Omit the field to use the daemon default.
The daemon-wide flag remains available through `LASTDB_QUERY_SECONDARY_CONCURRENCY=4` in its supervised environment.
Unset the variable or set it to `1` to keep serial reads. A change to that environment needs a supervised daemon reload.
The `lastdb-safe-upgrade` path must test a merged release binary on a copy of the real database before any live binary change.

For a short live trial, time the same 20-row BoardCards query after the supervised cutover.
Alternate requests with per-query values of one and four on the same daemon.
Check row keys and title values on every read. Capture the median and 95th percentile, request-ops totals, daemon RSS, and timeout count.
Keep the query count small. Stop the trial by omitting the override if rows differ, timeouts increase, or memory reaches the live guard.
Use the supervised release path for any binary rollback.
