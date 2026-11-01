# Record counter (retired)

A format 6 home kept one counter row for each record body.
The counter planner wrote the tips (`mk:`), the record bodies, the record counters
and the field key headers in one batch. `lastdb db verify-refs` checked them.

Record model plan #911 replaced that layout. A format 7 home stores one record
for each key (`rk:`). It has no tip, no record body and no record counter. This
release does not open a format 6 home.

What remains:

- The blob counter: `docs/lastdb-blob-counter.md`.
- The time-bucketed queue of the retention janitor and the blob dwell pass:
  `docs/lastdb-time-queue.md`.
- The row commit (`record_store/counter/commit.rs`). It releases record conflict
  rows, writes queue rows and runs guarded deletes in one batch.
- `lastdb db verify-refs`: the retention queue phase and the two blob phases.
- The writer boundary lint: `scripts/lints/lint-store-writers.py`.
