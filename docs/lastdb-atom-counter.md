# Atom counter (retired)

A format 6 home kept one counter row for each atom body (`atom:{uuid}\0c`).
The counter planner wrote the tips (`mk:`), the atom bodies, the atom counters
and the molecule headers in one batch. `lastdb db verify-refs` checked them.

Record model plan #911 replaced that layout. A format 7 home stores one record
for each key (`rk:`). It has no tip, no atom body and no atom counter. This
release does not open a format 6 home.

What remains:

- The blob counter: `docs/lastdb-blob-counter.md`.
- The time-bucketed queue of the retention janitor and the blob dwell pass:
  `docs/lastdb-time-queue.md`.
- The row commit (`atom_store/counter/commit.rs`). It releases record conflict
  rows, writes queue rows and runs guarded deletes in one batch.
- `lastdb db verify-refs`: the retention queue phase and the two blob phases.
- The writer boundary lint: `scripts/lints/lint-store-writers.py`.
