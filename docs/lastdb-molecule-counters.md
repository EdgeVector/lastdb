# Molecule counters (retired)

A format 6 home kept three counters for each field molecule in a scope:

- `slots`: the tip records (`mk:`) of the molecule, in its header (`mh:`).
- `records`: the conflict records that named the molecule, in the same header.
- `bindings`: the schema and catalog bindings (`mref:`).

A molecule went to the drain only when all three counters were zero.

Record model plan #911 replaced that layout. A format 7 home stores one record
for each key (`rk:`) and has no tip, no molecule header and no binding record.
The key law refuses the `mk`, `mh` and `mref` families. API version 15 removed
the operator tools of these counters, and record model 8f removed the drain.

What remains:

- The blob counter: `docs/lastdb-blob-counter.md`.
- The conflicts of a record: `docs/operations/selected-key-conflict-metadata.md`.
- The retired drain: `docs/lastdb-molecule-drain.md`.
