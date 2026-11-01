# Molecule drain (retired)

A format 6 home kept the field molecules of a dropped schema (tips, headers and
bindings) until a background drain erased them. A schema install waited for the
drain of the same schema (`SchemaBeingDropped`).

Record model plan #911 replaced that layout. A format 7 home stores one record
for each key (`rk:`) and has no field molecule rows. A schema drop removes the
schema record and its catalog rows. The records of the schema stay. API
version 15 removed the `molecule-drain` route, and record model 8f removed the
drain.

The pacing of the background writers is in `docs/lastdb-time-queue.md`.
