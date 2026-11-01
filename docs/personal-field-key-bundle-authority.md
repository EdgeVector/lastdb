# Personal field key authority

> **Model note (2026-11-01).** Since home format 7 (the record model) a schema has no separate field-key rows. Key bundles (`mkb:{id}` in the stored namespace) exist for
> the record space, the field IDs and the alternate IDs of a schema. The
> [canonical model](lastdb-canonical-model.md) wins where this page differs.

Personal queries, counts, and mutation batches load existing field key bundles for each touched schema before they use field keys.
The exact schema metadata supplies the field key UUIDs.
This step reads bundle metadata and the node wrap. It does not fetch record values.

A missing bundle retains the legacy node key path.
A present bundle supplies the primary keys, with the existing legacy read fallback.
Corrupt metadata or a missing node wrap refuses the operation.
Personal writes do not create bundles or change record body encryption.
Named database writes retain the existing bundle creation path.

Personal batches load their addressed schemas before preconditions, prior-tip reads, and key plans.
The schema supplies its own field keys and named alternate keys.
Namespaces, access checks, count semantics, and warm set record rules retain their existing authority.

The source omission is established. A real cold personal query on accepted 857 returned zero for an expected document.
The document's physical key format and the cause of that observed zero remain unproved until an actual read with the changed source.
The change does not migrate data or repair duplicate slots.

Normal Delete uses the exact finite physical key list from the point reader.
The list includes both readable field key UUID spellings and the existing node-key fallback.
Each barrier retains the original mutation time, counter, device, and mutation UUID.
A newer tip at any candidate prevents a logical memory tombstone for that field.
Readers check each exact barrier before they select the first visible candidate. They keep the existing candidate order.

Delete convergence removes only the exact tips that their barriers block.
It uses each exact tip entry for history and reference edges.
It traces retained candidates under the same locks and keeps any shared history or exact reference edge.
It removes protected history keys before the existing path derives history backrefs.
The existing removal path retains its generation-delete and reference-count order.
It writes the normal physical field key header after the source deletes.

A physical header supplies only its version and update time for removal metadata.
If it is absent, the existing canonical header can supply those fields only when both UUID spellings name the same 32-byte identity.
The header read retains the exact storage prefix. A missing physical and canonical header refuses the operation.
Header metadata does not supply mutation time or device authority.
