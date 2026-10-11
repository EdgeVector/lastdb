# Personal molecule key authority

Personal queries, counts, and mutation batches load existing molecule key bundles for each touched schema before they use molecule keys.
The exact schema metadata supplies the molecule UUIDs.
This step reads bundle metadata and the node wrap. It does not fetch molecule records or atoms.

A missing bundle retains the legacy node key path.
A present bundle supplies the primary keys, with the existing legacy read fallback.
Corrupt metadata or a missing node wrap refuses the operation.
Personal writes do not create bundles or change atom body encryption.
Named database writes retain the existing bundle creation path.

Personal batches load their original schemas before protein Delete expansion reads source fields.
They then load any additional sibling schemas before preconditions, prior-tip reads, and key plans.
The change does not alter sibling selection, namespaces, access checks, count semantics, or the warm set record rules.

The source omission is established. A real cold personal query on accepted 857 returned zero for an expected document.
The document's physical key format and the cause of that observed zero remain unproved until an actual read with the changed source.
The change does not migrate data or repair duplicate slots.

Normal Delete uses the exact finite physical key list from the point reader.
The list includes both readable molecule UUID spellings and the existing node-key fallback.
Each barrier retains the original mutation time, counter, device, and mutation UUID.
A newer tip at any candidate prevents a logical memory tombstone for that field.
Readers check each exact barrier before they select the first visible candidate. They keep the existing candidate order.

Delete convergence removes only the exact tips that their barriers block.
It uses each exact tip entry for history and reference edges.
It traces retained candidates under the same locks and keeps any shared history or exact reference edge.
It removes protected history keys before the existing path derives history backrefs.
The existing removal path retains its generation-delete and reference-count order.
It writes the normal physical molecule header after the source deletes.

A physical header supplies only its version and update time for removal metadata.
If it is absent, the existing canonical header can supply those fields only when both UUID spellings name the same 32-byte identity.
The header read retains the exact storage prefix. A missing physical and canonical header refuses the operation.
Header metadata does not supply mutation time or device authority.
