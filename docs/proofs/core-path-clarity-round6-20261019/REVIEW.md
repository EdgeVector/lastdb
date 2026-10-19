# Round 6 source review

Reviewed source: `b5b0018e64c2ad01931a684ad75a37237bc27f17`.

Result: no unresolved blocker in the reviewed routes. This document records source traces, not product proof. Separate operation receipts must establish the product result.

## Writes and deletes

- Scalar key conversion accepts strings, numbers, and booleans. The shared conversion now serves query layout checks and HashRange peer-delete coordinates.
- The general Delete peer-key check retains its refusal for null and structured values. Scalar support does not change that policy.
- Both user Delete forms retain tip-only removal. `must_exist` remains a caller precondition. Explicit Purge retains its separate body-removal checks.
- The removed `emit_derived_events` option had one initializer, always false. Its callback and `spawn_index_mutations` chain had no other caller.
- Active retention work and native after-commit Search delivery remain. The removal does not replace these active routes.

## Query key selection and sort values

The independent review found two source issues during this round. Condition-only fields could replace the caller's fallback field. A hidden sort field could also enter the caller's projection and change the selected keys. The final source fixes both issues.

- Pass A uses the unchanged caller projection for key selection. It loads condition and sort values for those keys.
- Pass B fetches the projection and the sort helper only after key selection. The helper cannot introduce another key.
- The host sorts with the captured helper value, then removes its value and metadata from the response.
- Raw core field maps can include key and sort helpers. The API comment states this contract. The query-slice caller exports only its requested fields.
- Key-only counts use the same preferred field and fallback choice as reads. Conditions and other body-dependent controls decline that count route.

The one-field fallback is intentional. An empty preferred key field permits one selected non-primary field to supply keys. Reads and counts do not union all projected field keys. A nonempty preferred field remains authoritative, including after the named-layout check removes its keys.

## Representation boundary

The reviewed routes use the canonical per-field model: schema, field, molecule, atom, then file. The optional whole-record compaction path is absent. Native file packing remains a disk responsibility; it does not introduce a second logical record representation.

This review did not run tests, builds, or product operations. It did not access the primary, the archive, or cloud data.
