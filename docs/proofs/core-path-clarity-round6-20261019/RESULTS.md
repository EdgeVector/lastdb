# Round 6 results

**Final product completion: pending the fresh synthetic run.**

Final source: `c20b11e977e695efcd5ede080643a54c7b9728ea`.

## Motivation and changes

The review found separate numeric filter passes and inconsistent rules for scalar keys, sparse shared fields, and hidden sort values. The changes use one captured field query and one shared key conversion.

- Numeric conditions fetch their fields independently of the response projection. Unknown fields receive an explicit refusal. Conditions run before the result limit.
- Missing numeric values remain accepted. A concrete nonnumeric value, including null, fails its numeric comparison.
- Key discovery uses the unchanged caller projection. A hidden sort field cannot add keys. The host removes its helper value and metadata after the final sort.
- Reads and counts share one fallback rule. An empty key field permits one other selected field to supply keys, even when the projection includes the key field.
- Strings, numbers, and booleans use the same key conversion. Hash reads trust the lookup coordinates. HashRange layout checks use the actual declared key field.
- Counts decline field-dependent queries with `Ok(None)`. They do not present an unfiltered key count as the selected result count.
- The unused derived-event callback and its alternate dispatch chain are removed. Active retention work and Search delivery remain.

The current system uses schema, field, molecule, atom, then file. The optional whole-record copy and record-compaction route are absent. Native file packing remains part of the disk engine.

## Product failure and correction

The operation against `b5b0018e` passed 74 cohort reads and 26 typed query cases. It then failed `Round6ProjectionAbsent-exact-0`: the correct key and `id` arrived without the expected shared payload. The failure remains recorded in `ordinary-product/result.json` and the saved request receipts.

Concurrent schema declarations exposed a protein binding race. Separate binders could create different proteins and leave shared field members apart. Non-fatal bind errors allowed the declarations to return success.

The automatic binder selects a protein, creates it when needed, and adds both members behind one clone-shared gate. Direct member changes and orphan cleanup use the same gate. The lock order is native preparation, protein membership, then molecule liveness. Schema writes and deletes enter the native boundary before their liveness locks. Nested writes reuse that private transaction. A failure discards its staged changes together.

## Validation and limits

The required workspace and additional Clippy checks passed for the final source. Three boxed entry futures and one boxed lane dispatch retain the compiler's required type boundaries. They add no recursion-limit override or lint exemption. The normal release build passed. Independent checks matched all 1,876 source files, the executable, build settings, and frozen requests. The corrected product operation remains pending. No tests ran.

Raw core field maps can include key and sort helpers; response formatters remove the unprojected sort helper. `FieldQueryOptions` replaces the processor's long positional argument list. This changes its Rust call signature, not the wire query format.

The fallback uses one field, not a union of all sparse field keys. A nonempty key field remains authoritative after a layout check. Oversized orphan cleanup can refuse the whole operation at the existing caps. Existing distinct proteins remain distinct; this round adds no repair, migration, or backfill for a prior split.

The product operation uses fresh synthetic data. This round contains no throughput comparison or whole-process memory-cap measurement.
