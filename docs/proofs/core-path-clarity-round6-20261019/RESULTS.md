# Round 6 results

**Final product completion: passed on fresh synthetic data.**

Final source: `7ce1056b71877534b30bb58a51891ecc0b1ca42a`.

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

The fresh operation against `c20b11e9` passed all 74 cohort reads and 84 query items before reopen. Three Hash count guards then returned HTTP 409 after a normal reopen. The nine HashRange items in the same batch returned the correct results. This failure remains recorded in `completion-product/result.json` and its separate sealed receipts.

The cold Hash loader found the stored tip but compared its encoded hash with the caller's original hash. The data remained on disk. Exact Hash selectors now use the existing batch reader, which retains the caller's hash and the selected physical tip. The obsolete decode route is removed. Counts and value reads use the same exact winner reader.

## Validation and limits

The required workspace and additional Clippy checks passed for the final source. Format, file size, function size, and added-lint-exemption checks also passed. Three boxed entry futures and one boxed lane dispatch retain the compiler's required type boundaries. They add no recursion-limit override or lint exemption. No tests ran.

The normal release build passed. Its executable SHA-256 is `44cbb843110030779eb38d2c75c5620f30043c4b0e4971fe6c010d2d79081196`. All 1,876 captured build inputs match the reviewed source. The compact source receipt reconstructs this inventory from the retained Round 5 inventory.

The final ordinary API exercise passed:

- 74 existing cohort read calls passed. All 29 strict warm read calls required zero file reads and passed.
- 100 static query items passed: 99 successful responses and one intended unknown-field refusal.
- The items cover scalar keys, conditions, projection, hidden sort values, sparse shared fields, and numeric and Boolean protein keys.
- A normal stop and reopen retained the values, counts, and atom identities. All three formerly failed cold Hash counts passed.
- Explicit Purge removed the target through both protein keys. The unrelated record remained correct before and after reopen.

The original operation inputs, hidden-sort extension, and nine existing helpers remained unchanged across the attempts. No executable exercise helper was added. `current-product/result.json` records the final result; `current-seal.json` records hashes for its nine evidence files. Both earlier failed results remain separate and unchanged. All five final node sessions stopped normally without force.

All three owned synthetic homes are removed through the supported exact-home cleanup. Independent reviews checked ownership, stopped processes, open files, sockets, and preserved evidence before each cleanup. All 27 sealed evidence files remain unchanged.

Raw core field maps can include key and sort helpers; response formatters remove the unprojected sort helper. `FieldQueryOptions` replaces the processor's long positional argument list. Schema write methods return boxed futures with the same awaited result. These changes affect Rust signatures. The wire query format stays the same.

The fallback uses one field, not a union of all sparse field keys. A nonempty key field remains authoritative after a layout check. Oversized orphan cleanup can refuse the whole operation at the existing caps. Existing distinct proteins remain distinct; this round adds no repair, migration, or backfill for a prior split.

The product operation uses fresh synthetic data. The primary and archive were not accessed. This round adds no cloud activation, migration, install, or upgrade. This round contains no throughput comparison or whole-process memory-cap measurement.
