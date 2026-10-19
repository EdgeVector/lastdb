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

## Independent binder follow-up at 003f2d58

Reviewed source: `003f2d5840197426c4c339d59e2159bea0154356`.

The later product operation at `b5b0018e` failed its first sparse peer check, `Round6ProjectionAbsent-exact-0`. The saved response contained the correct key and `id`, but no expected `payload`. The other five peer reads in the same batch showed the same omission. These requests had no numeric conditions or field ordering. The initial source review above did not cover concurrent protein creation.

The saved node log recorded non-fatal protein binding failures during concurrent schema declarations. The old binder read each molecule's protein reference, chose or created a protein, then added the two members through separate writes. Concurrent binders could choose different proteins and leave shared fields on separate proteins. The writer then updated only the members of its selected protein. The matching field definitions made the expected payload valid; this was not a weaker projection contract.

The seven-file correction preserves one membership authority:

- `AtomStore` clones share one membership gate. The automatic binder, public member addition, and orphan-protein cleanup use this gate.
- Native preparation precedes the membership gate. Molecule liveness locks follow the membership gate.
- The binder reads both references in one batch. It chooses the protein and adds both members inside the same native transaction.
- The binder calls the private member-add body, so it does not take its own membership gate twice.
- `DbOperations::store_schema` and `drop_schema` enter the native transaction before their molecule liveness locks. This removes the opposite lock order.
- Nested schema and raw writes reuse the existing private transaction. A new outer transaction captures the current root after native preparation.

The native preparation gate remains held until queue acceptance, after the inner membership and liveness guards leave scope. A failure discards the binder's staged members, protein body, and references together. An enclosing older read cannot supply the transaction's comparison state.

Orphan-protein cleanup now captures candidates and stages their removal inside the same boundary. An oversized cleanup can refuse the whole operation at the existing limits. It does not partially commit its chunks. Public protein creation alone still creates an empty protein until a member is added.

Distinct existing proteins remain distinct. The change adds no repair, migration, or backfill for a prior split. The next product operation uses a fresh synthetic home.

Result: no unresolved blocker in the final independent binder review. This follow-up used source and saved failure receipts only. It did not run tests, builds, or product calls. A separate operation against the corrected release must establish the final product result.

## Compiler boundary

The membership scope and the schema write/delete scopes return boxed futures. The lane dispatch also uses one boxed future. These four boundaries keep the compiler's type depth within its limit. Each call uses one box at its boundary; there is no nested box. They preserve the native transaction and lock order.

The required workspace Clippy check passes with these boundaries. The receipt is `validation/clippy-workspace-final-layout.txt` (exit 0). The saved failure without the lane box explains why that boundary remains. This compiler check does not establish the product result.

## Independent cold Hash follow-up at 7ce1056b

Reviewed source: `7ce1056b71877534b30bb58a51891ecc0b1ca42a`.

The saved `c20b11e9` operation passed the checks before normal stop. After normal reopen, three Hash count guards returned 409. The other nine HashRange queries returned correct results. Full workspace Clippy and the additional non-test lint checks pass for the corrected source.

The cold Hash loader found the stored tip but kept its encoded hash. The next filter compared that hash with the caller's API hash and found no key. The stored tips remained intact; this was a comparison failure. The warm path bypassed that loader, which explains the earlier pass. HashRange used API coordinates and retained its correct result.

The correction uses the existing exact batch reader for Hash selectors and removes the old `scan_1d_hash_slot` path. It adds no cache or count-only route. The independent source review confirms these rules:

- The loader keeps the caller's API hash and the canonical empty range.
- The exact reader records the actual winning physical tip before API projection. Later field projection preserves that selected atom location.
- The full entry, field metadata, writer metadata, and namespace scope remain unchanged.
- Captured writes, Delete barriers, and the existing tombstone checks still determine visibility.
- The selected-tip identity treats an absent range and an empty range equally. Hash projection therefore finds the same selected tip and file location.
- Range and page paths remain unchanged.

The final compiler boundary remains three direct `fn` to `BoxFuture` scope wrappers and one boxed lane dispatch. There is no recursion-limit override or nested experimental box.

Result: no unresolved blocker in this independent source review. This review used source and saved failure receipts; it did not run product calls. The fresh product result remains pending at this review. `RESULTS.md` records that separate outcome.

## Independent main integration review at 3dfcf8d6

Main advanced to `fadd60ab` during finalization. PR 221 removed the unused protein fold queue and its boot prefix read. The clean merge produced source `3dfcf8d658a7f9d8efdd55cc0635c0322e0ef88f`.

The independent interaction review found no blocker. The incoming change removes the unused queue methods and imports from `protein/ops.rs`. The public member-add wrapper, private member body, shared membership gate, and schema lock order remain intact. Current shared-field writes retain their existing same-request route.

The removed queue type, processor, prefix constant, and counter methods have no active Rust references. The remaining physical prefix classifications serve disk administration. The report field stays at zero for installed clients. It does not represent another write authority.

This integration review used source only. The required workspace and additional Clippy checks passed on the combined source. `RESULTS.md` records the separate release and product outcome for that source.
