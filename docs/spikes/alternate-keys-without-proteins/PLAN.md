# Alternate key implementation plan

This plan accompanies the [design spike](README.md).
The branch implements the Task slice, supported-home activation, resumable declaration changes, and CI comparisons.
Existing protein schemas stay usable. Their explicit correspondence conversion and final protein removal remain later stages.
Named pages use cursors. Counts and hash-scoped key lists use the named route.
Field conditions, mapped schemas, other database scopes, and compound primary keys remain outside this slice.
Signed peer import checks declaration revisions and derives entries from accepted winners.
Schema removal requires declaration removal and completed entry retirement first.
Tom authorizes a merge after the compatibility and CI checks pass (2026-10-29).
Proteins are deprecated for new app designs. App conversion follows [the migration plan](../../protein-deprecation.md).

## Source changes

Paths below use the repository root.
New modules are proposals. Existing paths identify the current request owners.

| Area | Current source | Planned change |
|---|---|---|
| Schema wire contract | `schema_service/crates/schema_types/src/declarative_schemas/`, `key_config.rs` | Add named declarations, validation, capability requirements, and a separate declaration digest. |
| Schema identity | `schema_service/crates/schema_types/src/declarative_schemas/identity.rs` | Preserve primary identity; do not accidentally remint it for each alternate generation. |
| Node schema and runtime | `fold_db/crates/core/src/schema/types/declarative_schemas/` | Resolve alternate molecule identities and profiles without a second runtime schema. |
| Schema declaration | `lastdb_node/src/exec/schema_declare/` | Preserve alternate metadata through proposal, reuse, alias resolution, and the service reply. |
| Key encoding | `fold_db/crates/core/src/atom/molecule_key_codec/`, `db_operations/molecule_key_store/` | Add a typed key-to-primary entry kind and independent range profiles. |
| Pure rules | Proposed `fold_db/crates/core/src/schema/alternate_keys.rs` | Derive and compare old and accepted keys; return typed changes without I/O. |
| Native mutation | `fold_db/crates/core/src/fold_db_core/mutation_manager/write/native.rs`, `molecules/prepare/`, `write/native_paths/` | Fetch dependencies in batches; stage entries in the existing transaction and clean publication. |
| Primary queries | `fold_db/crates/core/src/fold_db_core/query/query_executor/`, `query/hash_range_query/` | Select a named key, then batch-read primary fields in the captured root. |
| Public contract | `fold_db/crates/core/src/schema/types/operations/mod.rs`, `query_cursor.rs`; `lastdb_node/src/exec/query_mutation_routes.rs`, `query_batch.rs` | Add named-key selection, primary result keys, matched keys, and cursor checks. |
| SDK | `lastdb_app_sdk/src/types.ts`, `schemaMapping.ts` | Preserve the named key and both key forms through app aliases. |
| Native storage and memory | `fold_db/crates/core/src/storage/disk_pack/`, `resident/` | Cover entry records, references, byte charges, invalidation, and the home feature fence. |
| Lifecycle | `fold_db/crates/core/src/db_operations/atom_store/`, `fold_db_core/purge/` | Keep atom references correct; remove alternate entries with the primary key. |

Keep source files within 400 lines and functions within 100 lines.
Do not extract helpers that only forward arguments.
Keep the read sequence and write sequence visible in their current request owners.

## Stage 1 Contract and fresh data

Implement the proposed `Task` declaration with one primary and two named alternate keys.
Use existing schema service field identities and local app field aliases.
Accept ordinary owned fields, a Hash primary key, and scalar Hash or HashRange alternate keys.
Require each complete alternate key to contain the primary key field.
Leave unique aliases without that field and compound primary keys for a later explicit contract.

Create the new typed entry kind and its native molecule metadata.
Each alternate molecule receives the profile of its own range field.
The current schema profile assumes one layout; do not reuse it blindly for every alternate.
Advance the logical home format before a writer stores the first new entry.
Confirm that current older binaries refuse it before logical access.

Deliver create, partial update, key change, Delete, Purge, and primary CAS through one native transaction.
Use the accepted current field state after author-clock decisions.
Keep existing history, conflict, idempotency, retention, and capture effects.
Derived entries do not create independent logical mutations.

Exit condition: one write gives the same current fields through every declared key after a durable ack and a reopen.
The accepted field state explains every alternate entry.
This condition needs CI product results before any implementation claim.

## Stage 2 Queries and complete public behavior

Extend exact reads, range pages, counts, key lists, query batches, and watches.
Keep one captured root through alternate selection and primary field fetch.
Return the primary mutation key and the matched alternate key separately.
Retain typed range order, key conditions, page limits, and supported cursor behavior.

Keep the first public release within the owner's database scope.
Explicitly refuse alternate selection in received-data scopes.
Do not bypass consent, field grants, schema membership, or encryption scopes.
Received-data support needs a separate decision about which alternate entries a recipient can derive and disclose.

Inspect signed mutation capture, replay, import, and restore for schema-generation compatibility.
Rebuild derived entries from the accepted primary state on replay.
Require matching declarations before replay; refuse unknown generations.
A byte backup retains the matching schema definitions, primary fields, and entries at the same captured cut.

Exit condition: every supported public operation names its key layout explicitly or uses the primary default.
No alternate query falls back to an unrestricted read.
All unsupported scope and query combinations return explicit errors.

## Stage 3 Existing data and explicit conversion

Use a separate owner command for native homes that the current program supports.
Do not add a startup migration or restore support for retired home formats.
The spike adds a format activation command and a resumable declaration change route for standalone owned schemas.
It runs those commands only on generated CI homes. It does not access the primary.
The correspondence converter below remains necessary before existing protein relationships can retire.

The command requires:

- The authoritative source schema and its primary key.
- An explicit list of source record keys, or named-hash ranges with bounded pages and a complete caller-supplied hash list.
- The target alternate declarations and field-name maps.
- An explicit classification for each old schema: same record, same-key field alias, or unrelated records.
- A field winner decision for every divergence that prevents a lossless conversion.
- Exclusive ownership for the final conversion and cutover.

An arbitrary page or sample cannot establish completeness.
The operator must supply the complete authorized key domain.
The product still exposes no scan.
If the operator cannot establish complete coverage, refuse cutover.

Before a write, inspect the supplied records and report:

- Missing required keys or source records.
- Alternate collisions, duplicate old records, and unequal key types.
- Fields absent from the proposed single schema.
- Different current values, author clocks, metadata, or delete barriers.
- Unresolved field aliases, external schema references, shares, or app manifests.
- History, conflict, TTL, and source attribution that a direct remap cannot preserve.

Do not choose a winner from equal values or atom UUIDs.
Do not recreate atoms just to make the schema names match.
Retain selected atom identities and metadata when native contracts permit them.
Keep counted old references until the same transaction creates their accepted replacement references.

Start with an offline conversion and an explicit maintenance interval.
Chunk large copies into an unpublished alternate generation with a durable progress cursor.
Fence ordinary writes for the conversion; enforce existing resource budgets on each chunk.
Verify the supplied key domain, then publish the new schema generation and the caller cutover.
A crash before publication leaves the old generation authoritative and resumes or discards only private conversion work.
A crash after publication opens the new generation as a complete unit.

Update app manifests, retained schema references, retention policies, and schema service aliases before retirement.
Do not delete an old molecule while another schema or share still references it.
Keep old references for an explicit recovery interval.
New writes invalidate a simple switch back; recovery then needs an explicit reverse conversion or accepted backup.

Exit condition: the operator can account for every converted record and retained reference by supplied keys.
Every unresolved case blocks retirement rather than silently loses a relationship.

## Stage 4 Protein removal

Remove the following routes after conversion and caller cutover:

- `fold_db/crates/core/src/protein/`: definitions, membership, peer updates, and field protein joins.
- `schema/core/load/coherence.rs` and `schema/field_hash_coherence.rs`: automatic protein membership.
- `mutation_manager/molecules/protein_batch.rs`, `protein_key_fields.rs`, `protein_moves/`, and `persist/protein_delete.rs`.
- `fold_db_core/fold_db/protein_backfill/` and `fold_db_core/protein_reaper.rs`.
- `lastdb_node/src/exec/db_maintenance/protein_backfill.rs` and its route and CLI surfaces.
- Protein-specific `mref` edges, orphan cleanup, status counters, and attribution categories.
- Native prefix routes for `protein:`, `molprot:`, `fldprot:`, and `pfq:`.
- Protein terms in SDK contracts, public API descriptions, agent rules, schema declarations, and current design docs.

Preserve canonical field definitions and explicit valid same-key field mappers.
Cross-key mappers need conversion to an alternate key or an explicit record reference.
They must not activate the old peer write route.
Delete protein-only operational cases as the matching product behavior disappears; keep the accepted current operational scopes.

Audit replay, capture, backups, restore, admin inventory, molecule counters, and dropped-schema cleanup.
An empty source search alone does not prove data compatibility.
CI must build all consumers and exercise the public commands on supported converted homes.
The final current format contains no protein records or live references.

## CI product checks and latency comparison

The spike requires CI builds, operational results, and both latency comparisons.
Use CI for all future LastDB builds, operational checks, and measurements.
Use fresh generated homes and the accepted operational scope; do not copy the primary.
Do not add a general test suite or a fixture harness.

Required public behavior includes:

- Read the same Task by its primary, project, and owner keys.
- Update only a title; confirm both alternate entries remain unchanged.
- Change project; confirm the old key disappears and the new key returns the current fields.
- Omit owner, add owner, then set owner to null; confirm the declared sparse behavior.
- Refuse a required null key, an invalid type, a foreign cursor, and an unsupported scope.
- Refuse a CAS conflict without any entry change.
- Process repeated keys in one batch and an idempotent retry.
- Replay an older key-field value without a stale alternate entry.
- Delete, Purge, TTL, crash, and reopen without a partial record-to-key relation.
- Retain atom counts, history, conflict records, and valid file references.
- Stop and resume an explicit conversion without a premature generation publication.

Compare the eventual candidate with its exact parent source in CI.
Also compare equivalent logical records through the old multiple-schema route and the proposed one-schema route.
Use matching fields, payload sizes, request schedules, generated keys, and durable ack semantics.

Measure primary reads, alternate exact reads, range pages, field-only updates, key moves, batch insert, and deletes.
Include schemas with zero, one, and several alternates.
Keep mixed reads and writes active throughout each run.
Report P50, P95, P99, throughput, errors, physical reads, bytes written, disk size, and memory charges.
Include warm and cold logical-record cases and a controlled equal-home comparison where formats permit it.
Do not describe an incompatible layout as an identical physical control.

A repeat in balanced source order must investigate a material tail change.
Record exact source revisions, runner identity, valid request counts, and verified artifact uploads.
Keep raw output in CI artifacts and link the CI summary.
Retain `ci-required`, `benchmark-complete`, generated-evidence, and other applicable non-test gates.
The future merge owner reviews the latency tradeoff; this spike grants no merge permission.

## Design result

Proceed with the fresh-home Task slice after design acceptance.
Treat existing-data conversion and final protein removal as explicit later stages of the same replacement.
The design removes per-field peer propagation from alternate access.
Its main costs are an extra read hop, a new typed entry kind, and a controlled data conversion.
Keep the spike unmerged.
