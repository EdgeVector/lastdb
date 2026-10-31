# Reader And Writer Inventory For Encrypted Range Keys

Source review: 2026-10-24, LastDB worktree revision `1e45b9a458dce2c80166a3b322f81293608c265b`.
This is the source of open PR 321, rather than a claim about the installed primary.
Code links below name that immutable source revision. Refresh each affected route against accepted main before its implementation.

The [OPE recommendation and implementation plan](ope-range-keys-implementation-plan.md) is the current proposal.
This inventory retains the earlier mandatory-preload impact descriptions; its source map remains useful for the OPE review.

The [design](encrypted-range-keys-local-preload.md) replaces stored range order with encrypted directories and local plaintext key trees.
The [work plan](encrypted-range-keys-local-preload-plan.md) defines the delivery sequence.

## Scope at a glance

This is a logical key-format change across reads, writes, membership controls, and recovery.
The principal risk is a mismatch between a new opaque tip identifier and another record that still names the old range key.
The local key-format change does not require a new pack engine.
The cloud and browser read path needs new per-atom encryption first (see the [design gap](encrypted-range-keys-local-preload.md#gap-per-atom-content-encryption)).

The static search finds 80 files with explicit range-codec, key-layout, generation, delete-barrier, or preload references.
That set contains 78 core Rust files and two source scripts.
These are review candidates, not a prediction of 80 edited files.
The tables also include indirect callers that do not contain those literal references.

The review follows codec calls, key constructors and parsers, preload gates, query selection, and the existing writer registry.
It also follows full-molecule operations, maintenance, reference counters, replay, and native recovery.
The writer registry itself notes that generic store helpers can hide keys supplied by callers.
This inventory therefore names both those helpers and their semantic callers.

| Area | Size of change | Main concern |
|---|---|---|
| Query selection and response keys | Substantial | Replace stored order and keyless decoding with verified local keys |
| Membership writers and controls | Substantial | Keep directories, tips, barriers, and generations coherent |
| Conflicts and retention | Substantial | Protect original key copies and preserve reference lifetimes |
| Notifications and local app delivery | Privacy review | Account for original keys outside atom content |
| Conversion and recovery | Substantial | Rewrite related identities and verify complete directories |
| Atom and file encryption | Integration check | Preserve existing encrypted content and stable atom references |
| Native pack engine | Expected reuse | Keep opaque exact keys and snapshot publication |
| Direct cloud and browser access | Later work | Add a logical read protocol and browser-safe client |

## Current cases without complete preload

| Case | Current behavior | Source |
|---|---|---|
| Schema is not selected | Skip preload; the process default selects no schemas | [Policy](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys.rs#L71) |
| Read occurs inside an active internal write batch | Skip the normal preload gate | [Schema gate](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys.rs#L160), [field gate](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/binding.rs) |
| Domain entry or memory reservation is unavailable | Continue without a ready complete domain | [Domain entry](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys.rs#L97), [load admission](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/load.rs#L73) |
| Load times out, exceeds limits, fails, or encounters unsupported mixed keys | Discard the incomplete domain and select `DiskOnly` | [Load failure](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/load.rs) |
| A later write exhausts key memory | Clear completeness and select `DiskOnly` | [Publication](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/publication.rs) |
| The query cannot use the ready domain at its captured revision | Use its compatible disk route | [Selection](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/selection.rs#L15) |
| A fallback or received field supplies another membership source | Load that domain when the source becomes necessary | [Fallback binding](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/binding.rs), [received fields](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/hash_range_query/shares.rs#L51) |
| A domain is purged, rebound, or receives incompatible controls | Clear readiness; a subsequent call can need another load | [Load lifetime](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/load.rs), [control publication](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/publication.rs) |

Writes alone do not require all range keys.
Hash-only schemas have no range-key preload requirement.
A selected schema initially loads its primary membership source, rather than every field molecule at once.
Fallback and shared sources have their own gates.
These facts prevent a claim that every current reader already sees a complete local range tree.

## Reader inventory

Change means the route needs new behavior or transaction integration under opaque range identifiers.
Check means the function can retain its purpose, but its format or publication integration requires verification.
An integration change can occur in a caller while a generic helper remains unchanged.

| ID | Reader | Impact | Source and required result |
|---|---|---|---|
| R01 | Query entry and schema gate | Change | [Query executor](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/query_executor/run.rs#L7), [other executor entry](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/query_executor.rs): require complete authorized domains for the new format. |
| R02 | Exact and batched tip reads | Change | [Exact query tips](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/schema_keys/query.rs), [visible tips](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/filter/per_key/load.rs), [batch reader](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/filter/per_key/batch.rs): derive or obtain opaque identifiers. |
| R03 | Ready range, prefix, and page selection | Change | [Window](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/schema_keys/hash_window.rs), [prefixes](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/schema_keys/prefix.rs), [pages](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/schema_keys/page.rs): compare plaintext local ranges and retain their opaque identifiers. |
| R04 | Disk range selection and stored-key pages | Change | [Hash selection](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/filter/hash_selection.rs), [partition pages](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/filter/partition_page.rs), [per-key bounds](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/filter/per_key/scan.rs): separate directory load from public range selection. |
| R05 | Typed field reads and original API keys | Change | [Field read](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/schema/types/field/variant/read.rs#L72), [collection](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/schema/types/field/variant/read/collect.rs), [stored-row recovery](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/schema/types/field/variant/read/resolve_scan.rs): recover original ranges from verified encrypted membership. |
| R06 | Primary source, fallback source, and record-envelope membership | Change | [Primary resolution](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/hash_range_query/resolve.rs), [schema-key listing](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/fold_db/admin_db/schema_keys.rs), [membership listing](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/filter/per_key/list.rs): preserve record existence and source precedence. |
| R07 | Secondary fields, copied fields, and received shares | Change | [Secondary reads](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/hash_range_query/secondary.rs), [received windows](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/hash_range_query/received_window.rs), [shared fields](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/query/hash_range_query/shares.rs): use each authorized molecule's compatible directory. |
| R08 | Full molecule and write-preparation reads | Change | [Molecule load](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/load.rs), [field refresh](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/schema/types/field/base.rs), [mutation preparation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/mutation_manager/molecules/restore.rs): keep API keys and storage identifiers distinct. |
| R09 | Conditional writes | Change | Preserve exact primary-key and expected-value checks before writes or erasures. |
| R10 | Conflict and retained-event reads | Change | [Conflict API](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/conflict_operations.rs), [event reads](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/atoms/history.rs): recover authorized display keys without readable original ranges on disk. |
| R11 | Retention candidates, TTL, and backfill reads | Change | [Retention backfill](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/fold_db/admin_db/retention_backfill.rs), [TTL erase checks](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/ops/ttl_sweep/erase.rs): preserve candidate identity and expiry checks. |
| R12 | Administrative key output and repair input | Change | [Key inventory](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/admin_db/inventory_molecule_keys.rs), [key-family inventory](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/admin_db/inventory_key_classes.rs), [key repair](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/fold_db/hashrange_key_repair.rs): distinguish opaque storage tokens from original API values. |
| R13 | Warm-path dependencies and invalidation | Change | [Tip dependencies](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/logical_paths/dependencies.rs), [controls](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/logical_paths/controls.rs), [source identities](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/resident/core_set/source_keys.rs): track new tip and barrier identities without false warm hits. |
| R14 | Recount and molecule drain readers | Change | [Recount](https://github.com/EdgeVector/lastdb/tree/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/counter/recount), [drain keys](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecule_drain/family.rs), [drain batches](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecule_drain/batch.rs): account for every authoritative directory and reference. |
| R15 | Backup and restored file-dependency proof | Change | [Metadata proof](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/file_dependencies/metadata.rs#L27), [generation selection](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/file_dependencies/metadata/generation.rs), [restore checks](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/native_cloud_restore/records.rs): prove selected metadata and complete directory dependencies. |
| R16 | Load-state and resource reporting | Check | [Schema-key status](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys.rs#L123), [key memory owner](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/resident/core_keys.rs): expose mandatory readiness and refusals without plaintext keys. |
| R17 | App change notifications | Change | [Feed reader](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/change_feed.rs), [authorized app route](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/exec/app_routes.rs): retain authorized key output while private stored fields use encryption. |
| R18 | Search rebuild and delivery consumers | Check | [Search rebuild](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/search_index/search_rebuild.rs): recover logical keys through the new directory and define the external app privacy boundary. |

The field bounds helper also requires a change: [storage bounds](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/schema/types/field/filter_utils/storage_bounds.rs).
It currently translates API range bounds into ordered stored bounds.
That operation has no equivalent under ordinary opaque lookup tokens.

## Writer inventory

| ID | Writer | Impact | Source and required result |
|---|---|---|---|
| W01 | Molecule keys and codec configuration | Change | [Key bundles](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/molecule_key_store.rs), [frozen bundle reader](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/molecule_key_store/frozen.rs), [codec](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/atom/molecule_key_codec.rs): version new key purposes and exact token creation. |
| W02 | Ordinary insert and overwrite | Change | [Mutation persistence](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/mutation_manager/molecules/persist.rs), [changed-key storage](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/store/changed_keys.rs), [store items](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/store/items.rs): commit membership and tips together. |
| W03 | Full molecule replacement and generation activation | Change | [Single records](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/store/single.rs), [generation preparation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/store/generation_activation.rs), [activation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/generation/activate.rs): replace ordered range suffixes and preserve the selected generation. |
| W04 | Delete, purge, and suppressed-write barriers | Change | [Deletion](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecules/delete.rs#L45), [delete barrier](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/atom/delete_barrier.rs), [purge targets](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/purge/delete_candidates.rs): bind each barrier to the same opaque key as its tip. |
| W05 | Named alternate address changes | Change | Publish old-address removal, new-address insertion, and primary values in one native request. |
| W06 | Conflict records and suppressed Put events | Change | [Conflict preparation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/conflict_record/mod.rs#L281), [record values](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/conflict_record/rows.rs#L47), [event writer](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/atoms/events.rs): protect original key copies and preserve retained references. |
| W07 | Conflict retention and expiry release | Change | [Retention keys](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/retention/keys.rs#L60), [slot release](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/retention/slots.rs), [release commit](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/retention/page.rs): rewrite slot identities without missed or duplicate releases. |
| W08 | TTL, retention backfill, and hard erasure | Change | [Backfill writer](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/schema_store/retention_backfill.rs), [TTL mutation batch](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/ops/ttl_sweep/send.rs), [purge job](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/mutation_manager/molecules/persist/purge_job.rs): remove membership with the corresponding tips and references. |
| W09 | Key repair and schema maintenance | Change | [Key repair](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/fold_db/hashrange_key_repair.rs), [repair entry](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/mutation_manager/aggregate/repair.rs), [administrative repair](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/fold_db/admin_db/repair.rs): use typed keys or verified directory identities. |
| W10 | Ready-domain publication and invalidation | Change | [Raw change publication](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/schema_keys/publication.rs), [typed native paths](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/fold_db_core/mutation_manager/write/native_paths.rs): update plaintext key trees from verified membership changes, rather than decode ranges from raw keys. |
| W11 | Tip, atom, and counter transaction planner | Change | [Planner](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/counter/build.rs), [commit](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/counter/commit.rs): include directory records in the same accepted change while preserving atom-reference rules. |
| W12 | Molecule drain and scoped cleanup | Change | [Drain batches](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecule_drain/batch.rs), [drain families](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/molecule_drain/family.rs): remove new directory state and respect active readers. |
| W13 | Explicit transfer conversion and format creation | Change | [Current conversion](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/convert_envelopes.rs#L19), [target preparation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/native_transfer_target.rs#L14), [format marker](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/key_format.rs): add a real key-identity conversion and final format publication. |
| W14 | Legacy sync replay and snapshot import | Change before support for the new format | [Replay dispatcher](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/sync/engine/replay/apply.rs#L23), [tip replay](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/sync/engine/replay/records.rs#L26), [snapshot import](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/sync/snapshot/restore_scan.rs): replay complete membership changes or refuse incompatible input. |
| W15 | Generic restored-record stores | Check | [Encrypted store](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/encrypting_store/kv_store_impl.rs), [native store](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/laststore/kv_store_impl.rs), [store trait](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/traits/kv.rs): preserve exact opaque bytes and route format validation correctly. |
| W16 | Meter repair, conflict annotations, and reference recount | Check | [Writer registry](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/scripts/lints/atom-counter-writers.allow), [offline recount](https://github.com/EdgeVector/lastdb/tree/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/counter/recount), [conflict annotations](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/conflict_index_heal.rs): preserve counters and avoid deletion of required new records. |
| W17 | Durable app change notifications | Change | [Event type](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/change_feed.rs#L27), [event writer](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/change_feed/native.rs), [host event creation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/exec/batch_mutation_routes.rs): protect original key fields without a second authority for product data. |
| W18 | Mutation capture, re-export, and sync selection | Change or explicit compatibility decision | [Capture policy](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/sync/policy.rs), [intent markers](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/sync/capture/worker/reexport.rs), [logical capture](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/sync/capture/write_path/logical_intent.rs): define directory replay and protect retained key information. |
| W19 | Local search inbox files | Check privacy scope | [Change payload](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/search_index/sink.rs), [JSON delivery](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/search_index/search_outbox.rs): account for plaintext logical keys in local app delivery. |

The writer registry confirms that ordinary tip changes converge on the counter planner.
That gives us one useful transaction boundary, but it does not remove the need to update its callers and control records.
Replay and generic restore functions also write through independent compatibility routes.
Their presence in source does not establish that a shared cloud service currently selects them.

## Records that require coordinated treatment

| Record | Current dependency | Required treatment |
|---|---|---|
| `mk:` tip | Its suffix holds the stored hash and range | Use the selected opaque exact-key layout |
| `mgr:v1:` generation record | Its suffix mirrors the tip suffix and order | Update generation readers, writers, and conversion together |
| `mgd:v1:` generation delete | Its suffix mirrors the deleted tip | Preserve the exact new identity |
| `mgp:v1:` generation pointer | Selects the complete immutable generation | Publish compatible directory state with the selected generation |
| `rdel:v2:` delete barrier | Its key and value contain the complete tip key | Rewrite both copies and every dependent parser |
| `crs:v1:` retention directory | Embeds the molecule's hash and range suffix | Use compatible opaque slot identities |
| Retained event and conflict values | Can contain original API field keys or display keys | Encrypt private key details or recover them through authorized encrypted metadata |
| Molecule key bundles | Hold the existing content, blind, and OPE keys | Version directory and token purposes and preserve access grants |
| Native logical paths and warm dependencies | Remember tip and barrier source identities | Replace sources consistently and invalidate incompatible memory |
| New directory root and chunks | Do not exist in the current format | Add complete publication, load, capture, restore, and removal rules |

The current envelope converter removes wrappers; it does not rewrite these related key identities.
It cannot supply this migration unchanged.

## Copies of original keys outside the range codec

The conflict preparation path can copy API hash and range values into `MutationEvent.field_key`.
The conflict API value can also contain a display key derived from those fields.
The current branch's storage policy does not add a general outer envelope to those values.
Encrypting only the directory therefore does not close this source path for original range values.

The source chain is [API-key capture](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/conflict_record/mod.rs#L281),
[event and conflict serialization](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/atom_store/conflict_record/rows.rs#L47),
and [the retained event type](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/atom/mutation_event.rs).
This is a source finding, rather than an observation of a record in the primary.

Durable app notifications are another concrete path.
`ChangeFeedEvent` includes the original hash and range, and its native writer serializes those fields as JSON.
The notification namespace receives no generic outer envelope under the current branch's CAS-only wrapper policy.
Its writer and authorized reader therefore need a stored-payload decision before a claim that all database range values remain encrypted.

The source chain is [host key capture](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_node/src/exec/batch_mutation_routes.rs#L160),
[the feed type](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/change_feed.rs#L27),
and [native feed persistence](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/db_operations/change_feed/native.rs).
Exclusion from logical capture does not, by itself, prove exclusion from a complete native byte backup.

The local search inbox writes JSON batches that include logical keys and selected field values.
That is an external app-delivery boundary, rather than an encrypted molecule directory.
Decide whether that explicit plaintext delivery remains permitted or requires a separate protected format.
Do not silently expand this project into a rewrite of LastSeek.

Mutation-intent re-export markers serialize envelopes after removal of some field values.
Review their remaining key fields, persistence, and native backup coverage before the new format's privacy claim.
This inventory does not establish that every sync configuration selects that path.

## Recovery and engine boundaries

| Boundary | Required work | Reuse opportunity |
|---|---|---|
| Logical format validation | Update accepted families, suffix rules, key bundles, and markers | Existing strict startup and restore refusals |
| Backup dependency capture | Include directory roots, chunks, and their complete source revision | Existing immutable native snapshot capture |
| Restore publication | Verify complete directories and compatible keys before destination publication | Existing inactive-home and key-proof guards |
| Compaction | Retain current directory roots, live chunks, and required references | Existing native pack compaction |
| Atom bodies | Preserve atom identifiers. A local atom row stays plain JSON inside an encrypted run-file block. The cloud read path must add per-atom encryption (see the [design gap](encrypted-range-keys-local-preload.md#gap-per-atom-content-encryption)) | [Body address](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/atom/atom_key_codec.rs#L30) depends on the atom UUID, separately from range keys |
| File blobs | Preserve encrypted file bytes and authenticated atom access | Existing file keys and encrypted-byte addresses |
| Native byte lookup | Store exact opaque identifiers and ordinary records | [Native key contract](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/vendor/laststore/src/disk_pack/home.rs#L34) already treats higher-level codecs separately |

The affected format readers include [current-key validation](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/current_keys.rs),
[physical routing](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/mini_cutover/main_keys.rs),
[restore guards](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/restore.rs),
and [native backup capture](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/fold_db/crates/core/src/storage/disk_pack/backup_capture/mod.rs).

## Client and cloud work after the local release

| Boundary | Current state | Planned work |
|---|---|---|
| Native CLI and app API | Send logical hash and range values | Preserve request shapes; handle mandatory preload and capacity errors |
| Node SDK | Uses Node transport and file APIs | Keep its node route compatible and add a separate browser-safe entry |
| Browser client | No direct directory read implementation in this source | Unlock keys, load complete directories, select locally, verify returned encrypted data |
| Cloud backup protocol | Addresses opaque backup objects | Preserve backup semantics; define logical revision and exact-read types separately |
| Private cloud service | Backup and shared-access code form existing integration boundaries | Add authorized directory and opaque key reads through the accepted shared protocol |
| Concurrent cloud updates | Require one coherent published revision | Define head authority, tip proofs, expired revisions, and refresh behavior |

The [SDK transport](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/lastdb_app_sdk/src/transport.ts) imports Node APIs.
The [byte protocol](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/cloud_backup_protocol/src/v2/mod.rs) supplies backup types, rather than a direct logical query service.
Private service work belongs in `EdgeVector/exemem-infra`, under `exemem_service/lambdas/storage_service/src/`.
These are later packages, rather than hidden prerequisites for the local directory format.

## Operational tools and required checks

The [range continuation workflow source](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/scripts/benchmarks/range_continuation/node.py) selects `plain` or `ope_v1` explicitly.
It requires a new-format mode and version-matched setup for comparisons.
The [counter-writer lint](https://github.com/EdgeVector/lastdb/blob/1e45b9a458dce2c80166a3b322f81293608c265b/scripts/lints/lint-atom-counter-writers.py) and its registry require a review when new directory writers enter the transaction boundary.
The raw writer `lastdb_node/src/bin/csync_e2e_write.rs` also needs a format review before use with the new format.
Native transfer and restore operational commands require matching format support.

For each Change row, record its implementation PR and actual product-command result before cutover.
For each Check row, record whether source changes prove necessary and which operational result covers the boundary.
Use CI for builds and measurements, with fresh synthetic DEV data.
Do not add tests, run local LastDB builds, use the primary, or copy the primary for this work.

The current inventory establishes source dependencies.
It does not establish runtime coverage for every reader and writer.
No existing route can silently retain an OPE or plaintext-range assumption when the new format becomes the default.

## Recommended first implementation boundary

Preserve the existing key-family framing and hash placement where possible.
One candidate keeps `mk:{M}:{escaped_blinded_hash}\0{opaque_range_token}` and replaces only the range segment's meaning.
Generation and barrier helpers that treat that suffix as opaque bytes can retain their structure.
Readers that compare ranges or recover their original values require new behavior.
All affected stored identities still require conversion, even when their helper functions remain unchanged.

Settle the exact tip identifier, directory membership, generation, delete-barrier, retention-slot, and key-bundle formats as one contract.
Include protection for original range values inside conflict, event, notification, and retained sync payloads.
Then update the shared planner and loader against that contract before a format cutover.
This boundary contains the largest local correctness risk and avoids a partial codec replacement.
