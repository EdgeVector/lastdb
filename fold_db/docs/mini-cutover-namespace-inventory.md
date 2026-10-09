# Mini Cutover Namespace Inventory

This is the Phase 0 storage inventory for the Mini cutover. It records the
durable local Sled surfaces that `fold_db` opens today so the Last Store adapter
and migrator can be planned without rediscovery.

Source baseline: `origin/main` at `df72a3bc5` (merge of fold PR #666).

## Storage Stack

`SledPool` owns one Sled database path and lazily opens the underlying database
when a caller acquires a guard. The public `NamespacedStore` path is
`SledNamespacedStore`, whose `open_namespace(name)` maps the namespace name
directly to a Sled tree named `name`.

Primary source:

- `fold_db/crates/core/src/storage/sled/pool.rs:18` defines `SledPool`.
- `fold_db/crates/core/src/storage/sled/backend.rs:326` defines
  `SledNamespacedStore`.
- `fold_db/crates/core/src/storage/sled/backend.rs:351` maps
  `open_namespace(name)` to `SledKvStore::new(..., name.to_string())`.

## NamespacedStore Trees

These trees are opened through the storage trait boundary.

| Tree / namespace | Owner | Durable role | Notes |
| --- | --- | --- | --- |
| `main` | `AtomStore` | Canonical atoms, molecule records, tip history, and legacy mutation history. | Opened in `DbOperations` startup. |
| `metadata` | `MetadataStore` | Node-level metadata such as `node_id`. | Encrypted by the local store stack when at-rest crypto is active. |
| `schema_states` | `SchemaStore` | Schema availability/block state by schema name. | Sync-visible. |
| `schemas` | `SchemaStore` | Schema definitions by schema name. | Sync-visible. |
| `public_keys` | `PublicKeyStore` | System public-key record. | Sync-visible. |
| `idempotency` | `MetadataStore` | Mutation idempotency cache. | Listed as local-only by sync policy. |
| `schema_superseded_by` | `SchemaStore` | Supersession redirect map from old schema name to new schema name. | Opened as `superseded_by_kv`. |
| `schema_index` | `AtomStore` | Derived schema-to-atom secondary index. | Local-only and rebuildable from `main`. |
| `lineage_forward` | `LineageIndex` | Derived lineage index for source to derived molecules. | Local-only. |
| `lineage_reverse` | `LineageIndex` | Derived lineage index for derived to source molecules. | Local-only. |
| ~~`native_index`~~ | ~~`NativeIndexManager`~~ | **Retired 2026-08-05.** In-process native embeddings are no longer a Mini product collection. Residual cold-home keys (if any) are inventory-discoverable as Unknown, not a first-class plane/catalog entry. |

Primary source:

- `fold_db/crates/core/src/db_operations/core/mod.rs:83` opens all core
  namespaces listed above.
- `fold_db/crates/core/src/sync/policy.rs` marks `lineage_forward`,
  `lineage_reverse`, `schema_index`, `idempotency`, and
  `process_results` capture-skip (retired `native_index` is no longer listed).

## Sync-Internal NamespacedStore Trees

These are durable sync/capture bookkeeping trees and are intentionally skipped
by snapshot/capture export.

| Tree / namespace | Owner | Key prefixes / shape | Notes |
| --- | --- | --- | --- |
| `sync_outbox` | `SyncEngine` | `entry:{seq:020}` | Durable upload backlog. |
| `sync_capture` | `WatermarkStore` | `wm:{namespace}:{base64(key)}` | Store-derived export baseline fingerprints. |
| `sync_cursors` | sync cursor store | `cursor:*` | Persisted download cursors. |
| `sync_replay_quarantine` | replay transfer | `log:{prefix}:{seq:020}` | Tombstones for quarantined replay entries. |
| `sync_file_blob_known` | file blob sync | blob reference keys | Tracks known personal file blobs. |
| `sync_thumb_cache` | thumbnail sync | thumbnail hash keys | Local ciphertext cache for loose or packed thumbnails. |
| `sync_thumb_pack_index` | thumbnail sync | thumbnail hash keys | Points a thumbnail hash into a downloaded pack. |
| `__at_rest_strict_markers` | at-rest encryption | namespace marker keys | Tracks namespaces that have completed strict encrypted reads. |
| `__sled__default` | Sled | internal default tree | Listed so sync policy skips it if surfaced by `tree_names()`. |

Primary source:

- `fold_db/crates/core/src/sync/engine/types/bookkeeping.rs:6` defines
  `sync_outbox` and `entry:`.
- `fold_db/crates/core/src/sync/capture/watermark.rs:8` defines
  `sync_capture` and `wm:`.
- `fold_db/crates/core/src/sync/engine/wiring.rs:137` opens `sync_cursors`.
- `fold_db/crates/core/src/sync/engine/transfer/quarantine.rs:7` defines the
  replay quarantine key shape.
- `fold_db/crates/core/src/sync/engine/file_blob.rs:22` defines
  `sync_file_blob_known`.
- `fold_db/crates/core/src/sync/engine/thumb_pack.rs:13` defines
  `sync_thumb_cache` and `sync_thumb_pack_index`.
- `fold_db/crates/core/src/sync/policy.rs:13` lists sync-internal namespaces.

## `main` Key Prefixes

The `main` namespace contains user data plus several historical or derived
record shapes. Optional `storage_prefix` values are prepended as
`{prefix}:{base_key}` for share receive / historical org-scoped rows; readers
must use exact scoped prefixes and do not dual-read bare rows.

| Prefix / shape | Meaning | Source |
| --- | --- | --- |
| `atom:{uuid}` | Canonical atom JSON record. | `fold_db/crates/core/src/db_operations/atom_store/atoms.rs:14` |
| `mk:{M}:{esc(hash)}\0{range}` | Authoritative per-key molecule record for all field kinds. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:13` |
| `mh:{M}` | Molecule header record. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:17` |
| `tv:{version_id}` | Archived tip node in a per-slot tip-version chain. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:18` |
| `mo:{M}` | Legacy single-record HashRange update-order side record. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:46` |
| `mord:{M}:{seq}` | Append-only HashRange update-order log entry. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:51` |
| `moc:{M}` | Persisted HashRange update-order log count. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:54` |
| `mhr:{M}:{esc(range)}\0{esc(hash)}` | Range-major page-index marker for HashRange molecules. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:58` |
| `mhk:{M}:{esc(hash)}` | Hash-major uniqueness marker for HashRange fast path. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:64` |
| `mhi:{M}` | Completion marker for the HashRange page index. | `fold_db/crates/core/src/atom/molecule_key_codec.rs:69` |
| `history:{M}:{timestamp}` | Legacy mutation-event history. New as-of reads prefer the tip-version chain. | `fold_db/crates/core/src/atom/mutation_event.rs:6` |

Primary source:

- `fold_db/crates/core/src/schema/types/field/common.rs:98` describes scoped
  key construction and `build_storage_key`.
- `fold_db/crates/core/src/db_operations/atom_store/mod.rs:41` states that
  `AtomStore` is backed by `main`.
- `fold_db/crates/core/src/db_operations/atom_store/molecules/store.rs:18`
  writes per-key molecule records, tip versions, page-index markers, headers,
  and order-log entries into `main`.

## Other Key Prefixes By Namespace

| Namespace | Prefix / key shape | Meaning |
| --- | --- | --- |
| `schema_index` | `schemaidx:{schema}:{atom_uuid}` plus a backfill sentinel | Derived list-by-schema marker rows. |
| ~~`native_index`~~ | ~~`emb:…` / `graveyard:emb:…`~~ | **Retired** — historical embedding key shapes only; not a live product namespace. |
| `metadata` | `node_id` | Local node identifier. |
| `schemas` | `{schema_name}` | Declarative schema JSON. |
| `schema_states` | `{schema_name}` | Schema state enum. |
| `schema_superseded_by` | `{old_schema_name}` | Supersession target schema name. |
| `public_keys` | `SINGLE_PUBLIC_KEY_ID` | System public key. |
| `idempotency` | caller-supplied idempotency key | Mutation idempotency payload. |

Primary source:

- `fold_db/crates/core/src/db_operations/atom_store/atoms.rs:305` reads
  `schema_index`.
- `native_index` / `emb:` key shapes are historical only (product module removed).
- `fold_db/crates/core/src/db_operations/metadata_store.rs:59` stores
  `node_id`.
- `fold_db/crates/core/src/db_operations/schema_store.rs:60` stores schemas,
  states, and supersession rows by schema name.
- `fold_db/crates/core/src/db_operations/public_key_store.rs:37` stores the
  system public key.

## Direct `SledPool` Bypass Trees

These callers do not go through `NamespacedStore::open_namespace`; they acquire a
`SledPool` guard and open a Sled tree directly. A backend swap must either move
these behind the trait boundary or provide an explicit side-path adapter.

| Tree | Owner | Key prefixes / shape | Notes |
| --- | --- | --- | --- |
| `node_config` | `NodeConfigStore` | identity/config keys | Stores runtime identity; sensitive fields may be encrypted with the identity key. |
| `cas_blobs` | sharing blob CAS | `sha256:{hex}` blob refs | Stores verified content-addressed delivery blob payloads. |
| `share_rules` | sharing store | `share_rule:{rule_id}` | Local share rules. |
| `share_subscriptions` | sharing store | `share_sub:{sender_pubkey}` | Local receive subscriptions. |
| `share_delivery_outbox` | sharing store | `delivery:{id}`, `delivery_artifact:{id}` | Staged/pending delivery artifacts. |
| `org_sync_targets` | org sync target registry | `org_sync:{org_hash}` | Local org sync target registry including cloud prefix and E2E key material. |

Primary source:

- `fold_db/crates/core/src/storage/node_config_store.rs:5` defines the
  `node_config` tree and opens it directly at line 64.
- `fold_db/crates/core/src/sled_helpers.rs:10` centralizes direct
  `SledPool` tree opening for sharing helpers.
- `fold_db/crates/core/src/sharing/blob_cas.rs:12` defines `cas_blobs`.
- `fold_db/crates/core/src/sharing/store.rs:7` defines sharing rule,
  subscription, and delivery-outbox trees.
- `fold_db/crates/core/src/sharing/org_sync_target.rs:18` defines
  `org_sync_targets`.

## Migration Implications

1. The adapter can start at the `NamespacedStore` seam for the core trees and
   sync-internal trees, because current `SledNamespacedStore` already treats the
   namespace name as the storage collection name.
2. The first adapter should not change product defaults. It should preserve Sled
   as the default factory path until a separate factory-flag slice exists.
3. `main` is not a single logical table. The migrator must preserve exact key
   bytes and scoped-key behavior for `atom:`, `mk:`, `mh:`, `tv:`, `history:`,
   `mord:`, `moc:`, `mhr:`, `mhk:`, `mhi:`, and legacy `mo:` rows.
4. `schema_index`, `lineage_*`, and `idempotency` are local-only or derived by
   sync policy, but they are still durable local state. A cutover can choose
   rebuild-vs-copy per namespace, but that choice should be explicit. The
   retired `native_index` collection is no longer a product catalog entry.
5. Direct `SledPool` bypass trees are the highest-risk adapter gap. In
   particular, `node_config`, sharing delivery queues, blob CAS, and
   `org_sync_targets` hold state that is not reachable through the trait backend
   today.

