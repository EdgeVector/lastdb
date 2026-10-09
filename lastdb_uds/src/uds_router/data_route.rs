// lint:file-size-ok moved verbatim out of a larger file; further splitting is follow-up work
use super::*;

/// Which socket a request arrived on — the dimension that decides whether the
/// owner-only control plane is even *reachable*.
///
/// Option B (the decided design): a jailed app is denied the node data dir, so
/// it cannot reach the **owner** socket (`<data_dir>/folddb.sock`) that serves
/// the mint verb. Instead the node binds a SECOND socket OUTSIDE the data dir,
/// the **app** socket, whose router serves ONLY the app data plane. The no-mint
/// guarantee is structural: on [`SocketKind::App`], the route for
/// `/control/browser-pairing-code` is classified [`ControlRoute::NotFound`] —
/// the mint verb is not in the table at all, so a jailed app physically cannot
/// reach it even though it can reach the socket. The owner socket keeps its full
/// behavior under [`SocketKind::Owner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketKind {
    /// The owner control socket inside the node data dir (`folddb.sock`). Serves
    /// the full surface: data routes, health, and the owner-only mint verb.
    Owner,
    /// The app data-plane socket bound outside the data dir, reachable by a
    /// jailed app. Serves ONLY the data routes + health; the owner control plane
    /// (the mint verb and anything else) is absent → [`ControlRoute::NotFound`].
    App,
}

/// An app-facing data route the control socket exposes.
///
/// Each variant maps to a `(method, path)` pair. The router recognizes the
/// route; the daemon's `execute_data` callback executes it against live state.
/// The variant carries only route identity, with no caller data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoute {
    /// `POST /api/query` — read query against the node's data.
    Query,
    /// `POST /api/mutation` — write mutation against the node's data.
    Mutation,
    /// `POST /api/aggregate/repair` — rebuild one invalid summary from its
    /// bounded member partition and return a core-minted repair token. OWNER
    /// socket only because repair scans and can rewrite the bounded partition.
    AggregateRepair,
    /// `POST /api/aggregate/finalize` — mark a verified aggregate summary
    /// valid if its source-partition guard token did not change. OWNER socket
    /// only because finalize certifies owner-controlled repair state.
    AggregateFinalize,
    /// `POST /api/mutations/batch` — write multiple mutations in one request.
    MutationBatch,
    /// `POST /api/queries/batch` — several independent `/api/query` bodies in one request (both sockets).
    QueryBatch,
    /// `GET /api/schemas` — list the node's available schemas.
    ListSchemas,
    /// `GET /api/list?schema=` — keys-only page of live record identities
    /// (hash + range). No atom bodies. Served on both sockets.
    ListRecordKeys,
    /// `GET /api/schema/{name}` — fetch one schema by canonical name/hash or
    /// descriptive name.
    GetSchema,
    /// `POST /api/schemas/declare` — owner direct namespaced schema
    /// declaration. OWNER socket only; the app socket uses
    /// `/api/apps/declare-schema` through the owner-mediated app mapping flow,
    /// never this direct-load route.
    DeclareSchema,
    /// `POST /api/schemas/seed-system` — install one `system_seed` schema on
    /// an explicitly isolated daemon for attribution and copy harness setup.
    ///
    /// OWNER socket only. The handler also requires both the request
    /// acknowledgement and `LASTDB_ISOLATED_COPY=1`, so a primary daemon
    /// cannot acquire a synthetic system root by accident.
    SeedSystemSchema,
    /// `POST /api/schemas/retire-name-claim` — retire (or restore) one
    /// installed schema's claim on its `descriptive_name`.
    ///
    /// The supported way to stop a rekey predecessor from answering name
    /// resolution. It does NOT change the schema, its identity hash, or its
    /// `SchemaState`, so every by-hash pin that deliberately addresses the
    /// predecessor keeps reading it — which is why this is not
    /// `SchemaState::Blocked`, whose redirect swallows by-hash lookups too.
    ///
    /// **OWNER socket only**: it edits the node's name-resolution catalog for
    /// every caller, so a jailed app must not reach it.
    RetireSchemaNameClaim,
    /// `POST /api/schemas/drop` — remove one installed schema identity, or
    /// every identity owned by one app, from the node catalog.
    ///
    /// ACK is catalog absence: `GET /api/schema/{name}` misses. Product
    /// tips and atoms stay until a later janitor. This is not
    /// `RetireSchemaNameClaim` (name resolution only) and not a product-row
    /// Delete.
    ///
    /// **OWNER socket only**.
    DropSchema,
    /// `GET /api/status` — stateful daemon status: uptime, sampler, process
    /// vitals, disk usage, and cloud-sync health.
    ///
    /// Default response is the **cheap health** picture (request-ops scalars
    /// only). Pass `?recent=1` or `?forensics=1` for the full forensic
    /// request-ops ring and ranking tables (`lastdb ops` does this).
    ///
    /// **OWNER socket only**: the route summarizes whole-node state and can
    /// include sync error text, so jailed app sockets must not reach it.
    Status,
    /// `GET /api/system/log-filter` — the tracing `EnvFilter` directive the
    /// daemon is currently logging under.
    ///
    /// **OWNER socket only**: log directives name internal module paths, and
    /// the companion setter is an operator control. A jailed app sees
    /// `NotFound` for both methods.
    LogFilterGet,
    /// `POST /api/system/log-filter` — swap the daemon's tracing `EnvFilter`
    /// at runtime.
    ///
    /// Exists so raising or lowering log verbosity on a live node costs one
    /// socket call instead of a restart. On the primary brain a restart is the
    /// one operation standing rules forbid, which made the safe quiet default
    /// unreachable in both directions — it could not be lowered to cut noise
    /// nor raised to chase a symptom.
    ///
    /// **OWNER socket only**, same rationale as
    /// [`LogFilterGet`](Self::LogFilterGet).
    LogFilterSet,
    /// `POST /api/admin/shed` — drain deferred writes, trim the warm set to
    /// the floor, and release free malloc pages. Never refuses a request.
    /// OWNER socket only.
    AdminShed,
    /// `GET /api/system/auto-identity` — the node's public key + derived user
    /// hash, the identity probe the loopback route serves unauthenticated.
    ///
    /// **OWNER socket only** ([`SocketKind::Owner`]): classified solely on the
    /// owner socket, so a jailed app on the [`SocketKind::App`] socket sees
    /// `NotFound` and can never read the node identity. Needs node state (the
    /// node's keypair), so it goes through the executor like the other data
    /// routes; it reaches the executor only after the owner is resolved, so the
    /// node is provisioned by construction. Added so the CLI startup preflight
    /// — which now prefers the socket like the request client — can reach the
    /// identity probe over the socket on a node with no reachable TCP listener.
    AutoIdentity,
    /// `GET /api/system/boot-identity` — the newest durable daemon boot row.
    /// This is the bounded canary identity probe. It does not build a status
    /// snapshot and is owner-socket-only for the same reason as AutoIdentity.
    BootIdentity,
    /// `GET /api/system/boot-ledger` — a bounded recent history of durable
    /// boot rows for stateless canary reconciliation. OWNER socket only.
    BootLedger,
    /// `GET /api/native-index/search` — owner-wide index search over schemas
    /// the node can query. OWNER socket only: this is a cross-namespace read.
    NativeIndexSearch,
    /// `GET /api/search/query` — first-party Search app text query. Served on
    /// BOTH sockets, but the executor requires a verified `search` app identity
    /// and scopes ranking to Search-owned schemas. This gives Search a
    /// host-mediated app route without exposing an owner-wide native-index
    /// search route to jailed apps.
    SearchAppQuery,
    /// `POST /api/app/search` — canonical app-owned text search. Served on
    /// BOTH sockets, but the executor treats it as an app-boundary route:
    /// a verified app identity is required even on the owner socket, and native
    /// ranking is scoped to schemas owned by that app. This is the app SDK's
    /// search surface and the companion to owner-only
    /// owner-wide native-index search route.
    AppSearch,
    /// `POST /api/native-index/embeddings` — upsert one app-supplied vector
    /// for a `(schema, key, field)` slot, tagged with the app's embedder
    /// identity. Served on BOTH sockets: unlike
    /// owner-wide native-index search this is a write into a
    /// named schema, not an owner-wide read, and the handler enforces that a
    /// verified app indexes only into its own schemas (write-own-namespace,
    /// exactly like mutation).
    NativeIndexAppVectorPut,
    /// `POST /api/native-index/knn` — bounded k-NN over app-supplied vectors,
    /// scoped to an explicit non-empty schema list plus one exact embedder
    /// identity. Served on BOTH sockets: the caller must name its scope (the
    /// ranking starts from the named schemas' pointer sets, so out-of-scope
    /// entries are structurally unreachable), which is precisely what
    /// distinguishes it from retired owner-wide cross-namespace search.
    NativeIndexKnn,
    /// `GET /api/history/{molecule_uuid}` — a molecule's mutation-event log
    /// (the multi-device "resolve in history" read surface: every branch of a
    /// concurrent write, conflict winners AND losers, is listed here).
    ///
    /// **OWNER socket only** ([`SocketKind::Owner`]), like
    /// [`AutoIdentity`](Self::AutoIdentity): history spans every record of the
    /// owning (schema, field) and discloses atom UUIDs, so a jailed app must
    /// never reach it; on the app socket the path is unrecognized → `NotFound`.
    MoleculeHistory,
    /// `GET /api/atom/{atom_uuid}` — a single atom's content by UUID (the
    /// terminal read in the history chain: hydrate a conflict loser's value).
    ///
    /// **OWNER socket only**, same rationale as
    /// [`MoleculeHistory`](Self::MoleculeHistory).
    AtomContent,
    /// `PUT /api/app/blob/cas/sha256/{hash}` — app-namespaced raw blob write.
    AppBlobPut,
    /// `GET /api/app/blob/cas/sha256/{hash}` — app-namespaced raw blob read.
    AppBlobGet,
    /// `PUT /api/app/org/{org_hash}/blob/cas/sha256/{hash}` — org-scoped,
    /// app-namespaced raw blob write.
    AppOrgBlobPut,
    /// `GET /api/app/org/{org_hash}/blob/cas/sha256/{hash}` — org-scoped,
    /// app-namespaced raw blob read.
    AppOrgBlobGet,
    /// `GET /api/app/blob/footprint` — local app namespace byte footprint.
    AppBlobFootprint,
    /// `GET /api/storage/app` — live storage grouped by `owner_app_id`, read
    /// from the keep-small projection. OWNER socket only: it reports across
    /// every app, so a jailed app must not see it. Bounded work — point reads
    /// of the write-path meters, never a store walk.
    AppStorage,
    /// `POST /api/storage/app/reconcile` — one bounded page of declared
    /// schema/key layouts that recovers unresolved owner attribution.
    /// OWNER socket only. Never a collection scan.
    AppStorageReconcile,
    /// `GET /api/storage/home` — the latest persisted unique physical ledger
    /// for the configured LastDB home. OWNER socket only. The read performs
    /// one metadata point-get and never inventories the filesystem.
    HomeStorage,
    /// `POST /api/storage/home/reconcile` — process one bounded page of the
    /// configured home inventory and persist its resume cursor. OWNER only.
    HomeStorageReconcile,
    /// `POST /api/org/sync/register` — register an org cloud-sync target
    /// (org_hash + E2E key). OWNER socket only.
    OrgSyncRegister,
    /// `GET /api/org/sync/targets` — list registered org cloud-sync targets.
    /// OWNER socket only.
    OrgSyncTargets,
    /// `POST /api/org/sync/deactivate` — disarm registered org cloud-sync
    /// targets by `org_hash` and/or `slug` (local registry only; no cloud
    /// call). `dry_run` lists without writing. OWNER socket only.
    OrgSyncDeactivate,
    /// `POST /api/org/sync/grant-member` — owner grants another principal
    /// writer/reader on the org cloud head (Exemem registry). OWNER only.
    OrgSyncGrantMember,
    /// `POST /api/org/sync/revoke-member` — owner kicks or principal leaves
    /// the org cloud head (Exemem registry). OWNER only.
    OrgSyncRevokeMember,
    /// `GET /api/db/catalog` — point-get one `(db_locator, schema_name)`
    /// membership row. OWNER socket only. Never enumerates the catalog.
    DbCatalogGet,
    /// `POST`/`PUT` `/api/db/catalog` — insert or replace one membership row.
    /// OWNER socket only. Point put; no scan.
    DbCatalogPut,
    /// `DELETE /api/db/catalog` — delete one exact membership row.
    /// OWNER socket only.
    DbCatalogDelete,
    /// `POST /api/db/catalog/share` — zero-copy share: catalog entry reuses
    /// the source instance_id and grants target-domain key wraps.
    /// OWNER socket only.
    DbCatalogShare,
    /// `POST /api/db/catalog/reclaim` — drop leftover `{64hex}:` copy-geometry
    /// rows whose prefix is not a live catalog instance. OWNER socket only.
    DbCatalogReclaim,
    /// `POST /api/sync/heal-staging` — live cloud snapshot + clear upload
    /// staging (OWNER only). Never stops Mini; concurrent local R/W continue.
    SyncHealStaging,
    /// `POST /api/sync/laststore-snapshot` — LastStore manifest/chunk backup
    /// snapshot commit (OWNER only). Uploads missing chunks + manifest, CASes
    /// backup/latest, then advances local backup state.
    SyncLastStoreSnapshot,
    /// `POST /api/sync/backup-gc` — product-path orphan sweep of unreferenced
    /// cloud backup chunks (OWNER only). Body: `{ dry_run?: bool }`.
    SyncBackupGc,
    /// `POST /api/sync/prefix-inventory` — read-only R2 prefix/category size
    /// breakdown for the connected cloud account (OWNER only). No body. Never
    /// deletes and never fetches an object body.
    SyncPrefixInventory,
    /// `POST /api/sync/cloud-off` — intentional Cloud Sync pause: stamp grace
    /// clock on the live engine + rename `cloud_sync.json` → `.paused` for
    /// durable reboot intent. OWNER only. Local R/W never blocked.
    SyncCloudOff,
    /// `POST /api/sync/cloud-on` — restore `cloud_sync.json` if paused, then
    /// re-enable live engine (pull→snapshot when past grace). OWNER only.
    SyncCloudOn,
    /// `POST /api/sync/cloud-resume-primary` — durable owner job or status.
    SyncCloudResumePrimary,
    /// `POST /api/sync/quarantine-replay-blocker` — clear the exact active
    /// `replay_blocker` (target+seq required). Corrupt entries delete the cloud
    /// object; apply-failed pins skip locally without deleting. OWNER only.
    SyncQuarantineReplayBlocker,
    /// `POST /api/sync/backup-concurrency` — get, set, or clear the live
    /// sealed-home backup PUT concurrency override. OWNER only.
    SyncBackupConcurrency,
    /// `GET /api/db/inventory` — live main-tree key-class + per-schema atom/history
    /// byte breakdown. OWNER socket only.
    ///
    /// Not a passive read: this route runs the bounded schema/system/retention
    /// root walks and durably writes their attribution rows before it
    /// summarizes the ledger, so a GET here has real (idempotent, catalog-sized)
    /// write side effects.
    DbInventory,
    /// `GET /api/db/schemas` — atom-only per-schema logical storage.
    /// OWNER socket only. Cheaper than [`Self::DbInventory`] (no history /
    /// order-log / tip-format walks) but still a heavy admin scan.
    DbSchemas,
    /// `POST /api/storage/schema` — bounded logical-current storage from the
    /// schema catalog and its declared molecule counters. OWNER socket only.
    SchemaStorage,
    /// `GET /api/storage/schemas` — labelled logical-current storage for all
    /// installed schemas from catalog metadata and molecule counters.
    /// OWNER socket only; this route never scans atoms.
    SchemaStorageReport,
    /// `POST /api/storage/liveness/explain` — point-read one target's active
    /// reverse-edge partition and fail-closed completeness state.
    LivenessExplain,
    /// `POST /api/storage/liveness/bootstrap` — rebuild local derived edges
    /// on a daemon started for an isolated copy. OWNER socket only.
    LivenessBootstrap,
    /// `POST /api/db/clear-history` — trim or purge mutation-history rows.
    /// OWNER socket only. Body: `{schema?, keep_last?, dry_run?}`.
    /// `keep_last: 0` = full purge (latest tip only).
    DbClearHistory,
    /// `POST /api/db/compact` — compact one allowlisted LastStore collection
    /// (`schemas`, `schema_states`, `schema_index`). OWNER only. Body:
    /// `{ collection: string, dry_run?: bool }` (default dry_run=true).
    /// Execute skips while a backup cut is held.
    DbCompact,
    /// `POST /api/db/stamp-purged-atom-retirements` — stamp committed atom
    /// successor-history SHAs into pending purged retirements, or report
    /// would-retire counts. OWNER only. Body: `{ dry_run?: bool }` (default true).
    /// Execute skips while a backup cut is held. Never stamps missing groups.
    DbStampPurgedAtomRetirements,
    /// `POST /api/db/compact-record` — zip one HashRange key onto the schema's
    /// record molecule R. OWNER socket only. Body:
    /// `{ schema, hash, range }`. Not collection compact (`/api/db/compact`).
    CompactRecord,
    /// `POST /api/db/purge-schemaidx` — delete retired `schemaidx:` full-atom
    /// copies (and sentinel). OWNER socket only.
    DbPurgeSchemaIdx,
    /// `POST /api/db/gc-atoms` — prune tombstoned tip-version chains, then
    /// delete unreferenced `atom:` rows. Body:
    /// `{ dry_run?: bool }` (default true).
    DbGcAtoms,
    /// `POST /api/db/reap-dropped-schema` — reap live tips of one dropped
    /// schema identity via schema_index / drop-receipt molecules. Never scans
    /// `atom:`. OWNER only. Body: `{ schema, dry_run?: bool, max_ops?: u64 }`.
    DbReapDroppedSchema,
    /// `POST /api/db/gc-file-blobs` — delete local file-blob rows
    /// (`cas_blobs` + resident `cas_blob:`) no live atom references; the
    /// reclaim path for sealed bytes a purge orphans. OWNER socket only.
    /// Body: `{ dry_run?: bool }` (default true).
    DbGcFileBlobs,
    /// `POST /api/db/gc-proteins` — delete empty, unbound `protein:` rows.
    /// OWNER socket only. Body: `{ dry_run?: bool }` (default true).
    DbGcProteins,
    /// `POST /api/db/purge-ref-blobs` — measure (and optionally delete) legacy
    /// `ref:` whole-molecule blobs (pre-per-key layout residue). OWNER socket
    /// only. Body: `{ dry_run?: bool }` (default true).
    DbPurgeRefBlobs,
    /// `POST /api/db/reclaim-keep-small-legacy` — drop the dead `metadata`
    /// hash group that held the keep-small snapshot before 2026-09-21,
    /// without loading it. OWNER socket only. Body: `{ dry_run?: bool }`
    /// (default true).
    DbReclaimKeepSmallLegacy,
    /// `POST /api/db/reclaim-keep-small-snapshot` — drop the current
    /// `keep_small` hash group when it holds only the rebuildable
    /// `keep_small:meters` row, without loading it. OWNER socket only.
    /// Body: `{ dry_run?: bool }` (default true).
    DbReclaimKeepSmallSnapshot,
    /// `POST /api/db/repair-dangling-tips` — remove live `mk:` tips whose atom
    /// body is unreachable by every reader route. OWNER only. Body:
    /// `{ dry_run?: bool, max_ops?: u64, tip_page?: u64, audit_unresolved?: u64 }`.
    DbRepairDanglingTips,
    /// `GET /api/db/unresolved-atoms` — the bounded atom identities behind
    /// skipped reads. Owner socket only; keys can name user data.
    DbUnresolvedAtoms,
    /// `POST /api/db/drain-tip-history` — one bounded tip-version (`tv:`) chain
    /// drain pass for legacy live history (opt-in reclaim; never collection
    /// compact). OWNER only. Body:
    /// `{ dry_run?: bool, max_keys?: usize, max_prunes?: usize,
    ///    after_key?: string, from_checkpoint?: bool }`.
    DbDrainTipHistory,
    /// `POST /api/db/retain-superseded-versions` — drop `tv:` / tip-chain nodes
    /// older than 7 days on **live** records only. Tombstoned heads are skipped.
    /// OWNER only. Dry-run default. Execute skips while a backup cut is held.
    /// Body: `{ dry_run?: bool, max_keys?: usize, max_prunes?: usize,
    ///    after_key?: string, from_checkpoint?: bool, retention_seconds?: u64 }`.
    DbRetainSupersededVersions,
    /// `POST /api/db/probe-locator-only` — bounded sample of tips reachable only
    /// via `aloc:` locator (not tip-derived/flat). OWNER only. Body:
    /// `{ max_tips?: u64, tip_page?: u64 }` (default max_tips=512). Always read-only.
    DbProbeLocatorOnly,
    /// `GET /api/db/delete-ledger` — read the durable atom hard-delete audit
    /// trail (purge + gc-atoms), oldest first. Query: `?limit=N` (0/absent =
    /// all). Read-only; OWNER socket only, because it names which schemas had
    /// records erased.
    DbDeleteLedger,
    /// `POST /api/db/migrate-photo-blobs` — move Photo.file_bytes into cas_blobs.
    /// Body: `{ dry_run?: bool }` (default true).
    DbMigratePhotoBlobs,
    /// `POST /api/db/migrate-thin-tips` — rewrite fat `mk:` tip values to thin.
    /// Body: `{ dry_run?: bool }` (default true).
    DbMigrateThinTips,
    /// `POST /api/db/rekey-atom-partition-prefix` — dual-write atom bodies to
    /// partition-prefixed keys (+ locators). Body:
    /// `{ dry_run?: bool, remove_flat?: bool, max_ops?: u64 }` (dry_run default true).
    DbRekeyAtomPartitionPrefix,
    /// `POST /api/db/reseal-at-rest` — rewrite existing `ENC:` values in one
    /// sealed plane to `ENB:` (+ deflate ≥256 B) under the same key.
    /// OWNER only. Body: `{ collection, dry_run?: bool, max_rows?: usize,
    /// max_secs?: u64, progress_only?: bool, restart?: bool }` (dry_run default true).
    DbResealAtRest,
    /// `POST /api/db/reap-unsealed` — remove un-enveloped rows from one
    /// encrypted plane; they already read as absent. OWNER only. Body:
    /// `{ collection, dry_run?: bool, max_rows?: usize, max_secs?: u64,
    /// progress_only?: bool, restart?: bool }` (dry_run default true).
    DbReapUnsealed,
    /// `POST /api/db/tombstone-flag-audit` — count `mk:` records whose
    /// `meta.tombstoned` disagrees with their atom content, and optionally stamp
    /// the flag onto them. Body: `{ schema?: string, dry_run?: bool }`
    /// (default: whole store, dry run).
    DbTombstoneFlagAudit,
    /// `POST /api/db/drain-legacy-tombstones` — bounded, resumable hard erase
    /// of tombstone-content `mk:` slots. Body:
    /// `{ schema?: string, dry_run?: bool, max_keys?: usize, after_key?: string }`.
    DbDrainLegacyTombstones,
    /// `POST /api/db/legacy-key-fork-audit` — audit or drain legacy/plain
    /// HashKey-encoding tips. Body:
    /// `{ dry_run?: bool, max_keys?: usize, after_key?: string }`.
    DbLegacyKeyForkAudit,
    /// `POST /api/db/order-log-audit` — compare every `moc:{M}` order-log count
    /// against its molecule's live `mk:` record count. Read-only detector for a
    /// truncated `update_order` log, which no point read can see. Body:
    /// `{ max_keys?: usize, after_key?: string }`.
    DbOrderLogAudit,
    /// `POST /api/db/pin-log-audit` — bounded, resumable, read-only pin-log
    /// plane audit. Classifies entry rows as confirmed-orphan vs pending using
    /// durable published-F high-water marks. Never deletes. Body:
    /// `{ max_keys?: usize, after_key?: string }`.
    DbPinLogAudit,
    /// `POST /api/db/order-log-bloat-audit` — measure append-only order-log
    /// excess (stale entries + zero-live residue) with exact stored bytes.
    /// Read-only. Body: `{ max_keys?: usize, after_key?: string }`.
    DbOrderLogBloatAudit,
    /// `POST /api/db/compact-order-log` — plan or delete the order log of a
    /// zero-live molecule, a bloated molecule, and a clean molecule. Does not
    /// write a new log. Body:
    /// `{ dry_run?: bool, max_keys?: usize, after_key?: string, retention_seconds?: u64 }`.
    DbCompactOrderLog,
    /// `POST /api/db/repair-order-log-shortfall` — the verb writes nothing.
    /// Body: `{ dry_run?: bool, max_keys?: usize, after_key?: string }`.
    DbRepairOrderLogShortfall,
    /// `POST /api/db/molecule-keys` — list one molecule's live `mk:` storage
    /// keys (decoded hash/range + collision flag). Read-only.
    DbMoleculeKeys,
    /// `POST /api/db/schema-retention` — owner read/set/clear of the
    /// node-local time-based retention policy for one installed schema.
    DbSchemaRetention,
    /// `POST /api/db/repair-schema-molecule-map` — compare-and-set repair of
    /// one installed schema's field-to-molecule metadata. Owner only. The
    /// request is a dry run unless `execute=true`.
    DbRepairSchemaMoleculeMap,
    /// `POST /api/db/repair-hashrange-key-fields` — plan or repair sparse
    /// declared key-field molecules for one named HashRange partition.
    /// Owner only. Body: `{ schema, api_hash, execute?: bool }`.
    DbRepairHashRangeKeyFields,
    /// `POST /api/db/drain-plane-residue` — copy one bounded page of rows
    /// sitting outside their canonical plane collection (for example
    /// `conflict:` rows in legacy `sync_conflicts`) into the canonical home,
    /// deleting the source copy per key once the target holds it. Body:
    /// `{ family: "tip"|"protein"|"index"|"conflict"|"order_log",
    ///    source_collection: string, target_collection: string,
    ///    after?: string, limit?: usize, execute?: bool,
    ///    drop_empty_source?: bool }` (default: dry run).
    DbDrainPlaneResidue,
    /// `POST /api/db/fetch-file-blob` — explicitly fetch exactly one
    /// `$lastdb_file` pointer's remote CAS object and cache it locally.
    /// Body: `{ "pointer": <field value> }`.
    DbFetchFileBlob,
    /// `POST /api/db/fork-file-blob` — copy a shared `$lastdb_file` pointer's
    /// bytes into the caller's personal blob scope and rewrite the local field.
    /// Body includes `{ schema, field, key, pointer }`.
    DbForkFileBlob,
    /// `POST /api/db/file-blob` — upload one personal file blob through the
    /// native file-blob plane and persist the returned `$lastdb_file` pointer
    /// into the requested schema field. OWNER socket only.
    DbPutFileBlob,
    /// `POST /api/db/put-blob-local` — store one blob in the local `cas_blobs`
    /// plane and return its `$lastdb_file` pointer. No sync engine, no record
    /// written. Raw `application/octet-stream` or JSON `{ bytes_b64 }`. OWNER only.
    DbPutBlobLocal,
    /// `POST /api/sharing/deliver` — stage a consent-gated snapshot delivery.
    /// OWNER socket only; no network until approve.
    DeliverStage,
    /// `POST /api/sharing/snapshot` — materialize + seal a query snapshot and
    /// return `snapshot_key = sha256(canonical_query || recipient)` + sealed
    /// blob (no messaging append; object-store publishers overwrite by key).
    /// OWNER socket only.
    DeliverSnapshot,
    /// `GET /api/sharing/deliveries` — list staged deliveries awaiting approve.
    /// OWNER socket only.
    DeliverList,
    /// `POST /api/sharing/deliveries/{id}/approve` — seal + send via Exemem.
    /// OWNER socket only.
    DeliverApprove,
    /// `POST /api/sharing/deliveries/{id}/reject` — discard staged delivery.
    /// OWNER socket only.
    DeliverReject,
    /// `GET /api/local-watch` — short-TTL local mutation doorbell poll/long-poll.
    /// Not product truth; clients still keyed-read tables after wake.
    LocalWatch,
    /// `POST /api/app/changes` — durable ordered mutation metadata, scoped to
    /// schemas visible to the verified app (or the owner on the owner socket).
    AppChanges,
    /// `GET /api/protein/{uuid}` — load a protein record by UUID.
    ///
    /// Read-only by design. Proteins used to be an app-facing write surface
    /// (`POST /api/protein{,/member,/write,/fold}`), which made every app that
    /// wanted multi-key coherence responsible for creating proteins, binding
    /// members, and driving folds itself — and made a mistake in any one of them
    /// an app's way to corrupt another app's tips. The node now detects
    /// multi-key siblings and binds them on schema load (`field_hash_coherence`),
    /// and folds on the ordinary mutation path, so nothing outside the engine
    /// needs to name a protein. What remains here is introspection.
    ProteinGet,
    /// `GET /api/protein/of-molecule/{molecule_uuid}` — resolve the protein a
    /// molecule is already bound to, or `null` when unbound.
    ///
    /// Exists so a client can **adopt** an existing binding without writing.
    /// Without it, the only way to learn the binding was to `POST /api/protein`
    /// (a durable write), attempt the bind, and read the owning UUID out of the
    /// rejection message — which orphaned the just-created protein on every
    /// call, permanently, because nothing deletes `protein:` rows.
    ProteinOfMolecule,
}

impl DataRoute {
    /// The `(method, path)` pair this route matches — the inverse of [`route`]'s
    /// classification, useful for asserting the table is self-consistent.
    pub fn method_and_path(self) -> (&'static str, &'static str) {
        // lint:fn-size-ok moved verbatim from uds_router.rs; splitting it is a separate change
        match self {
            Self::Query => ("POST", API_QUERY_PATH),
            Self::Mutation => ("POST", API_MUTATION_PATH),
            Self::AggregateRepair => ("POST", API_AGGREGATE_REPAIR_PATH),
            Self::AggregateFinalize => ("POST", API_AGGREGATE_FINALIZE_PATH),
            Self::MutationBatch => ("POST", "/api/mutations/batch"),
            Self::QueryBatch => ("POST", API_QUERY_BATCH_PATH),
            Self::ListSchemas => ("GET", API_SCHEMAS_PATH),
            Self::ListRecordKeys => ("GET", API_LIST_PATH),
            Self::GetSchema => ("GET", "/api/schema/{name}"),
            Self::DeclareSchema => ("POST", "/api/schemas/declare"),
            Self::SeedSystemSchema => ("POST", "/api/schemas/seed-system"),
            Self::RetireSchemaNameClaim => ("POST", "/api/schemas/retire-name-claim"),
            Self::DropSchema => ("POST", "/api/schemas/drop"),
            Self::Status => ("GET", "/api/status"),
            Self::LogFilterGet => ("GET", API_LOG_FILTER_PATH),
            Self::LogFilterSet => ("POST", API_LOG_FILTER_PATH),
            Self::AdminShed => ("POST", "/api/admin/shed"),
            Self::AutoIdentity => ("GET", API_AUTO_IDENTITY_PATH),
            Self::BootIdentity => ("GET", API_BOOT_IDENTITY_PATH),
            Self::BootLedger => ("GET", API_BOOT_LEDGER_PATH),
            Self::NativeIndexSearch => ("GET", "/api/native-index/search"),
            Self::SearchAppQuery => ("GET", "/api/search/query"),
            Self::AppSearch => ("POST", API_APP_SEARCH_PATH),
            Self::NativeIndexAppVectorPut => ("POST", "/api/native-index/embeddings"),
            Self::NativeIndexKnn => ("POST", "/api/native-index/knn"),
            Self::MoleculeHistory => ("GET", "/api/history/{molecule_uuid}"),
            Self::AtomContent => ("GET", "/api/atom/{atom_uuid}"),
            Self::AppBlobPut => ("PUT", "/api/app/blob/cas/sha256/{hash}"),
            Self::AppBlobGet => ("GET", "/api/app/blob/cas/sha256/{hash}"),
            Self::AppOrgBlobPut => ("PUT", "/api/app/org/{org_hash}/blob/cas/sha256/{hash}"),
            Self::AppOrgBlobGet => ("GET", "/api/app/org/{org_hash}/blob/cas/sha256/{hash}"),
            Self::AppBlobFootprint => ("GET", "/api/app/blob/footprint"),
            Self::AppStorage => ("GET", "/api/storage/app"),
            Self::AppStorageReconcile => ("POST", "/api/storage/app/reconcile"),
            Self::HomeStorage => ("GET", "/api/storage/home"),
            Self::HomeStorageReconcile => ("POST", "/api/storage/home/reconcile"),
            Self::OrgSyncRegister => ("POST", "/api/org/sync/register"),
            Self::OrgSyncTargets => ("GET", "/api/org/sync/targets"),
            Self::OrgSyncDeactivate => ("POST", "/api/org/sync/deactivate"),
            Self::OrgSyncGrantMember => ("POST", "/api/org/sync/grant-member"),
            Self::OrgSyncRevokeMember => ("POST", "/api/org/sync/revoke-member"),
            Self::DbCatalogGet => ("GET", "/api/db/catalog"),
            Self::DbCatalogPut => ("POST", "/api/db/catalog"),
            Self::DbCatalogDelete => ("DELETE", "/api/db/catalog"),
            Self::DbCatalogShare => ("POST", "/api/db/catalog/share"),
            Self::DbCatalogReclaim => ("POST", "/api/db/catalog/reclaim"),
            Self::SyncHealStaging => ("POST", "/api/sync/heal-staging"),
            Self::SyncLastStoreSnapshot => ("POST", "/api/sync/laststore-snapshot"),
            Self::SyncBackupGc => ("POST", "/api/sync/backup-gc"),
            Self::SyncPrefixInventory => ("POST", "/api/sync/prefix-inventory"),
            Self::SyncCloudOff => ("POST", "/api/sync/cloud-off"),
            Self::SyncCloudOn => ("POST", "/api/sync/cloud-on"),
            Self::SyncCloudResumePrimary => ("POST", "/api/sync/cloud-resume-primary"),
            Self::SyncQuarantineReplayBlocker => ("POST", "/api/sync/quarantine-replay-blocker"),
            Self::SyncBackupConcurrency => ("POST", "/api/sync/backup-concurrency"),
            Self::DbInventory => ("GET", "/api/db/inventory"),
            Self::DbSchemas => ("GET", "/api/db/schemas"),
            Self::SchemaStorage => ("POST", "/api/storage/schema"),
            Self::SchemaStorageReport => ("GET", "/api/storage/schemas"),
            Self::LivenessExplain => ("POST", "/api/storage/liveness/explain"),
            Self::LivenessBootstrap => ("POST", "/api/storage/liveness/bootstrap"),
            Self::DbClearHistory => ("POST", "/api/db/clear-history"),
            Self::DbCompact => ("POST", "/api/db/compact"),
            Self::DbStampPurgedAtomRetirements => ("POST", "/api/db/stamp-purged-atom-retirements"),
            Self::CompactRecord => ("POST", "/api/db/compact-record"),
            Self::DbPurgeSchemaIdx => ("POST", "/api/db/purge-schemaidx"),
            Self::DbGcAtoms => ("POST", "/api/db/gc-atoms"),
            Self::DbReapDroppedSchema => ("POST", "/api/db/reap-dropped-schema"),
            Self::DbGcFileBlobs => ("POST", "/api/db/gc-file-blobs"),
            Self::DbGcProteins => ("POST", "/api/db/gc-proteins"),
            Self::DbPurgeRefBlobs => ("POST", "/api/db/purge-ref-blobs"),
            Self::DbReclaimKeepSmallLegacy => ("POST", "/api/db/reclaim-keep-small-legacy"),
            Self::DbReclaimKeepSmallSnapshot => ("POST", "/api/db/reclaim-keep-small-snapshot"),
            Self::DbRepairDanglingTips => ("POST", "/api/db/repair-dangling-tips"),
            Self::DbUnresolvedAtoms => ("GET", "/api/db/unresolved-atoms"),
            Self::DbDrainTipHistory => ("POST", "/api/db/drain-tip-history"),
            Self::DbRetainSupersededVersions => ("POST", "/api/db/retain-superseded-versions"),
            Self::DbProbeLocatorOnly => ("POST", "/api/db/probe-locator-only"),
            Self::DbDeleteLedger => ("GET", "/api/db/delete-ledger"),
            Self::DbMigratePhotoBlobs => ("POST", "/api/db/migrate-photo-blobs"),
            Self::DbMigrateThinTips => ("POST", "/api/db/migrate-thin-tips"),
            Self::DbRekeyAtomPartitionPrefix => ("POST", "/api/db/rekey-atom-partition-prefix"),
            Self::DbResealAtRest => ("POST", "/api/db/reseal-at-rest"),
            Self::DbReapUnsealed => ("POST", "/api/db/reap-unsealed"),
            Self::DbTombstoneFlagAudit => ("POST", "/api/db/tombstone-flag-audit"),
            Self::DbDrainLegacyTombstones => ("POST", "/api/db/drain-legacy-tombstones"),
            Self::DbLegacyKeyForkAudit => ("POST", "/api/db/legacy-key-fork-audit"),
            Self::DbOrderLogAudit => ("POST", "/api/db/order-log-audit"),
            Self::DbPinLogAudit => ("POST", "/api/db/pin-log-audit"),
            Self::DbOrderLogBloatAudit => ("POST", "/api/db/order-log-bloat-audit"),
            Self::DbCompactOrderLog => ("POST", "/api/db/compact-order-log"),
            Self::DbRepairOrderLogShortfall => ("POST", "/api/db/repair-order-log-shortfall"),
            Self::DbMoleculeKeys => ("POST", "/api/db/molecule-keys"),
            Self::DbSchemaRetention => ("POST", "/api/db/schema-retention"),
            Self::DbRepairSchemaMoleculeMap => ("POST", "/api/db/repair-schema-molecule-map"),
            Self::DbRepairHashRangeKeyFields => ("POST", "/api/db/repair-hashrange-key-fields"),
            Self::DbDrainPlaneResidue => ("POST", "/api/db/drain-plane-residue"),
            Self::DbFetchFileBlob => ("POST", "/api/db/fetch-file-blob"),
            Self::DbForkFileBlob => ("POST", "/api/db/fork-file-blob"),
            Self::DbPutFileBlob => ("POST", "/api/db/file-blob"),
            Self::DbPutBlobLocal => ("POST", "/api/db/put-blob-local"),
            Self::DeliverStage => ("POST", "/api/sharing/deliver"),
            Self::DeliverSnapshot => ("POST", "/api/sharing/snapshot"),
            Self::DeliverList => ("GET", "/api/sharing/deliveries"),
            Self::DeliverApprove => ("POST", "/api/sharing/deliveries/{id}/approve"),
            Self::DeliverReject => ("POST", "/api/sharing/deliveries/{id}/reject"),
            Self::LocalWatch => ("GET", "/api/local-watch"),
            Self::AppChanges => ("POST", API_APP_CHANGES_PATH),
            Self::ProteinGet => ("GET", "/api/protein/{uuid}"),
            Self::ProteinOfMolecule => ("GET", "/api/protein/of-molecule/{molecule_uuid}"),
        }
    }
}
