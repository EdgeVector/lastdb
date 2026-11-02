// lint:file-size-ok moved verbatim out of a larger file; further splitting is follow-up work

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
    /// `POST /api/mutations/batch` — write multiple mutations in one request.
    MutationBatch,
    /// `POST /api/queries/batch` — several independent `/api/query` bodies in one request (both sockets).
    QueryBatch,
    /// `POST /api/conflicts/keys` — the kept conflicts of selected record fields (owner only).
    ConflictMetadata,
    /// `GET /api/schemas` — list the node's available schemas.
    ListSchemas,
    /// `GET /api/list?schema=` — keys-only page of live record identities
    /// (hash + range). No record bodies. Served on both sockets.
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
    /// records stay until a later janitor. This is not
    /// `RetireSchemaNameClaim` (name resolution only) and not a product-row
    /// Delete.
    ///
    /// **OWNER socket only**.
    DropSchema,
    /// `GET /api/status` — stateful daemon status: uptime, sampler, process
    /// vitals, disk usage, and backup health.
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
    /// `POST /api/field-history` — owner-only field policies, lookup, purge and restore.
    FieldHistory,
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
    /// `POST /api/sync/byte-backup-v2` — explicit owner byte backup work or status.
    SyncByteBackupV2,
    /// `POST /api/sync/byte-backup-v2/connect` — import owner credentials for manual backup.
    SyncByteBackupV2Connect,
    /// `POST /api/sync/logical-v1` — owner trust and bounded signed device events.
    SyncLogicalV1,
    /// `GET /api/db/inventory` — live main-tree key-class + per-schema record/history
    /// byte breakdown. OWNER socket only. It writes nothing.
    DbInventory,
    /// `POST /api/db/verify-refs` — one bounded step of the reference
    /// verifier (the retention queue and the blob counters). OWNER socket only.
    /// Body: `{cursor?, max_pages?, max_ms?, pause_ms?, page_rows?, scope?}`. Reads and reports; it
    /// writes nothing and takes no write gate. Without `cursor` it starts a
    /// new run; with the `next_cursor` of the last step it continues the run.
    DbVerifyRefs,
    /// `GET /api/db/verify-refs` — the last complete verify run and the run in
    /// progress, from memory. OWNER socket only. Reads no store.
    DbVerifyRefsLast,
    /// `GET /api/db/verify-refs/scopes` — the storage scope prefixes that the
    /// database catalog names, besides the personal scope. OWNER socket only.
    /// Reads the catalog, not the data.
    DbVerifyRefsScopes,
    /// `GET /api/db/schemas` — record-only per-schema logical storage.
    /// OWNER socket only. Cheaper than [`Self::DbInventory`] (no history or
    /// order-log walks) but still a heavy admin scan.
    DbSchemas,
    /// `POST /api/storage/liveness/explain` — point-read the counter of one
    /// file blob, and its dwell queue state.
    LivenessExplain,
    /// `POST /api/db/flush` — wait for accepted local writes to reach disk.
    /// OWNER only. Body: `{}` or empty. No cloud work.
    DbFlush,
    /// `POST /api/db/cleanup` saves a resumable cleanup request; GET reads its receipt.
    DbCleanup,
    /// `GET /api/db/native-status` — read native completion and writer state.
    /// OWNER only. This does not request a flush.
    DbNativeStatus,
    /// `GET /api/db/delete-ledger` — read the durable record hard-delete audit
    /// trail (purge and delete), oldest first. Query: `?limit=N` (0/absent =
    /// all). Read-only; OWNER socket only, because it names which schemas had
    /// records erased.
    DbDeleteLedger,
    /// `POST /api/db/schema-retention` — owner read/set/clear of the
    /// node-local time-based retention policy for one installed schema.
    DbSchemaRetention,
    /// `POST /api/db/fetch-file-blob` — explicitly fetch exactly one
    /// `$lastdb_file` pointer from the local store, with byte verification.
    /// Body: `{ "pointer": <field value> }`.
    DbFetchFileBlob,
    /// `POST /api/db/put-blob-local` — store one blob in the local `cas_blobs`
    /// plane and return its `$lastdb_file` pointer. No sync engine, no record
    /// written. Raw `application/octet-stream` or JSON `{ bytes_b64 }`. OWNER only.
    DbPutBlobLocal,
    /// `POST /api/sharing/snapshot` — materialize + seal a query snapshot and
    /// return `snapshot_key = sha256(canonical_query || recipient)` + sealed
    /// blob (no messaging append; object-store publishers overwrite by key).
    /// OWNER socket only.
    DeliverSnapshot,
    /// `GET /api/local-watch` — short-TTL local mutation doorbell poll/long-poll.
    /// Not product truth; clients still keyed-read tables after wake.
    LocalWatch,
    /// `POST /api/app/changes` — durable ordered mutation metadata, scoped to
    /// schemas visible to the verified app (or the owner on the owner socket).
    AppChanges,
}
