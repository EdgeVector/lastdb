//! Data routes exposed on the owner control socket.

/// A data route the control socket exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataRoute {
    /// `POST /api/query` — read query against the node's data.
    Query,
    /// `POST /api/mutation` — write mutation against the node's data.
    Mutation,
    /// `POST /api/mutations/batch` — write multiple mutations in one request.
    MutationBatch,
    /// `POST /api/queries/batch` — several independent `/api/query` bodies in one request.
    QueryBatch,
    /// `GET /api/schemas` — list the node's available schemas.
    ListSchemas,
    /// `GET /api/list?schema=` — keys-only page of live record identities.
    ListRecordKeys,
    /// `GET /api/schema/{name}` — fetch one schema by canonical name/hash or descriptive name.
    GetSchema,
    /// `POST /api/schemas/declare` — owner direct namespaced schema declaration.
    DeclareSchema,
    /// `POST /api/schemas/seed-system` — install one `system_seed` schema on an isolated daemon.
    SeedSystemSchema,
    /// `POST /api/schemas/retire-name-claim` — retire or restore one installed schema's claim.
    RetireSchemaNameClaim,
    /// `POST /api/schemas/drop` — remove one installed schema identity or an app's schemas.
    DropSchema,
    /// `GET /api/status` — daemon status summary.
    Status,
    /// `POST /api/admin/shed` — flush pending writes and purge clean warm memory.
    AdminShed,
    /// `GET /api/system/auto-identity` — node public key and derived user hash.
    AutoIdentity,
    /// `GET /api/system/boot-identity` — current process pid, start timestamp, and build.
    BootIdentity,
    /// `POST /api/sync/byte-backup-v2` — explicit owner byte backup work or status.
    SyncByteBackupV2,
    /// `POST /api/sync/byte-backup-v2/connect` — import owner credentials for manual backup.
    SyncByteBackupV2Connect,
    /// `POST /api/sync/shared-snapshot` — publish and finish shared writer upload.
    SyncSharedSnapshot,
    /// `GET /api/db/inventory` — live key-class and per-schema record byte breakdown.
    DbInventory,
    /// `GET /api/db/schemas` — record-only per-schema logical storage.
    DbSchemas,
    /// `POST /api/db/flush` — wait for accepted local writes to reach disk.
    DbFlush,
    /// `POST /api/db/cleanup` / `GET /api/db/cleanup` — resumable cleanup request and receipt.
    DbCleanup,
    /// `GET /api/db/native-status` — read native completion and writer state.
    DbNativeStatus,
    /// `GET /api/db/delete-ledger` — read the durable record hard-delete audit trail.
    DbDeleteLedger,
    /// `POST /api/db/schema-retention` — read/set/clear time-based schema retention policy.
    DbSchemaRetention,
    /// `POST /api/db/put-blob` — store the file of one record with its marker and fields.
    DbPutBlob,
    /// `POST /api/db/fetch-blob` — read the file of one record by schema and key.
    DbFetchBlob,
    /// `POST /api/db/delete-blob` — remove the file and marker of one record.
    DbDeleteBlob,
    /// `POST /api/app/changes` — durable ordered mutation metadata.
    AppChanges,
}
