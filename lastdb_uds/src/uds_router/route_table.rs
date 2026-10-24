//! Fixed route table for single-method, exact-path routes.
//!
//! One row per `(path, scope, method)`. [`super::route`] consults the table
//! after its few hand-written arms (health, version, multi-method and
//! prefix-matched paths). A row with [`Scope::Owner`] exists only on the owner
//! socket: on the app socket the path is unrecognized and falls through to
//! `NotFound`, the structural no-mint guarantee for owner-only verbs.

use super::DataRoute;

/// Which sockets serve a table row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Scope {
    /// Both the owner socket and the app socket.
    Both,
    /// The owner socket only.
    Owner,
}

/// One exact-path, single-method data route.
pub(super) struct Row {
    pub(super) path: &'static str,
    pub(super) scope: Scope,
    pub(super) method: &'static str,
    pub(super) route: DataRoute,
}

const fn row(path: &'static str, scope: Scope, method: &'static str, route: DataRoute) -> Row {
    Row {
        path,
        scope,
        method,
        route,
    }
}

#[rustfmt::skip]
pub(super) static EXACT_ROUTES: &[Row] = &[
    row("/api/schemas", Scope::Both, "GET", DataRoute::ListSchemas),
    row("/api/list", Scope::Both, "GET", DataRoute::ListRecordKeys),
    row("/api/db/list", Scope::Both, "GET", DataRoute::ListRecordKeys),
    row("/api/schemas/declare", Scope::Owner, "POST", DataRoute::DeclareSchema),
    row("/api/schemas/seed-system", Scope::Owner, "POST", DataRoute::SeedSystemSchema),
    row("/api/schemas/retire-name-claim", Scope::Owner, "POST", DataRoute::RetireSchemaNameClaim),
    row("/api/schemas/drop", Scope::Owner, "POST", DataRoute::DropSchema),
    row("/api/status", Scope::Owner, "GET", DataRoute::Status),
    row("/api/admin/shed", Scope::Owner, "POST", DataRoute::AdminShed),
    row("/api/db/inventory", Scope::Owner, "GET", DataRoute::DbInventory),
    row("/api/db/verify-refs/scopes", Scope::Owner, "GET", DataRoute::DbVerifyRefsScopes),
    row("/api/db/molecule-drain", Scope::Owner, "POST", DataRoute::DbMoleculeDrain),
    row("/api/db/schemas", Scope::Owner, "GET", DataRoute::DbSchemas),
    row("/api/storage/schema", Scope::Owner, "POST", DataRoute::SchemaStorage),
    row("/api/storage/schemas", Scope::Owner, "GET", DataRoute::SchemaStorageReport),
    row("/api/storage/liveness/explain", Scope::Owner, "POST", DataRoute::LivenessExplain),
    row("/api/db/clear-history", Scope::Owner, "POST", DataRoute::DbClearHistory),
    row("/api/db/compact", Scope::Owner, "POST", DataRoute::DbCompact),
    row("/api/db/native-backup", Scope::Owner, "POST", DataRoute::DbNativeBackup),
    row("/api/db/flush", Scope::Owner, "POST", DataRoute::DbFlush),
    row("/api/db/native-status", Scope::Owner, "GET", DataRoute::DbNativeStatus),
    row("/api/db/stamp-purged-atom-retirements", Scope::Owner, "POST", DataRoute::DbStampPurgedAtomRetirements),
    row("/api/db/gc-proteins", Scope::Owner, "POST", DataRoute::DbGcProteins),
    row("/api/db/reclaim-keep-small-snapshot", Scope::Owner, "POST", DataRoute::DbReclaimKeepSmallSnapshot),
    row("/api/db/unresolved-atoms", Scope::Owner, "GET", DataRoute::DbUnresolvedAtoms),
    row("/api/db/delete-ledger", Scope::Owner, "GET", DataRoute::DbDeleteLedger),
    row("/api/db/reseal-at-rest", Scope::Owner, "POST", DataRoute::DbResealAtRest),
    row("/api/db/reap-unsealed", Scope::Owner, "POST", DataRoute::DbReapUnsealed),
    row("/api/db/pin-log-audit", Scope::Owner, "POST", DataRoute::DbPinLogAudit),
    row("/api/db/export-mutation-log", Scope::Owner, "POST", DataRoute::DbExportMutationLog),
    row("/api/db/replay-mutation-log", Scope::Owner, "POST", DataRoute::DbReplayMutationLog),
    row("/api/db/molecule-keys", Scope::Owner, "POST", DataRoute::DbMoleculeKeys),
    row("/api/db/schema-retention", Scope::Owner, "POST", DataRoute::DbSchemaRetention),
    row("/api/db/repair-schema-molecule-map", Scope::Owner, "POST", DataRoute::DbRepairSchemaMoleculeMap),
    row("/api/db/repair-hashrange-key-fields", Scope::Owner, "POST", DataRoute::DbRepairHashRangeKeyFields),
    row("/api/db/fetch-file-blob", Scope::Owner, "POST", DataRoute::DbFetchFileBlob),
    row("/api/db/fork-file-blob", Scope::Owner, "POST", DataRoute::DbForkFileBlob),
    row("/api/db/file-blob", Scope::Owner, "POST", DataRoute::DbPutFileBlob),
    row("/api/db/put-blob-local", Scope::Owner, "POST", DataRoute::DbPutBlobLocal),
    row("/api/db/catalog/share", Scope::Owner, "POST", DataRoute::DbCatalogShare),
    row("/api/org/sync/register", Scope::Owner, "POST", DataRoute::OrgSyncRegister),
    row("/api/org/authority/publish", Scope::Owner, "POST", DataRoute::OrgAuthorityPublish),
    row("/api/org/authority/status", Scope::Owner, "POST", DataRoute::OrgAuthorityStatus),
    row("/api/org/sync/targets", Scope::Owner, "GET", DataRoute::OrgSyncTargets),
    row("/api/org/sync/deactivate", Scope::Owner, "POST", DataRoute::OrgSyncDeactivate),
    row("/api/org/sync/grant-member", Scope::Owner, "POST", DataRoute::OrgSyncGrantMember),
    row("/api/org/sync/revoke-member", Scope::Owner, "POST", DataRoute::OrgSyncRevokeMember),
    row("/api/sync/heal-staging", Scope::Owner, "POST", DataRoute::SyncHealStaging),
    row("/api/native-index/search", Scope::Owner, "GET", DataRoute::NativeIndexSearch),
    row("/api/sync/laststore-snapshot", Scope::Owner, "POST", DataRoute::SyncLastStoreSnapshot),
    row("/api/sync/backup-gc", Scope::Owner, "POST", DataRoute::SyncBackupGc),
    row("/api/sync/prefix-inventory", Scope::Owner, "POST", DataRoute::SyncPrefixInventory),
    row("/api/sync/cloud-off", Scope::Owner, "POST", DataRoute::SyncCloudOff),
    row("/api/sync/cloud-on", Scope::Owner, "POST", DataRoute::SyncCloudOn),
    row("/api/sync/native-backup", Scope::Owner, "POST", DataRoute::SyncNativeBackup),
    row("/api/sync/byte-backup-v2", Scope::Owner, "POST", DataRoute::SyncByteBackupV2),
    row("/api/sync/cloud-resume-primary", Scope::Owner, "POST", DataRoute::SyncCloudResumePrimary),
    row("/api/sync/quarantine-replay-blocker", Scope::Owner, "POST", DataRoute::SyncQuarantineReplayBlocker),
    row("/api/sync/backup-concurrency", Scope::Owner, "POST", DataRoute::SyncBackupConcurrency),
    row("/api/sharing/deliver", Scope::Owner, "POST", DataRoute::DeliverStage),
    row("/api/sharing/recipient", Scope::Owner, "POST", DataRoute::RecipientAddress),
    row("/api/sharing/snapshot", Scope::Owner, "POST", DataRoute::DeliverSnapshot),
    row("/api/sharing/deliveries", Scope::Owner, "GET", DataRoute::DeliverList),
    row("/api/query", Scope::Both, "POST", DataRoute::Query),
    row("/api/mutation", Scope::Both, "POST", DataRoute::Mutation),
    row("/api/aggregate/repair", Scope::Owner, "POST", DataRoute::AggregateRepair),
    row("/api/aggregate/finalize", Scope::Owner, "POST", DataRoute::AggregateFinalize),
    row("/api/mutations/batch", Scope::Both, "POST", DataRoute::MutationBatch),
    row("/api/queries/batch", Scope::Both, "POST", DataRoute::QueryBatch),
    row("/api/conflicts/keys", Scope::Owner, "POST", DataRoute::ConflictMetadata),
    row("/api/local-watch", Scope::Both, "GET", DataRoute::LocalWatch),
    row("/api/app/changes", Scope::Both, "POST", DataRoute::AppChanges),
    row("/api/app/search", Scope::Both, "POST", DataRoute::AppSearch),
    row("/api/app/blob/footprint", Scope::Both, "GET", DataRoute::AppBlobFootprint),
    row("/api/storage/app", Scope::Owner, "GET", DataRoute::AppStorage),
    row("/api/storage/app/reconcile", Scope::Owner, "POST", DataRoute::AppStorageReconcile),
    row("/api/storage/home", Scope::Owner, "GET", DataRoute::HomeStorage),
    row("/api/storage/home/reconcile", Scope::Owner, "POST", DataRoute::HomeStorageReconcile),
    row("/api/system/auto-identity", Scope::Owner, "GET", DataRoute::AutoIdentity),
    row("/api/system/boot-identity", Scope::Owner, "GET", DataRoute::BootIdentity),
    row("/api/system/boot-ledger", Scope::Owner, "GET", DataRoute::BootLedger),
    row("/api/native-index/embeddings", Scope::Both, "POST", DataRoute::NativeIndexAppVectorPut),
    row("/api/native-index/knn", Scope::Both, "POST", DataRoute::NativeIndexKnn),
    row("/api/search/query", Scope::Both, "GET", DataRoute::SearchAppQuery),
];
