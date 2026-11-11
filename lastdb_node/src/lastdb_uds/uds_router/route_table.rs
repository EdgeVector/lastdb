//! Fixed route table for single-method, exact-path routes.

use super::DataRoute;

pub(super) struct Row {
    pub(super) path: &'static str,
    pub(super) method: &'static str,
    pub(super) route: DataRoute,
}

const fn row(path: &'static str, method: &'static str, route: DataRoute) -> Row {
    Row {
        path,
        method,
        route,
    }
}

#[rustfmt::skip]
pub(super) static EXACT_ROUTES: &[Row] = &[
    row("/api/schemas", "GET", DataRoute::ListSchemas),
    row("/api/list", "GET", DataRoute::ListRecordKeys),
    row("/api/schemas/declare", "POST", DataRoute::DeclareSchema),
    row("/api/schemas/seed-system", "POST", DataRoute::SeedSystemSchema),
    row("/api/schemas/retire-name-claim", "POST", DataRoute::RetireSchemaNameClaim),
    row("/api/schemas/drop", "POST", DataRoute::DropSchema),
    row("/api/status", "GET", DataRoute::Status),
    row("/api/admin/shed", "POST", DataRoute::AdminShed),
    row("/api/db/inventory", "GET", DataRoute::DbInventory),
    row("/api/db/schemas", "GET", DataRoute::DbSchemas),
    row("/api/db/flush", "POST", DataRoute::DbFlush),
    row("/api/db/delete-ledger", "GET", DataRoute::DbDeleteLedger),
    row("/api/db/schema-retention", "POST", DataRoute::DbSchemaRetention),
    row("/api/db/put-blob", "POST", DataRoute::DbPutBlob),
    row("/api/db/fetch-blob", "POST", DataRoute::DbFetchBlob),
    row("/api/db/delete-blob", "POST", DataRoute::DbDeleteBlob),
    row("/api/sync/byte-backup-v2", "POST", DataRoute::SyncByteBackupV2),
    row("/api/sync/byte-backup-v2/connect", "POST", DataRoute::SyncByteBackupV2Connect),
    row("/api/sync/shared-snapshot", "POST", DataRoute::SyncSharedSnapshot),
    row("/api/query", "POST", DataRoute::Query),
    row("/api/mutation", "POST", DataRoute::Mutation),
    row("/api/mutations/batch", "POST", DataRoute::MutationBatch),
    row("/api/queries/batch", "POST", DataRoute::QueryBatch),
    row("/api/app/changes", "POST", DataRoute::AppChanges),
    row("/api/system/auto-identity", "GET", DataRoute::AutoIdentity),
    row("/api/system/boot-identity", "GET", DataRoute::BootIdentity),
];
