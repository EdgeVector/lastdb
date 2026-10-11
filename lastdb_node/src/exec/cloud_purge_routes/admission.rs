//! Fail local writes promptly while the selected source holds its fixed cut.
use super::*;

pub(in crate::exec) fn reject_local_write(
    route: DataRoute,
    host: &Host,
    ctx: &AccessContext,
) -> Option<UdsResponse> {
    if matches!(
        route,
        DataRoute::Query
            | DataRoute::QueryBatch
            | DataRoute::ListSchemas
            | DataRoute::ListRecordKeys
            | DataRoute::GetSchema
            | DataRoute::Status
            | DataRoute::BootIdentity
            | DataRoute::BootLedger
            | DataRoute::AutoIdentity
            | DataRoute::SyncCloudPurgeResync
            | DataRoute::DbInventory
            | DataRoute::DbSchemas
            | DataRoute::DbCatalogGet
            | DataRoute::ProteinGet
            | DataRoute::ProteinOfMolecule
            | DataRoute::MoleculeHistory
            | DataRoute::AtomContent
            | DataRoute::SchemaStorage
            | DataRoute::SchemaStorageReport
            | DataRoute::HomeStorage
            | DataRoute::AppStorage
            | DataRoute::NativeIndexSearch
            | DataRoute::SearchAppQuery
            | DataRoute::AppSearch
            | DataRoute::LocalWatch
            | DataRoute::AppChanges
            | DataRoute::OrgSyncTargets
            | DataRoute::LogFilterGet
    ) {
        return None;
    }
    match crate::cloud_purge_runtime::local_writes_paused(host) {
        Ok(false) => None,
        Ok(true) => Some(error_response(
            409,
            "the selected device pauses local writes until cloud purge and resync completes",
            ctx,
        )),
        Err(_) => Some(error_response(
            503,
            "the selected cloud source write gate is unavailable",
            ctx,
        )),
    }
}
