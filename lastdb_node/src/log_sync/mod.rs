//! Daemon adapter for the encrypted cloud mutation log.
//!
//! Resolve a database by its registered id. Authorization determines the
//! permitted writes. Snapshot recovery installs into that database and the
//! sync loop acknowledges only after the delivered prefix is durable.

mod accepted;
mod apply;
mod automatic_retention;
mod blob_retention;
mod cloud;
mod contributions;
mod disk;
mod http;
mod inbox;
mod index_history;
mod maintenance;
mod members;
mod native;
mod native_reset;
mod owner_recovery;
mod received;
pub(crate) mod received_access;
mod registry;
mod restore;
mod retention;
mod retry;
mod run;
mod session;
mod signals;
mod snapshot;
mod status;
mod turn;
mod upload;
mod wire;

pub(crate) use contributions::{
    schema_id, status as share_write_status, submit as submit_share_write, validate_write_scopes,
    ContributionError,
};
pub use index_history::{index_history, IndexHistoryReport};
pub use native_reset::install_prepared_native_recovery;
pub use owner_recovery::{recover_owned, RecoveredPrefix};
pub(crate) use restore::SnapshotFiles;
pub use retention::{reclaim as reclaim_snapshot_log, ReclaimReport};
pub(crate) use signals::ensure_wake;
pub use signals::{publish_first_backup_intent, request_wake};
pub(crate) use snapshot::{
    completed as snapshot_completed, error as snapshot_error, request as request_snapshot,
};
pub use snapshot::{restore_base as restore_snapshot_base, RestoredBase};
pub use turn::spawn;

mod account {
    use cloud_backup_protocol::v2::context::ContextReadRequest;
    use cloud_backup_protocol::v2::Version;
    use cloud_backup_protocol::{AccountId, CloudScope};

    use crate::host::Host;

    use super::http::{self, Endpoint};

    /// Publisher account scope from the saved connection. Receivers do not call this.
    pub(super) async fn publisher_scope(
        host: &Host,
    ) -> Result<(CloudScope, Endpoint, &'static str), String> {
        let selected = http::selected_service(&host.home)?;
        let account = account_of(host)?;
        let endpoint =
            Endpoint::open(&selected.api_url, selected.label)?.load_owner_key(&host.home);
        let backup = crate::cloud::client_from_home(&host.home)?;
        let context = backup
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id: account.clone(),
                required_destinations: Vec::new(),
            })
            .await
            .map_err(|_| "cloud account context failed")?;
        if context.scope.account_id != account {
            return Err("cloud account does not match this node key".into());
        }
        Ok((context.scope, endpoint, selected.label))
    }

    fn account_of(host: &Host) -> Result<AccountId, String> {
        let derived = crate::identity::user_hash_from_pubkey(&host.public_key())?;
        if derived != host.user_hash {
            return Err("node account id does not match the node key".into());
        }
        AccountId::try_from(derived).map_err(|_| "node account id is not a cloud account id".into())
    }
}

mod files {
    use cloud_backup_protocol::v2::blobs::BlobPath;
    use cloud_backup_protocol::{canonical_bytes, Digest, Id};
    use sha2::{Digest as _, Sha256};

    /// Stable object path for one accepted file write.
    pub(super) fn path_for(
        database_id: &str,
        mutation_id: &str,
        canonical: &[u8],
    ) -> Result<BlobPath, ()> {
        let value: serde_json::Value = serde_json::from_slice(canonical).map_err(|_| ())?;
        if value.get("mutation_id").and_then(|item| item.as_str()) != Some(mutation_id) {
            return Err(());
        }
        let schema = value
            .get("schema")
            .and_then(|item| item.as_str())
            .ok_or(())?;
        let space = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
        let key = value.get("key").ok_or(())?;
        let key_bytes = canonical_bytes(key).map_err(|_| ())?;
        let record = fold_db::hex::hex_lower(Sha256::digest(key_bytes));
        let path = BlobPath {
            database: Id::try_from(database_id.to_owned()).map_err(|_| ())?,
            space: space.to_owned().try_into().map_err(|_| ())?,
            record: Digest::try_from(record).map_err(|_| ())?,
            write_id: mutation_id.to_owned().try_into().map_err(|_| ())?,
        };
        path.validate().map_err(|_| ())?;
        if path.database.as_str() != database_id {
            return Err(());
        }
        Ok(path)
    }
}

mod paths {
    pub(crate) const CREATE: &str = "/api/cloud/backups/v2/mutation-log/create-database";
    pub(crate) const ACCESS: &str = "/api/cloud/backups/v2/mutation-log/access";
    pub(crate) const APPEND: &str = "/api/cloud/backups/v2/mutation-log/append";
    pub(crate) const HEAD: &str = "/api/cloud/backups/v2/mutation-log/head";
    pub(crate) const LIST: &str = "/api/cloud/backups/v2/mutation-log/list";
    pub(super) const SETTLE_SUBMISSIONS: &str =
        "/api/cloud/backups/v2/mutation-log/settle-submissions";
    pub(crate) use cloud_backup_protocol::v2::share_access::{
        MUTATION_LOG_DELIVERY_OBJECT_PATH as DELIVERY_OBJECT,
        MUTATION_LOG_DELIVERY_SNAPSHOT_PATH as DELIVERY_SNAPSHOT,
    };
}
