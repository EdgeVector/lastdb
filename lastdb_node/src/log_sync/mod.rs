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
mod http;
mod index_history;
mod native;
mod native_reset;
mod owner_recovery;
mod received;
pub(crate) mod received_access;
mod restore;
mod retention;
mod run;
mod session;
mod snapshot;
mod turn;
mod upload;
mod wire;

use crate::profile::private_fs as disk;

pub(crate) use contributions::{
    schema_id, status as share_write_status, submit as submit_share_write, validate_write_scopes,
    ContributionError,
};
pub use index_history::{index_history, IndexHistoryReport};
pub use native_reset::install_prepared_native_recovery;
pub use owner_recovery::{recover_owned, RecoveredPrefix};
pub(crate) use restore::SnapshotFiles;
pub use retention::{reclaim as reclaim_snapshot_log, ReclaimReport};
pub(crate) use session::ensure_wake;
pub use session::{publish_first_backup_intent, request_wake};
pub(crate) use snapshot::{
    completed as snapshot_completed, error as snapshot_error, request as request_snapshot,
};
pub use snapshot::{restore_base as restore_snapshot_base, RestoredBase};
pub use turn::spawn;

mod account {
    use std::sync::Arc;

    use cloud_backup_protocol::CloudScope;

    use crate::cloud::load_connected_home_context;
    use crate::host::Host;

    use super::http::Endpoint;

    /// Publisher account scope from the saved connection. Receivers do not call this.
    pub(super) async fn publisher_scope(
        host: &Host,
    ) -> Result<(CloudScope, Endpoint, &'static str), String> {
        let ctx = load_connected_home_context(&host.home, Some(Arc::clone(&host.keypair))).await?;
        if ctx.account_id.as_str() != host.user_hash {
            return Err("node account id does not match the node key".into());
        }
        let endpoint = Endpoint::open(ctx.api_url, ctx.env.as_str())?.load_owner_key(&host.home);
        Ok((ctx.scope, endpoint, ctx.env.as_str()))
    }
}

mod retry {
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Serialize};

    use super::disk;

    const META_LIMIT: usize = 4096;
    const SEAL_LIMIT: usize = 1024 * 1024 + 4096;

    #[derive(Clone, Serialize, Deserialize)]
    pub(super) struct Seal {
        pub writer_seq: u64,
        pub authored_at_ms: u64,
        pub namespace: String,
    }

    pub(super) fn load(
        home: &Path,
        database_id: &str,
        mutation_id: &str,
    ) -> Result<Option<(Seal, Vec<u8>)>, ()> {
        if !crate::profile::valid_label_128(mutation_id) {
            return Err(());
        }
        let Some(meta) = disk::read_limited(&meta_path(home, database_id, mutation_id), META_LIMIT)
            .map_err(|_| ())?
        else {
            return Ok(None);
        };
        let seal: Seal = serde_json::from_slice(&meta).map_err(|_| ())?;
        if seal.writer_seq == 0 || seal.namespace.is_empty() {
            return Err(());
        }
        let Some(bytes) =
            disk::read_limited(&seal_path(home, database_id, mutation_id), SEAL_LIMIT)
                .map_err(|_| ())?
        else {
            return Err(());
        };
        if bytes.is_empty() {
            return Err(());
        }
        Ok(Some((seal, bytes)))
    }

    pub(super) fn store(
        home: &Path,
        database_id: &str,
        mutation_id: &str,
        seal: &Seal,
        envelope: &[u8],
    ) -> Result<(), ()> {
        if !crate::profile::valid_label_128(mutation_id)
            || envelope.is_empty()
            || seal.writer_seq == 0
        {
            return Err(());
        }
        let meta = serde_json::to_vec(seal).map_err(|_| ())?;
        disk::write_private(&seal_path(home, database_id, mutation_id), envelope)
            .map_err(|_| ())?;
        disk::write_private(&meta_path(home, database_id, mutation_id), &meta).map_err(|_| ())
    }

    pub(super) fn store_file(
        home: &Path,
        database_id: &str,
        mutation_id: &str,
        ciphertext: &[u8],
    ) -> Result<(), ()> {
        if !crate::profile::valid_label_128(mutation_id) || ciphertext.is_empty() {
            return Err(());
        }
        disk::write_private(&file_path(home, database_id, mutation_id), ciphertext).map_err(|_| ())
    }

    pub(super) fn load_file(
        home: &Path,
        database_id: &str,
        mutation_id: &str,
    ) -> Result<Option<Vec<u8>>, ()> {
        if !crate::profile::valid_label_128(mutation_id) {
            return Err(());
        }
        let limit = fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES + 4096;
        let limit = usize::try_from(limit).map_err(|_| ())?;
        disk::read_limited(&file_path(home, database_id, mutation_id), limit).map_err(|_| ())
    }

    fn meta_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
        seal_dir(home, database_id).join(format!("{mutation_id}.json"))
    }

    fn seal_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
        seal_dir(home, database_id).join(format!("{mutation_id}.bin"))
    }

    fn file_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
        seal_dir(home, database_id).join(format!("{mutation_id}.file"))
    }

    fn seal_dir(home: &Path, database_id: &str) -> PathBuf {
        home.join("log-sync").join(database_id).join("seals")
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
