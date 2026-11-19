//! Daemon adapter for the mutation log. A `proposals` share also drains the proposal log.
//! Decided state: docs/designs/shared-database-mutation-logs.md (mutation log, proposal log, share settings).
//!
//! Resolve a database by its registered id. Authorization determines the
//! permitted writes. Snapshot recovery installs into that database and the
//! sync loop acknowledges only after the delivered prefix is durable.

mod accepted;
mod automatic_retention;
mod blob_retention;
mod cloud;
mod cloud_state;
mod contributions;

pub(crate) use crate::named_grant::cloud_state::{database_cloud_state, CloudState};
pub(crate) use cloud_state::wait_for_enrollment;
mod direct;
mod publish_state;
pub(crate) use publish_state::row as publish_row;
mod http;
mod native;
mod native_reset;
mod owner_recovery;
pub(crate) mod received;
mod restore;
mod retention;
mod run;
mod served;
mod session;
mod share_mode;
mod snapshot;
mod turn;
mod upload;
mod wire;

use std::path::{Path, PathBuf};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use cloud_backup_protocol::Digest;
use fold_db::schema::types::Schema;
use mutation_log::{Entry, Incorporation, StoredEntry};

use crate::database_replay::{DeliveredEntry, IncorporatedFrom, RestoreError, SnapshotEntry};
use crate::host::{BlockedDelivery, Host};
use crate::log_envelope::{IncorporatedRef, LogBinding};
use crate::profile::private_fs as disk;

use cloud::CloudLog;

const MARKER_LIMIT: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SyncState {
    pub(super) cursor: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct SyncOutcome {
    pub(super) entries: Vec<StoredEntry>,
    pub(super) cursor: u64,
}

pub(crate) enum PagePlan {
    /// The page was handled through this delivery sequence.
    Ack(u64),
    /// Stopped mid-page. Ack only when a delivery sequence was fully handled.
    Hold(Option<u64>),
}

pub(super) struct PageFailure {
    pub(super) message: String,
    pub(super) blocked: Option<BlockedDelivery>,
}

impl From<String> for PageFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            blocked: None,
        }
    }
}

impl From<RestoreError> for PageFailure {
    fn from(error: RestoreError) -> Self {
        Self {
            message: error.to_string(),
            blocked: error.blocked_delivery(),
        }
    }
}

pub(crate) use contributions::{
    status as share_write_status, submit as submit_share_write, validate_write_scopes,
    ContributionError,
};
pub use native_reset::install_prepared_native_recovery;
pub use owner_recovery::{recover_owned, RecoveredPrefix};
pub(crate) use restore::SnapshotFiles;
pub use retention::{reclaim as reclaim_snapshot_log, ReclaimReport};
pub(crate) use session::ensure_wake;
pub use session::{publish_first_backup_intent, request_wake};
pub(crate) use share_mode::direct_write_allowed;
pub(crate) use snapshot::{
    completed as snapshot_completed, error as snapshot_error, request as request_snapshot,
};
pub use snapshot::{restore_base as restore_snapshot_base, RestoredBase};
pub use turn::spawn;

pub(crate) fn schema_id(schema: &Schema) -> Result<Digest, String> {
    let id = schema
        .identity_hash
        .as_deref()
        .ok_or("declared schema has no catalog identity")?;
    let portable = schema_types::PortableSchemaId::from_catalog(id, schema.to_wire())?;
    Digest::try_from(portable.as_str().to_owned()).map_err(|e| e.to_string())
}

pub(super) async fn apply_page(
    host: &Host,
    log: &CloudLog,
    outcome: &SyncOutcome,
    publisher: Option<&str>,
) -> Result<PagePlan, PageFailure> {
    let entries = &outcome.entries;
    let cursor = outcome.cursor;
    if let Some(publisher) = publisher {
        return received_page(host, log, publisher, entries, cursor)
            .await
            .map_err(Into::into);
    }
    let owned_private = host
        .owned
        .is_private_id(log.database_id())
        .map_err(|_| "owned database was not resolved".to_owned())?;
    if owned_private {
        let delivered: Vec<_> = entries.iter().map(delivered_entry).collect();
        let mut files = restore::file_source(log.clone());
        crate::database_replay::replay_owned(
            host,
            log.database_id(),
            Some(log.device().as_str()),
            &delivered,
            &mut files,
        )
        .await?;
        blob_retention::record_pulled_personal(host, log, entries)?;
        return Ok(PagePlan::Ack(cursor));
    }
    let mut handled = None;
    let whole = crate::host::owned::find_shareable(&host.home, log.database_id())
        .is_ok_and(|entry| entry.whole_database());
    if whole
        && entries.iter().any(|stored| {
            &stored.entry.writer.key == log.principal()
                && &stored.entry.writer.device != log.device()
        })
    {
        let reason = format!(
            "Shared database {} has a published write from another publisher device. This device cannot continue shared sync.",
            log.database_id()
        );
        host.cloud_sync.record_error(reason.clone());
        return Err(reason.into());
    }
    if whole {
        return served::apply_served(host, log, log.principal().as_str(), entries, cursor)
            .await
            .map_err(Into::into);
    }
    for stored in entries {
        let entry = &stored.entry;
        // An owned store may have independent local writes beyond its delivery
        // cursor. Only acknowledge writes this device already applied here.
        if entry.database_id.as_str() != log.database_id()
            || &entry.writer.key != log.principal()
            || &entry.writer.device != log.device()
            || !mark_local(host, log.database_id(), entry)?
        {
            return Ok(PagePlan::Hold(handled));
        }
        handled = Some(stored.delivery_seq);
    }
    Ok(PagePlan::Ack(cursor))
}

fn mark_local(host: &Host, database_id: &str, entry: &Entry) -> Result<bool, String> {
    let name = format!("{}-{}", entry.writer.device.as_str(), entry.writer_seq);
    if !session::label_ok(entry.writer.device.as_str()) {
        return Ok(false);
    }
    let marker = marker_path(&host.home, database_id, &name);
    match disk::read_limited(&marker, MARKER_LIMIT) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => disk::write_private(&marker, b"1")
            .map(|()| true)
            .map_err(|_| "applied marker was not stored".to_owned()),
        Err(_) => Ok(false),
    }
}

async fn received_page(
    host: &Host,
    log: &CloudLog,
    publisher: &str,
    entries: &[StoredEntry],
    cursor: u64,
) -> Result<PagePlan, String> {
    served::apply_served(host, log, publisher, entries, cursor).await
}

pub(super) fn delivered_entry(stored: &StoredEntry) -> DeliveredEntry {
    let entry = &stored.entry;
    DeliveredEntry {
        delivery_seq: stored.delivery_seq,
        database_id: entry.database_id.as_str().to_owned(),
        entry: SnapshotEntry {
            accepted_at_ms: stored.accepted_at_ms,
            writer_key: entry.writer.key.as_str().to_owned(),
            writer_device: entry.writer.device.as_str().to_owned(),
            writer_seq: entry.writer_seq,
            authored_at_ms: entry.authored_at_ms,
            namespace: entry.namespace.as_str().to_owned(),
            incorporated_from: entry.incorporated_from.as_ref().map(incorporated_from),
            payload_b64: URL_SAFE_NO_PAD.encode(&entry.payload),
        },
    }
}

fn incorporated_from(source: &Incorporation) -> IncorporatedFrom {
    IncorporatedFrom {
        writer_key: source.writer.key.as_str().to_owned(),
        writer_device: source.writer.device.as_str().to_owned(),
        writer_seq: source.writer_seq,
    }
}

pub(super) fn stored_binding<'a>(database_id: &'a str, stored: &'a StoredEntry) -> LogBinding<'a> {
    let entry = &stored.entry;
    LogBinding {
        database_id,
        writer_key: entry.writer.key.as_str(),
        writer_device: entry.writer.device.as_str(),
        writer_seq: entry.writer_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: entry.namespace.as_str(),
        incorporated_from: entry
            .incorporated_from
            .as_ref()
            .map(|source| IncorporatedRef {
                writer_key: source.writer.key.as_str(),
                writer_device: source.writer.device.as_str(),
                writer_seq: source.writer_seq,
            }),
    }
}

fn marker_path(home: &Path, database_id: &str, mutation_id: &str) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("applied")
        .join(mutation_id)
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
