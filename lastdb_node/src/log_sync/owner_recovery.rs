//! Replay a fixed signed owner prefix into an isolated new home.

use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use cloud_backup_protocol::{CloudScope, Id};
use mutation_log::{ListPage, LogError, MutationLog};
use zeroize::Zeroizing;

use crate::database_replay::{self, SnapshotEntry};
use crate::host::owned::Resolved;
use crate::host::Host;
use crate::log_envelope::{self, DatabaseContentKey, IncorporatedRef, LogBinding};
use crate::node_read::CanonicalPost;

use super::cloud::{CloudLog, LogOpen};
use super::http::{self, Endpoint, Proof};
use super::status::NativeView;
use super::{apply, paths, restore, session, status, wire};

/// Delivery cursor applied into the fresh home, and the local database name.
pub struct RecoveredPrefix {
    /// Last delivery sequence applied. Zero is an empty log.
    pub through: u64,
    /// `personal` when the envelope used the owner key. Otherwise the sibling name.
    pub name: String,
}

/// Restore an owned log into a private store with the same cloud id.
/// The caller publishes the isolated home after successful shutdown.
pub async fn recover_owned(
    host: &Arc<Host>,
    connection_home: &Path,
    scope: CloudScope,
    database_id: &Id,
    name: &str,
) -> Result<RecoveredPrefix, String> {
    if scope.account_id.as_str() != host.user_hash {
        return Err("signed recovery account differs from the recovery key".into());
    }
    let selected = http::selected_service(connection_home)?;
    let device = session::device_id(&host.home)?;
    let log = CloudLog::open(LogOpen {
        host,
        endpoint: Endpoint::open(&selected.api_url, selected.label)?,
        scope,
        database_id: database_id.clone(),
        device: &device,
        member_revision: 0,
        environment_label: selected.label,
    })?;
    let body = wire::database_body(log.database_id())
        .map_err(|_| "signed recovery could not name the database")?;
    let (status_code, bytes) = log
        .post_status(paths::ACCESS, body, Proof::Canonical(CanonicalPost::Access))
        .await
        .map_err(|_| "the signed cloud database is unavailable to this owner".to_owned())?;
    if !(200..300).contains(&status_code)
        || status::native_body(&bytes, log.database_id(), log.environment()) != NativeView::Live
    {
        return Err("the signed cloud database is unavailable to this owner".into());
    }
    let through = head(&log).await?;
    let placed = place(host, &log, name, database_id, through).await?;
    apply_through(&log, through).await?;
    session::store_cursor(&host.home, database_id.as_str(), through)?;
    session::mark_prefix(&host.home, database_id.as_str(), through)?;
    Ok(RecoveredPrefix {
        through,
        name: placed,
    })
}

async fn place(
    host: &Arc<Host>,
    log: &CloudLog,
    name: &str,
    database_id: &Id,
    through: u64,
) -> Result<String, String> {
    match host.owned.registered_store(database_id.as_str()) {
        Ok((Resolved::Owner, _)) => return Ok("personal".to_owned()),
        Ok(_) => {}
        Err(error) => return Err(error.status().1.to_owned()),
    }
    if owner_envelope(host, log, through).await? {
        host.owned
            .adopt_owner_id(&host.home, database_id.as_str())
            .map_err(|error| error.status().1.to_owned())?;
        return Ok("personal".to_owned());
    }
    host.owned
        .register_recovered(&host.home, Arc::clone(&host.keypair), name, database_id)
        .await
        .map_err(|error| error.status().1.to_owned())?;
    Ok(name.to_owned())
}

async fn head(log: &CloudLog) -> Result<u64, String> {
    let log = log.clone();
    tokio::task::spawn_blocking(move || log.head(log.principal()))
        .await
        .map_err(|_| "signed recovery head did not complete")?
        .map(|head| head.map_or(0, |head| head.cursor))
        .map_err(|error| error.to_string())
}

async fn owner_envelope(host: &Host, log: &CloudLog, through: u64) -> Result<bool, String> {
    if through == 0 {
        return Ok(false);
    }
    match read_page(log, 0).await {
        Ok(page) => {
            let stored = page
                .entries
                .first()
                .ok_or("signed recovery log has no entry")?;
            owner_opens(
                host,
                log.database_id(),
                &stored_binding(log.database_id(), stored),
                &stored.entry.payload,
            )
        }
        Err(LogError::CursorExpired { .. }) => {
            let entry = restore::sample_entry(log)
                .await
                .map_err(|error| error.to_string())?;
            let payload = decode_payload(&entry)?;
            owner_opens(
                host,
                log.database_id(),
                &snapshot_binding(log.database_id(), &entry),
                &payload,
            )
        }
        Err(error) => Err(error.to_string()),
    }
}

fn owner_opens(
    host: &Host,
    database_id: &str,
    binding: &LogBinding<'_>,
    payload: &[u8],
) -> Result<bool, String> {
    let seed = Zeroizing::new(host.keypair.secret_key_bytes());
    let key = DatabaseContentKey::personal(&seed, database_id)
        .map_err(|_| "signed recovery could not derive the personal content key".to_owned())?;
    match log_envelope::open(&key, binding, payload) {
        Ok(_) => Ok(true),
        Err(log_envelope::LogEnvelopeError::Open) => Ok(false),
        Err(_) => Err("signed recovery could not read the database envelope".into()),
    }
}

fn stored_binding<'a>(
    database_id: &'a str,
    stored: &'a mutation_log::StoredEntry,
) -> LogBinding<'a> {
    let entry = &stored.entry;
    LogBinding {
        database_id,
        writer_key: entry.writer.key.as_str(),
        writer_device: entry.writer.device.as_str(),
        writer_seq: entry.writer_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: entry.namespace.as_str(),
        incorporated_from: entry.incorporated_from.as_ref().map(stored_source),
    }
}

fn stored_source(source: &mutation_log::Incorporation) -> IncorporatedRef<'_> {
    IncorporatedRef {
        writer_key: source.writer.key.as_str(),
        writer_device: source.writer.device.as_str(),
        writer_seq: source.writer_seq,
    }
}

fn snapshot_binding<'a>(database_id: &'a str, entry: &'a SnapshotEntry) -> LogBinding<'a> {
    LogBinding {
        database_id,
        writer_key: &entry.writer_key,
        writer_device: &entry.writer_device,
        writer_seq: entry.writer_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: &entry.namespace,
        incorporated_from: entry.incorporated_from.as_ref().map(snapshot_source),
    }
}

fn snapshot_source(source: &database_replay::IncorporatedFrom) -> IncorporatedRef<'_> {
    IncorporatedRef {
        writer_key: &source.writer_key,
        writer_device: &source.writer_device,
        writer_seq: source.writer_seq,
    }
}

fn decode_payload(entry: &SnapshotEntry) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(entry.payload_b64.as_bytes())
        .map_err(|_| "signed recovery snapshot entry is not sealed".to_owned())
}

async fn read_page(log: &CloudLog, after: u64) -> Result<ListPage, LogError> {
    let log = log.clone();
    tokio::task::spawn_blocking(move || {
        log.list(log.principal(), after, mutation_log::LIST_PAGE_LIMIT, 0)
    })
    .await
    .map_err(|_| LogError::UnexpectedOutcome)?
}

async fn apply_through(log: &CloudLog, through: u64) -> Result<(), String> {
    let mut cursor = 0u64;
    let mut recovered_snapshot = false;
    let mut files = restore::file_source(log.clone());
    while cursor < through {
        let page = match read_page(log, cursor).await {
            Ok(page) => page,
            Err(LogError::CursorExpired { .. }) if !recovered_snapshot => {
                let installed = restore::install(log.clone())
                    .await
                    .map_err(|error| error.to_string())?;
                if installed <= cursor || installed > through {
                    return Err("signed recovery snapshot moved past the selected head".into());
                }
                cursor = installed;
                recovered_snapshot = true;
                continue;
            }
            Err(error) => return Err(error.to_string()),
        };
        let mut delivered = Vec::new();
        let expected = page.next.min(through);
        for stored in page
            .entries
            .iter()
            .take_while(|entry| entry.delivery_seq <= through)
        {
            if stored.delivery_seq != cursor.saturating_add(1) {
                return Err("signed recovery log has a delivery gap".into());
            }
            cursor = stored.delivery_seq;
            delivered.push(apply::delivered_entry(stored));
        }
        if delivered.is_empty() || cursor != expected {
            return Err("signed recovery log stopped before the selected head".into());
        }
        database_replay::replay_delivered(
            log.host(),
            log.database_id(),
            log.principal().as_str(),
            &delivered,
            &mut files,
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    Ok(())
}
