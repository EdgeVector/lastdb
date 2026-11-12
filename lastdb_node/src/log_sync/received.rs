//! Pull a registered signed log into the one receiver store.
//!
//! This does not publish and does not call `accept_signed_log`. The member
//! revision comes from the standard member-list read, with no API key.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, MemberListReadResponse, MemberListRevision,
};
use cloud_backup_protocol::v2::{Stream, Version};
use mutation_log::{DatabaseId, DatabaseKind, DeviceId, PrincipalId};
use mutation_log_sync::{SyncRole, SyncState};
use serde::{Deserialize, Serialize};

use crate::host::Host;
use crate::node_read::CanonicalPost;
use crate::registered_share::{self, RegisteredShare};

use super::cloud::CloudLog;
use super::disk;
use super::http::Proof;
use super::run::{self, Ready};
use super::session;
use super::turn::{self, Pause};

const MEMBER_LIMIT: usize = 65_536;

pub(super) async fn turn(host: &Arc<Host>) -> Pause {
    let shares = match registered_share::list(&host.home) {
        Ok(shares) => shares,
        Err(_) => return Pause::Retry,
    };
    let mut pause = Pause::Idle;
    for share in shares.into_iter().filter(|share| share.signed_log) {
        pause = turn::shorter(pause, one(host, &share).await);
    }
    pause
}

async fn one(host: &Arc<Host>, share: &RegisteredShare) -> Pause {
    let device = match session::device_id(&host.home) {
        Ok(device) => device,
        Err(_) => return Pause::Retry,
    };
    let database_id = share.id.as_str();
    let stored = member_revision(&host.home, database_id);
    let log = match CloudLog::connect(host, database_id, &device, stored, true).await {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "received log is unavailable");
            return Pause::Retry;
        }
    };
    let log = match refresh(host, share, log, &device, stored).await {
        Some(log) => log,
        None => return Pause::Retry,
    };
    let state = match receiver_state(share, &log, &device, &host.home) {
        Some(state) => state,
        None => return Pause::Retry,
    };
    let state = match log.resumed(state).await {
        Ok(state) => state,
        Err(_) => return Pause::Retry,
    };
    run::run_ready(
        host.as_ref(),
        Ready {
            database_id: database_id.to_owned(),
            shareable: true,
            log,
            state,
        },
    )
    .await
}

async fn refresh(
    host: &Arc<Host>,
    share: &RegisteredShare,
    log: CloudLog,
    device: &str,
    stored: u64,
) -> Option<CloudLog> {
    let Some(fresh) = read_revision(&log, share).await else {
        return Some(log);
    };
    if fresh <= stored {
        return Some(log);
    }
    if store_member(&host.home, share.id.as_str(), fresh).is_err() {
        return None;
    }
    CloudLog::connect(host, share.id.as_str(), device, fresh, true)
        .await
        .ok()
}

async fn read_revision(log: &CloudLog, share: &RegisteredShare) -> Option<u64> {
    let stream = Stream {
        scope: log.scope.clone(),
        stream_id: share.id.clone(),
    };
    let body = canonical_bytes(&MemberListReadRequest {
        version: Version::V2,
        stream,
    })
    .ok()?;
    let (status, bytes) = log
        .post_status(
            CanonicalPost::MemberRead.path(),
            body,
            Proof::Canonical(CanonicalPost::MemberRead),
        )
        .await
        .ok()?;
    if !(200..300).contains(&status) {
        return None;
    }
    revision_of(&bytes, share.id.as_str())
}

fn revision_of(bytes: &[u8], database_id: &str) -> Option<u64> {
    if let Ok(view) = serde_json::from_slice::<MemberListRevision>(bytes) {
        if view.version != Version::V2 || view.database_id.as_str() != database_id {
            return None;
        }
        return Some(view.revision.get());
    }
    let view = serde_json::from_slice::<MemberListReadResponse>(bytes).ok()?;
    if view.version != Version::V2 || view.stream.stream_id.as_str() != database_id {
        return None;
    }
    Some(view.revision.get())
}

fn receiver_state(
    share: &RegisteredShare,
    log: &CloudLog,
    device: &str,
    home: &Path,
) -> Option<SyncState> {
    let database_id = DatabaseId::new(share.id.as_str()).ok()?;
    let publisher = PrincipalId::new(share.publisher.as_str()).ok()?;
    let principal = PrincipalId::new(log.principal().as_str()).ok()?;
    let device = DeviceId::new(device).ok()?;
    let cursor = session::load_cursor(home, share.id.as_str()).ok()?;
    SyncState::open(
        database_id,
        DatabaseKind::Shareable { publisher },
        SyncRole::Receiver { principal, device },
    )
    .ok()
    .map(|state| state.with_cursor(cursor))
}

fn member_revision(home: &Path, database_id: &str) -> u64 {
    let Some(bytes) = disk::read_limited(&member_path(home, database_id), MEMBER_LIMIT)
        .ok()
        .flatten()
    else {
        return 0;
    };
    serde_json::from_slice::<MemberFileOwned>(&bytes)
        .ok()
        .filter(|file| file.database_id == database_id)
        .map(|file| file.revision)
        .unwrap_or(0)
}

fn store_member(home: &Path, database_id: &str, revision: u64) -> Result<(), ()> {
    let bytes = serde_json::to_vec(&MemberFile {
        database_id,
        revision,
    })
    .map_err(|_| ())?;
    disk::write_private(&member_path(home, database_id), &bytes).map_err(|_| ())
}

fn member_path(home: &Path, database_id: &str) -> PathBuf {
    home.join("log-sync").join(database_id).join("member.json")
}

#[derive(Serialize)]
struct MemberFile<'a> {
    database_id: &'a str,
    revision: u64,
}

#[derive(Deserialize)]
struct MemberFileOwned {
    database_id: String,
    revision: u64,
}
