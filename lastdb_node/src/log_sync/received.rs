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

use crate::cloud_backup_client::byte_backup::ShareEnvironment;
use crate::host::Host;
use crate::node_read::CanonicalPost;
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use crate::registered_share::{self, RegisteredShare};

use super::cloud::{CloudLog, LogOpen};
use super::disk;
use super::http::{Endpoint, Proof};
use super::run::{self, Ready};
use super::session;
use super::turn::{self, Pause};

const MEMBER_LIMIT: usize = 65_536;

pub(super) async fn turn(host: &Arc<Host>) -> Pause {
    let Ok(shares) = registered_share::list(&host.home) else {
        return Pause::Retry;
    };
    let mut pause = Pause::Idle;
    for share in shares.into_iter().filter(|share| share.signed_log) {
        pause = turn::shorter(pause, one(host, &share).await);
    }
    pause
}

async fn one(host: &Arc<Host>, share: &RegisteredShare) -> Pause {
    let Ok(device) = session::device_id(&host.home) else {
        return Pause::Retry;
    };
    let database_id = share.id.as_str();
    let stored = member_revision(&host.home, database_id);
    let log = match connect_received(host, share, &device, stored) {
        Ok(log) => log,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "received log is unavailable");
            return Pause::Retry;
        }
    };
    let Some(log) = refresh(host, share, log, &device, stored).await else {
        return Pause::Retry;
    };
    let Some(state) = receiver_state(share, &log, &device, &host.home) else {
        return Pause::Retry;
    };
    let Ok(state) = log.resumed(state).await else {
        return Pause::Retry;
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
    connect_received(host, share, device, fresh).ok()
}

/// Open the publisher stream from the verified invitation. No account context and no API key.
fn connect_received(
    host: &Arc<Host>,
    share: &RegisteredShare,
    device: &str,
    revision: u64,
) -> Result<CloudLog, String> {
    let stream = share.stream();
    if stream.stream_id != share.id {
        return Err("registered invitation stream does not match the database".into());
    }
    stream
        .validate()
        .map_err(|_| "registered invitation stream was refused")?;
    let (label, api_url) = profile_service(share.environment());
    let endpoint = Endpoint::open(api_url, label)?;
    CloudLog::open(LogOpen {
        host,
        endpoint,
        scope: stream.scope.clone(),
        database_id: stream.stream_id.clone(),
        device,
        member_revision: revision,
        named: true,
        environment_label: label,
    })
}

fn profile_service(environment: ShareEnvironment) -> (&'static str, &'static str) {
    let profile = match environment {
        ShareEnvironment::Dev => Environment::Dev,
        ShareEnvironment::Prod => Environment::Prod,
    };
    (profile.as_str(), exemem_api_url_for(profile))
}

async fn read_revision(log: &CloudLog, share: &RegisteredShare) -> Option<u64> {
    let stream = share.stream().clone();
    if stream.scope != log.scope || stream.stream_id.as_str() != log.database_id() {
        return None;
    }
    let request = MemberListReadRequest {
        version: Version::V2,
        stream,
    };
    request.validate().ok()?;
    let body = canonical_bytes(&request).ok()?;
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
    revision_of(&bytes, &request.stream)
}

fn revision_of(bytes: &[u8], stream: &Stream) -> Option<u64> {
    if let Ok(view) = serde_json::from_slice::<MemberListRevision>(bytes) {
        if view.version != Version::V2 || view.database_id != stream.stream_id {
            return None;
        }
        return Some(view.revision.get());
    }
    let view = serde_json::from_slice::<MemberListReadResponse>(bytes).ok()?;
    if view.version != Version::V2 || view.stream != *stream {
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
        .map_or(0, |file| file.revision)
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
