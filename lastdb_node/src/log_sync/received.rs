//! Pull a registered signed log into the receiver store and track member-list access.
//!
//! This does not call `accept_signed_log`. A direct Submit member publishes
//! through the same path as an owned database. A link registration has no
//! member entry: it does not poll one and it does not publish. The member
//! revision comes from the standard member-list read, with no API key.
//! The stored view of that access is `self_access`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, MemberListReadResponse, ShareMemberRole, ShareWriteScope,
    SHARE_MEMBER_LIST_READ_PATH,
};
use cloud_backup_protocol::v2::{Stream, Version};
use cloud_backup_protocol::{DecimalU64, ErrorCode, Id, ProtocolError};
use mutation_log::{DatabaseId, DeviceId, PrincipalId};
use serde::{Deserialize, Serialize};

use crate::cloud_backup_client::ShareEnvironment;
use crate::host::Host;
use crate::profile::endpoints::{exemem_api_url_for, Environment};
use crate::registered_share::{self, RegisteredShare};

use super::cloud::{CloudLog, LogOpen};
use super::disk;
use super::http::{Endpoint, Proof};
use super::run::{self, Ready};
use super::session;
use super::turn::{self, Pause};
use super::SyncState;

// Beside this file. log_sync/mod.rs is owned by another change.
// self_access is visible in the crate so AccessView, the return type of view,
// is reachable. Callers use received::view and do not name the type.
#[path = "share_settings.rs"]
mod share_settings;

pub(crate) mod self_access;

pub(crate) use self_access::view;

const MEMBER_LIMIT: usize = 65_536;

pub(super) enum ObservedAccess {
    Active {
        revision: u64,
        role: Option<ShareMemberRole>,
        write_schemas: Option<ShareWriteScope>,
    },
    Revoked,
    Absent,
}

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
            super::publish_state::note_unreachable(
                database_id,
                &host.home,
                profile_service(share.environment()).0,
            );
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "received log is unavailable");
            return Pause::Retry;
        }
    };
    let log = if share.link {
        log
    } else {
        match refresh(host, share, log).await {
            Some(log) => log,
            None => return Pause::Retry,
        }
    };
    if let Err(error) = super::snapshot::receive::ensure_base(host.as_ref(), share, &log).await {
        tracing::warn!(target: "lastdbd::log_sync", error = %error, "received snapshot is unavailable");
        return Pause::Retry;
    }
    let Some(state) = receiver_state(share, &log, &device, &host.home) else {
        return Pause::Retry;
    };
    run::run_ready(
        host.as_ref(),
        Ready {
            database_id: database_id.to_owned(),
            publisher: Some(share.publisher.as_str().to_owned()),
            log,
            state,
        },
    )
    .await
}

async fn refresh(host: &Arc<Host>, share: &RegisteredShare, mut log: CloudLog) -> Option<CloudLog> {
    let observed = access(&log).await;
    let settings = if observed.is_some() {
        share_settings::read(&log).await
    } else {
        None
    };
    let confirm = match &observed {
        Some(ObservedAccess::Active { .. }) if settings.is_none() => None,
        Some(value) => Some(value),
        None => None,
    };
    self_access::record(&host.home, share.id.as_str(), confirm, settings.as_ref());
    let Some(ObservedAccess::Active {
        revision: fresh, ..
    }) = observed
    else {
        return Some(log);
    };
    if fresh <= log.member_revision {
        return Some(log);
    }
    if store_member(&host.home, share.id.as_str(), fresh).is_err() {
        return None;
    }
    log.member_revision = fresh;
    Some(log)
}

/// Open the publisher stream from the registration. No account context and no API key.
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
    let (label, api_url) = profile_service(share.environment());
    let endpoint = Endpoint::open(api_url, label)?;
    CloudLog::open(LogOpen {
        host,
        endpoint,
        scope: stream.scope.clone(),
        database_id: stream.stream_id.clone(),
        device,
        member_revision: revision,
        environment_label: label,
    })
}

pub(super) fn profile_service(environment: ShareEnvironment) -> (&'static str, &'static str) {
    let profile = match environment {
        ShareEnvironment::Dev => Environment::Dev,
        ShareEnvironment::Prod => Environment::Prod,
    };
    (profile.as_str(), exemem_api_url_for(profile))
}

fn receiver_state(
    share: &RegisteredShare,
    log: &CloudLog,
    device: &str,
    home: &Path,
) -> Option<SyncState> {
    let database_id = DatabaseId::new(share.id.as_str()).ok()?;
    let _publisher = PrincipalId::new(share.publisher.as_str()).ok()?;
    let device = DeviceId::new(device).ok()?;
    if log.device() != &device {
        return None;
    }
    let cursor = session::load_cursor(home, database_id.as_str()).ok()?;
    Some(SyncState { cursor })
}

pub(super) async fn revision(log: &CloudLog) -> Option<u64> {
    match read_member_access(log, Proof::Data).await {
        Some(ObservedAccess::Active { revision, .. }) => Some(revision),
        _ => None,
    }
}

pub(super) async fn access(log: &CloudLog) -> Option<ObservedAccess> {
    read_member_access(log, Proof::SelfAccess).await
}

async fn read_member_access(log: &CloudLog, proof: Proof) -> Option<ObservedAccess> {
    let request = MemberListReadRequest {
        version: Version::V2,
        stream: Stream {
            scope: log.scope.clone(),
            stream_id: Id::try_from(log.database_id().to_owned()).ok()?,
        },
    };
    request.validate().ok()?;
    let (status, bytes) = log
        .post_status(
            SHARE_MEMBER_LIST_READ_PATH,
            canonical_bytes(&request).ok()?,
            proof,
        )
        .await
        .ok()?;
    if (200..300).contains(&status) {
        return active_access(&bytes, &request.stream);
    }
    let refusal: ProtocolError = serde_json::from_slice(&bytes).ok()?;
    match (refusal.code, refusal.field.as_str()) {
        (ErrorCode::Unauthorized, "share.grant.revoked") => Some(ObservedAccess::Revoked),
        (ErrorCode::Unauthorized, "share.grant.absent") => Some(ObservedAccess::Absent),
        _ => None,
    }
}

#[derive(Deserialize)]
struct AccessRevision {
    version: Version,
    database_id: Id,
    revision: DecimalU64,
    role: Option<ShareMemberRole>,
    write_schemas: Option<ShareWriteScope>,
}

fn active_access(bytes: &[u8], stream: &Stream) -> Option<ObservedAccess> {
    if let Ok(view) = serde_json::from_slice::<AccessRevision>(bytes) {
        if view.version != Version::V2 || view.database_id != stream.stream_id {
            return None;
        }
        return Some(ObservedAccess::Active {
            revision: view.revision.get(),
            role: view.role,
            write_schemas: view.write_schemas,
        });
    }
    let view = serde_json::from_slice::<MemberListReadResponse>(bytes).ok()?;
    if view.version != Version::V2 || view.stream != *stream {
        return None;
    }
    Some(ObservedAccess::Active {
        revision: view.revision.get(),
        role: None,
        write_schemas: None,
    })
}

fn member_revision(home: &Path, database_id: &str) -> u64 {
    let Some(bytes) = disk::read_limited(&member_path(home, database_id), MEMBER_LIMIT)
        .ok()
        .flatten()
    else {
        return 0;
    };
    serde_json::from_slice::<MemberFile<String>>(&bytes)
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

#[derive(Serialize, Deserialize)]
struct MemberFile<S> {
    database_id: S,
    revision: u64,
}
