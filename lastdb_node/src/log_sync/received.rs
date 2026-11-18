//! Pull a registered signed log into the receiver store and track member-list access.
//!
//! This does not publish and does not call `accept_signed_log`. The member
//! revision comes from the standard member-list read, with no API key.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    MemberListReadRequest, MemberListReadResponse, ShareMemberRole, ShareRead, ShareWrite,
    ShareWriteScope, SHARE_MEMBER_LIST_READ_PATH,
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
#[path = "share_settings.rs"]
mod share_settings;

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

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccessView {
    state: AccessState,
    role: Option<ShareMemberRole>,
    #[serde(default = "share_settings::default_read")]
    read: ShareRead,
    #[serde(default = "share_settings::default_write")]
    write: ShareWrite,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    write_schemas: Option<ShareWriteScope>,
    revision: Option<DecimalU64>,
    confirmed_at_ms: Option<DecimalU64>,
    stale: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AccessState {
    Unknown,
    Active,
    Revoked,
    Absent,
}

impl AccessView {
    fn unknown() -> Self {
        Self {
            state: AccessState::Unknown,
            role: None,
            read: ShareRead::Members,
            write: ShareWrite::Proposals,
            write_schemas: None,
            revision: None,
            confirmed_at_ms: None,
            stale: true,
        }
    }

    pub(crate) fn can_submit(&self) -> bool {
        !self.stale && self.state == AccessState::Active
    }
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
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "received log is unavailable");
            return Pause::Retry;
        }
    };
    let Some(log) = refresh(host, share, log).await else {
        return Pause::Retry;
    };
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
    record(&host.home, share.id.as_str(), confirm, settings);
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

pub(crate) fn view(home: &Path, id: &str) -> AccessView {
    let mut result = disk::read_limited(&self_access_path(home, id), MEMBER_LIMIT)
        .ok()
        .flatten()
        .and_then(|bytes| serde_json::from_slice::<AccessView>(&bytes).ok())
        .unwrap_or_else(AccessView::unknown);
    let now = fold_db::clock::unix_millis();
    result.stale |= result
        .confirmed_at_ms
        .is_none_or(|stamp| now.checked_sub(stamp.get()).is_none_or(|age| age > 60_000));
    result
}

fn record(
    home: &Path,
    id: &str,
    observed: Option<&ObservedAccess>,
    settings: Option<(ShareRead, ShareWrite)>,
) {
    let mut previous = view(home, id);
    let result = match observed {
        Some(observed) => confirmed(observed, settings, &previous),
        None if previous.stale => return,
        None => {
            previous.stale = true;
            save_access(home, id, &previous);
            return;
        }
    };
    if previous.state == result.state
        && previous.role == result.role
        && previous.read == result.read
        && previous.write == result.write
        && previous.revision == result.revision
        && previous.write_schemas == result.write_schemas
        && previous.stale == result.stale
        && previous
            .confirmed_at_ms
            .zip(result.confirmed_at_ms)
            .is_some_and(|(a, b)| b.get().checked_sub(a.get()).is_some_and(|age| age < 10_000))
    {
        return;
    }
    save_access(home, id, &result);
}

fn save_access(home: &Path, id: &str, result: &AccessView) {
    if let Ok(bytes) = serde_json::to_vec(result) {
        let _ = disk::write_private(&self_access_path(home, id), &bytes);
    }
}

fn confirmed(
    observed: &ObservedAccess,
    settings: Option<(ShareRead, ShareWrite)>,
    previous: &AccessView,
) -> AccessView {
    let (read, write) = settings.unwrap_or((previous.read, previous.write));
    let (state, role, revision, write_schemas) = match observed {
        ObservedAccess::Active {
            revision,
            role,
            write_schemas,
        } => (
            AccessState::Active,
            *role,
            Some(DecimalU64::new(*revision)),
            write_schemas.clone(),
        ),
        ObservedAccess::Revoked => (AccessState::Revoked, None, None, None),
        ObservedAccess::Absent => (AccessState::Absent, None, None, None),
    };
    AccessView {
        state,
        role,
        read,
        write,
        revision,
        write_schemas,
        confirmed_at_ms: Some(DecimalU64::new(fold_db::clock::unix_millis())),
        stale: false,
    }
}

fn self_access_path(home: &Path, id: &str) -> PathBuf {
    home.join("log-sync").join(id).join("self-access.json")
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
