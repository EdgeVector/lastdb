//! The one place a member reads `read` and `write`.
//!
//! `kanban/share-config-settings` puts those fields on `AccessView`. This module
//! is the only call site. That branch can switch the source here. Until then the
//! settings get call is the source. A fetch that fails is unknown, not a refusal.

use std::sync::Arc;

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    absent_share_settings, ShareMemberRole, ShareRead, ShareSettingsGetRequest,
    ShareSettingsGetResponse, ShareWrite, SHARE_SETTINGS_GET_PATH,
};
use cloud_backup_protocol::v2::{Stream, Version};

use crate::host::Host;
use crate::registered_share::RegisteredShare;

use super::cloud::CloudLog;
use super::http::{Endpoint, Proof};
use super::received::{self, ObservedAccess};
use super::session;

pub(super) enum WriteMode {
    Direct,
    Closed,
    Unknown,
}

pub(crate) async fn direct_write_allowed(host: &Host, share: &RegisteredShare) -> bool {
    let Ok(log) = open_log(host, share) else {
        return false;
    };
    matches!(write_mode(&log).await, WriteMode::Direct)
}

pub(super) async fn write_mode(log: &CloudLog) -> WriteMode {
    let Some((_, write)) = database_settings(log).await else {
        return WriteMode::Unknown;
    };
    if write != ShareWrite::Direct {
        return WriteMode::Closed;
    }
    match received::access(log).await {
        Some(ObservedAccess::Active {
            role: Some(ShareMemberRole::Submit),
            ..
        }) => WriteMode::Direct,
        Some(_) => WriteMode::Closed,
        None => WriteMode::Unknown,
    }
}

async fn database_settings(log: &CloudLog) -> Option<(ShareRead, ShareWrite)> {
    let request = ShareSettingsGetRequest {
        version: Version::V2,
        stream: Stream {
            scope: log.scope.clone(),
            stream_id: cloud_backup_protocol::Id::try_from(log.database_id().to_owned()).ok()?,
        },
    };
    request.validate().ok()?;
    let (status, bytes) = log
        .post_status(
            SHARE_SETTINGS_GET_PATH,
            canonical_bytes(&request).ok()?,
            Proof::Data,
        )
        .await
        .ok()?;
    if !(200..300).contains(&status) {
        return None;
    }
    let response: ShareSettingsGetResponse = serde_json::from_slice(&bytes).ok()?;
    response.validate_for(&request).ok()?;
    Some(match &response.settings {
        Some(settings) => (settings.read, settings.write),
        None => absent_share_settings(),
    })
}

fn open_log(host: &Host, share: &RegisteredShare) -> Result<CloudLog, ()> {
    let device = session::device_id(&host.home).map_err(|_| ())?;
    let stream = share.stream();
    if stream.stream_id != share.id {
        return Err(());
    }
    let (label, api_url) = received::profile_service(share.environment());
    let endpoint = Endpoint::open(api_url, label).map_err(|_| ())?;
    CloudLog::open_offline(
        endpoint,
        stream.scope.clone(),
        stream.stream_id.clone(),
        &device,
        Arc::clone(&host.keypair),
        label,
    )
    .map_err(|_| ())
}
