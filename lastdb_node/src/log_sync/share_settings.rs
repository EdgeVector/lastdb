//! Read the share settings for a received database.
//!
//! The call is the settings get, signed the same way as the member's own
//! entry. No stored statement is members, proposals, and an empty map. A
//! statement whose signature does not verify is not a setting this node will
//! show.

use std::collections::BTreeMap;

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    absent_share_settings, ShareEnvironment, ShareSettings, ShareSettingsGetRequest,
    ShareSettingsGetResponse, SHARE_SETTINGS_GET_PATH,
};
use cloud_backup_protocol::v2::Version;

use super::super::cloud::CloudLog;
use super::super::http::Proof;
use super::self_access::SettingsSnapshot;

pub(super) async fn read(log: &CloudLog) -> Option<SettingsSnapshot> {
    let request = ShareSettingsGetRequest {
        version: Version::V2,
        stream: log.share_stream(),
    };
    request.validate().ok()?;
    let (status, bytes) = log
        .post_status(
            SHARE_SETTINGS_GET_PATH,
            canonical_bytes(&request).ok()?,
            Proof::SelfAccess,
        )
        .await
        .ok()?;
    if !(200..300).contains(&status) {
        return None;
    }
    let reply = serde_json::from_slice::<ShareSettingsGetResponse>(&bytes).ok()?;
    reply.validate_for(&request).ok()?;
    let Some(settings) = reply.settings else {
        let (read, write) = absent_share_settings();
        return Some(SettingsSnapshot {
            read,
            write,
            member_writes: BTreeMap::new(),
        });
    };
    let environment = ShareEnvironment::parse(log.environment()).ok()?;
    if settings.environment != environment {
        return None;
    }
    verify(&settings)?;
    Some(SettingsSnapshot {
        read: settings.read,
        write: settings.write,
        member_writes: settings.member_writes,
    })
}

fn verify(settings: &ShareSettings) -> Option<()> {
    let key_bytes = fold_db::hex::hex_decode_array(settings.publisher_public_key.as_str())?;
    let key = app_identity_crypto::VerifyingKey::from_bytes(&key_bytes).ok()?;
    let signature = fold_db::hex::hex_decode_array(settings.signature.as_str())?;
    let digest = settings.digest().ok()?;
    app_identity_crypto::verify(&key, &signature, digest.as_str().as_bytes()).ok()
}
