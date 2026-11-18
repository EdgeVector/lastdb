//! Read the share settings for a received database.
//!
//! The call is the settings get, signed the same way as the member's own
//! entry. No stored statement is members and proposals. A statement whose
//! signature does not verify is not a setting this node will show.

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::share_access::{
    absent_share_settings, ShareEnvironment, ShareRead, ShareSettings, ShareSettingsGetRequest,
    ShareSettingsGetResponse, ShareWrite, SHARE_SETTINGS_GET_PATH,
};
use cloud_backup_protocol::v2::Version;

use super::super::cloud::CloudLog;
use super::super::http::Proof;

pub(super) fn default_read() -> ShareRead {
    ShareRead::Members
}

pub(super) fn default_write() -> ShareWrite {
    ShareWrite::Proposals
}

pub(super) async fn read(log: &CloudLog) -> Option<(ShareRead, ShareWrite)> {
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
        return Some(absent_share_settings());
    };
    let environment = ShareEnvironment::parse(log.environment()).ok()?;
    if settings.environment != environment {
        return None;
    }
    verify(&settings)?;
    Some((settings.read, settings.write))
}

fn verify(settings: &ShareSettings) -> Option<()> {
    let key_bytes = fold_db::hex::hex_decode_array(settings.publisher_public_key.as_str())?;
    let key = app_identity_crypto::VerifyingKey::from_bytes(&key_bytes).ok()?;
    let signature = fold_db::hex::hex_decode_array(settings.signature.as_str())?;
    let digest = settings.digest().ok()?;
    app_identity_crypto::verify(&key, &signature, digest.as_str().as_bytes()).ok()
}
