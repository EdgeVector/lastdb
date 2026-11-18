//! Sign and check one publisher settings statement.
//!
//! The service decides whether a statement is the next one. This module only
//! builds the statement the publisher asked for and checks that a stored
//! statement is the one this publisher signed.

use super::{entries, PublisherSession};
use cloud_backup_protocol::v2::share_access::{
    absent_share_settings, Ed25519Signature, ShareRead, ShareSettings, ShareWrite,
};
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::DecimalU64;

pub(super) fn mode(current: Option<&ShareSettings>) -> (ShareRead, ShareWrite) {
    current.map_or_else(absent_share_settings, |settings| {
        (settings.read, settings.write)
    })
}

/// The next statement, or nothing when the caller left the stored pair as it is.
///
/// An omitted `read` or `write` keeps the stored value. No stored statement
/// is members and proposals. The same pair is not signed: the service refuses
/// it, and a repeat of a call whose answer was lost must not send it again.
pub(super) fn prepare(
    session: &PublisherSession,
    database_id: &str,
    current: Option<&ShareSettings>,
    read: Option<ShareRead>,
    write: Option<ShareWrite>,
) -> Result<Option<ShareSettings>, String> {
    if read.is_none() && write.is_none() {
        return Ok(None);
    }
    let (stored_read, stored_write) = mode(current);
    let next_read = read.unwrap_or(stored_read);
    let next_write = write.unwrap_or(stored_write);
    if next_read == stored_read && next_write == stored_write {
        return Ok(None);
    }
    sign(session, database_id, current, next_read, next_write).map(Some)
}

/// Accept only a statement this publisher signed for this database.
pub(super) fn verify(
    session: &PublisherSession,
    database_id: &str,
    settings: &ShareSettings,
) -> Result<(), String> {
    if settings.environment != session.environment
        || settings.stream.stream_id.as_str() != database_id
        || settings.stream.scope != session.scope
        || settings.publisher_public_key != session.publisher
    {
        return Err("share.settings.identity".into());
    }
    let key = app_identity_crypto::VerifyingKey::from_bytes(&session.signer.public_key_bytes())
        .map_err(|_| "share.settings.identity")?;
    let signature =
        fold_db::hex::hex_decode_array(settings.signature.as_str()).ok_or("share.signature")?;
    let digest = settings.digest().map_err(|error| error.field)?;
    app_identity_crypto::verify(&key, &signature, digest.as_str().as_bytes())
        .map_err(|_| "share.signature".to_owned())
}

fn sign(
    session: &PublisherSession,
    database_id: &str,
    current: Option<&ShareSettings>,
    read: ShareRead,
    write: ShareWrite,
) -> Result<ShareSettings, String> {
    let sequence = current
        .map_or(Some(1), |settings| settings.sequence.get().checked_add(1))
        .ok_or("share.settings.sequence")?;
    let now = fold_db::clock::unix_millis();
    if now == 0 {
        return Err("invalid clock for settings update".into());
    }
    let mut settings = ShareSettings {
        version: Version::V2,
        environment: session.environment,
        stream: entries::stream(session, database_id)?,
        read,
        write,
        sequence: DecimalU64::new(sequence),
        publisher_public_key: session.publisher.clone(),
        signed_at_ms: DecimalU64::new(now),
        signature: Ed25519Signature::try_from("0".repeat(128)).map_err(|_| "share.signature")?,
    };
    settings
        .validate_successor_of(current)
        .map_err(|error| error.field)?;
    let digest = settings.digest().map_err(|error| error.field)?;
    let signature = session.signer.sign(digest.as_str().as_bytes()).to_bytes();
    settings.signature = Ed25519Signature::try_from(fold_db::hex::hex_lower(signature))
        .map_err(|_| "share.signature")?;
    Ok(settings)
}
