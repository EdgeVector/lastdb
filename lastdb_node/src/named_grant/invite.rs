//! Named-database AES key inside the existing share invitation.
//!
//! The bytes are `share_invitation::seal_invitation`. That is E3's envelope.
//! It holds the AES content key, not the node seed and not the personal
//! content key. There is no second prefix and no raw key file.

use crate::cloud_backup_client::byte_backup::GuestReadPermission;
use crate::host::owned;
use crate::share_invitation::{self, ShareContact};
use cloud_backup_protocol::v2::Stream;
use cloud_backup_protocol::Id;
use fold_db::crypto::E2eKeys;
use std::path::Path;
use zeroize::Zeroizing;

/// Seal this database's AES key to `contact`. Opens a fresh publisher session.
pub async fn seal_named_invitation(
    home: &Path,
    database_id: &str,
    contact: &ShareContact,
    lifetime_hours: u32,
) -> Result<Vec<u8>, String> {
    if !(1..=168).contains(&lifetime_hours) {
        return Err("share invitation lifetime must be between 1 and 168 hours".into());
    }
    let id = Id::try_from(database_id.to_owned())
        .map_err(|_| "database is not registered on this node")?;
    let session = super::session::open(home).await?;
    let key = owned::content_encryption_key(&session.seed, id.as_str())?;
    refuse_invitable(&session.seed, &key)?;
    let permission = read_permission(&session, id, lifetime_hours)?;
    let sealed = share_invitation::seal_invitation(contact, &permission, &key, &session.signer)
        .map_err(|error| error.to_string())?;
    if sealed.len() > share_invitation::MAX_SEALED_INVITATION_BYTES {
        return Err("sealed share invitation exceeds its size limit".into());
    }
    Ok(sealed)
}

fn read_permission(
    session: &super::session::PublisherSession,
    id: Id,
    lifetime_hours: u32,
) -> Result<GuestReadPermission, String> {
    let expires = chrono::Utc::now() + chrono::Duration::hours(i64::from(lifetime_hours));
    GuestReadPermission::sign(
        Stream {
            scope: session.scope.clone(),
            stream_id: id,
        },
        session.guest_environment,
        &expires.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        session.signer.public_key_bytes(),
        |bytes| session.signer.sign(bytes).to_bytes(),
    )
    .map_err(|error| error.to_string())
}

fn refuse_invitable(seed: &[u8; 32], key: &[u8; 32]) -> Result<(), String> {
    let personal = Zeroizing::new(
        E2eKeys::from_ed25519_seed(seed)
            .map_err(|_| "cannot derive personal content key")?
            .encryption_key(),
    );
    if key == seed || key.iter().all(|byte| *byte == 0) || key == personal.as_ref() {
        return Err("named database content key is not invitable".into());
    }
    Ok(())
}
