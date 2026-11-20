//! Recipient contact identity for open-link share registrations.
//!
//! Sealed ExeMem invitations are deleted. Access is the signed grant chain;
//! People wraps a local open-link token in a hand-off consent code.

mod contact;
pub use contact::{ShareContact, ShareRecipient};

#[derive(Debug)]
pub struct ShareInvitationError(&'static str);

impl std::fmt::Display for ShareInvitationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ShareInvitationError {}

pub type Result<T> = std::result::Result<T, ShareInvitationError>;

/// Load the recipient contact authority from a LastDB identity home.
pub fn recipient(home: &std::path::Path) -> std::result::Result<ShareRecipient, String> {
    let seed = crate::offline_home::Directory::open(home)
        .map_err(|e| e.to_string())?
        .identity_seed()?
        .ok_or("share requires an initialized LastDB identity")?;
    ShareRecipient::from_identity_seed(&seed).map_err(|e| e.to_string())
}

fn verify_signature(key: &[u8; 32], signature: &[u8; 64], message: &[u8]) -> Result<()> {
    let invalid = || ShareInvitationError("invalid share invitation signature");
    let key = app_identity_crypto::VerifyingKey::from_bytes(key).map_err(|_| invalid())?;
    app_identity_crypto::verify(&key, signature, message).map_err(|_| invalid())
}
