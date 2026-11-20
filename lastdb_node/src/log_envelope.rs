//! Content-key checks shared by named-database invitation sealing.
//!
//! Unsigned mutation-log envelope seal/open was removed with the Phase 1
//! signed-history cutover. Invitation sealing still refuses a content key that
//! matches the owner's personal material.

use fold_db::crypto::E2eKeys;
use zeroize::Zeroizing;

/// Why a content key was refused for invitation sealing.
#[derive(Debug)]
pub(crate) enum LogEnvelopeError {
    Key,
    NotInvitable,
}

impl std::fmt::Display for LogEnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Key => "database content key is unavailable",
            Self::NotInvitable => "this content key cannot be put in an invitation",
        })
    }
}

impl std::error::Error for LogEnvelopeError {}

/// Refuse a content key that is the node seed, all zeros, or the personal AES key.
pub(crate) fn refuse_owner_material(
    node_seed: &[u8; 32],
    key: &[u8; 32],
) -> Result<(), LogEnvelopeError> {
    if key == node_seed || key == &[0u8; 32] {
        return Err(LogEnvelopeError::Key);
    }
    let personal = E2eKeys::from_ed25519_seed(node_seed).map_err(|_| LogEnvelopeError::Key)?;
    let personal_key = Zeroizing::new(personal.encryption_key());
    if key == &*personal_key {
        return Err(LogEnvelopeError::NotInvitable);
    }
    Ok(())
}
