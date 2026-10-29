//! Public Mini recovery profile. No seed, per-file key or credential is present.

use crate::{CloudScope, Digest, ProtocolError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Mini's existing keys and formats, not a new cryptographic construction.
/// BIP39 supplies 32 raw entropy bytes for the Ed25519 seed. The existing
/// E2eKeys derivation supplies the content and hash keys. Manifests use existing
/// envelope v1; embedded store keyrings are outside this profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryProfile {
    /// Mini's existing Ed25519 seed and E2E key derivation. This is not a new construction.
    MiniPortableV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Public recovery description. It holds no seed or per-file key.
pub struct RecoveryDescriptor {
    /// Which recovery construction this descriptor uses.
    pub profile: RecoveryProfile,
    /// Raw 32-byte Ed25519 public key, lowercase hex; this value is not a secret.
    pub account_public_key: Digest,
}

impl RecoveryDescriptor {
    /// Check that the first 16 bytes of SHA-256 over the raw public key equal the scope account id.
    pub fn validate_for(&self, scope: &CloudScope) -> Result<(), ProtocolError> {
        scope.validate()?;
        let public_key = decode_public_key(&self.account_public_key)?;
        let hash = Sha256::digest(public_key);
        let account: String = hash[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if account != scope.account_id.as_str() {
            return Err(ProtocolError::invalid(
                "recovery_descriptor.account_public_key",
            ));
        }
        Ok(())
    }

    /// Domain-separated SHA-256 of this descriptor.
    pub fn commitment(&self) -> Result<Digest, ProtocolError> {
        crate::input_digest("lastdb:native-backup:recovery:v1", self)
    }
}

fn decode_public_key(encoded: &Digest) -> Result<[u8; 32], ProtocolError> {
    let mut bytes = [0u8; 32];
    for (out, pair) in bytes
        .iter_mut()
        .zip(encoded.as_str().as_bytes().chunks_exact(2))
    {
        let pair = std::str::from_utf8(pair)
            .map_err(|_| ProtocolError::invalid("recovery_descriptor.account_public_key"))?;
        *out = u8::from_str_radix(pair, 16)
            .map_err(|_| ProtocolError::invalid("recovery_descriptor.account_public_key"))?;
    }
    Ok(bytes)
}
