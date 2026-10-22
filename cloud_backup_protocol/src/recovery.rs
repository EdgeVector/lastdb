//! Public recovery metadata. Secrets remain outside this protocol descriptor.

use crate::manifest::{NATIVE_REQUIRED_FEATURES_V1, NATIVE_SUPPORTED_FEATURES_V1};
use crate::{AccountId, AccountKind, CloudScope, DecimalU64, Digest, Id, ProtocolError, Version};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;

/// Exact existing Mini derivation: 24-word BIP39 -> 32 entropy bytes -> Ed25519
/// seed -> E2eKeys::from_ed25519_seed. This is not BIP39's PBKDF2 seed function.
/// Embedded store-keyring modes have no version-one wire variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryKeyScheme {
    MiniBip39Entropy32Ed25519V1,
}

/// Existing LocalCryptoProvider format: version 1, AES-256-GCM, 12-byte nonce,
/// 16-byte tag, no key ID and no AAD. Context stays in encrypted plaintext.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalEnvelopeProfile {
    AccountContentV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReaderCapability {
    NativePackV1,
    LogicalKeyLayout4,
    AccountContentEnvelopeV1,
    ConvergentFileBlobV1,
    LegacyDekFileBlobV1,
    RetainedExternalFilesV1,
    BackupFileExtentsV1,
}

/// Disaster recovery preserves account and native identities. A fresh device
/// credential or writer takeover requires separate authority, never this value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreIdentity {
    PreserveAccountAndNativeHome,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDescriptor {
    pub version: Version,
    pub scope: CloudScope,
    pub source_home: Id,
    /// Raw 32-byte Ed25519 public key encoded as lowercase hex, not a secret.
    pub account_public_key: Digest,
    pub expected_account_id: AccountId,
    pub key_scheme: RecoveryKeyScheme,
    pub local_envelope: LocalEnvelopeProfile,
    pub required_native_features: DecimalU64,
    pub reader_capabilities: Vec<ReaderCapability>,
    pub restore_identity: RestoreIdentity,
}

impl RecoveryDescriptor {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        self.scope.validate()?;
        let features = self.required_native_features.get();
        if self.scope.account.kind != AccountKind::Personal
            || self.expected_account_id != self.scope.account.id
            || self.expected_account_id.as_str().len() != 32
            || features & !NATIVE_SUPPORTED_FEATURES_V1 != 0
            || features & NATIVE_REQUIRED_FEATURES_V1 != NATIVE_REQUIRED_FEATURES_V1
            || self.reader_capabilities.is_empty()
            || self.reader_capabilities.len() > 16
        {
            return Err(ProtocolError::invalid("recovery_descriptor"));
        }
        let unique: BTreeSet<_> = self.reader_capabilities.iter().copied().collect();
        if unique.len() != self.reader_capabilities.len()
            || !unique.contains(&ReaderCapability::NativePackV1)
            || !unique.contains(&ReaderCapability::LogicalKeyLayout4)
            || !unique.contains(&ReaderCapability::AccountContentEnvelopeV1)
        {
            return Err(ProtocolError::invalid("recovery_descriptor.capabilities"));
        }
        let public_key = decode_public_key(&self.account_public_key)?;
        let hash = Sha256::digest(public_key);
        let account: String = hash[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if account != self.expected_account_id.as_str() {
            return Err(ProtocolError::invalid(
                "recovery_descriptor.account_public_key",
            ));
        }
        Ok(())
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
