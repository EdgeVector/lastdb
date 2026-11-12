//! Authenticated envelope for one database mutation-log payload.
//!
//! The plaintext is the canonical record bytes. The ciphertext uses the
//! database content key and binds the log metadata as associated data.
//! File bytes travel through the existing blob path. A separate encryption
//! domain binds them to the same log entry.

use std::ops::Deref;
use std::sync::Arc;

use fold_db::canonical::CanonicalWriter;
use fold_db::crypto::envelope::{
    decrypt_envelope_with_aad, encrypt_envelope_with_aad, ENVELOPE_VERSION,
};
use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use zeroize::Zeroizing;

use crate::cloud_backup_client::byte_backup::GuestReadPermission;
use crate::host::{owned::Resolved, Host};
use crate::share_invitation::{seal_invitation, OpenedShareInvitation, ShareContact};

/// Domain mixed into the associated data. It is not stored in the ciphertext.
pub const MUTATION_LOG_AAD_DOMAIN: &[u8] = b"lastdb:named-database:mutation-log:v1";
const MUTATION_LOG_BLOB_AAD_DOMAIN: &[u8] = b"lastdb:named-database:mutation-log-blob:v1";

/// Metadata both the producer and the receiver already know.
///
/// `delivery_seq` and `accepted_at_ms` are absent. The service assigns them
/// after the producer seals the payload. The writer key is the authenticated
/// node key. The append body does not carry it, and the receiver reads it
/// from the stored entry.
#[derive(Clone, Copy, Debug)]
pub struct LogBinding<'a> {
    pub database_id: &'a str,
    pub writer_key: &'a str,
    pub writer_device: &'a str,
    pub writer_seq: u64,
    pub authored_at_ms: u64,
    pub namespace: &'a str,
    pub incorporated_from: Option<IncorporatedRef<'a>>,
}

/// The incorporated writer, when the entry republishes a contribution.
#[derive(Clone, Copy, Debug)]
pub struct IncorporatedRef<'a> {
    pub writer_key: &'a str,
    pub writer_device: &'a str,
    pub writer_seq: u64,
}

/// Content key for one database. The bytes are the AES key, not a signing seed.
#[derive(Clone)]
pub struct DatabaseContentKey {
    bytes: Zeroizing<[u8; 32]>,
    database_id: String,
    invite: bool,
}

/// Why a log payload was not sealed or opened.
#[derive(Debug)]
pub enum LogEnvelopeError {
    InvalidBinding,
    Key,
    NotInvitable,
    PlaintextRefused,
    Seal,
    Open,
    Invitation,
}

impl std::fmt::Display for LogEnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidBinding => "mutation log binding is invalid",
            Self::Key => "database content key is unavailable",
            Self::NotInvitable => "this content key cannot be put in an invitation",
            Self::PlaintextRefused => "plaintext mutation log payload is refused",
            Self::Seal => "mutation log payload cannot be sealed",
            Self::Open => "mutation log payload cannot be opened",
            Self::Invitation => "database content key invitation cannot be sealed",
        })
    }
}

impl std::error::Error for LogEnvelopeError {}

impl std::fmt::Debug for DatabaseContentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseContentKey")
            .field("database_id", &self.database_id)
            .field("invite", &self.invite)
            .finish()
    }
}

impl DatabaseContentKey {
    /// Resolve an owned store and its existing content key by stable database id.
    pub(crate) fn local_store(
        host: &Host,
        database_id: &str,
    ) -> Result<Option<(Arc<FoldDB>, Self)>, LogEnvelopeError> {
        let selection = host
            .owned
            .registered_store(database_id)
            .map_err(|_| LogEnvelopeError::Key)?;
        let seed = Zeroizing::new(host.keypair.secret_key_bytes());
        match selection {
            (Resolved::Owner, None) => {
                let key = Self::personal(&seed, database_id)?;
                Ok(Some((Arc::clone(&host.db), key)))
            }
            (Resolved::Owned, Some(store)) => {
                let key = Self::owned(&seed, database_id)?;
                Ok(Some((store, key)))
            }
            (Resolved::Absent, None) => Ok(None),
            _ => Err(LogEnvelopeError::Key),
        }
    }

    /// Legacy root store key. Existing backups use this derivation.
    pub fn personal(node_seed: &[u8; 32], database_id: &str) -> Result<Self, LogEnvelopeError> {
        let database_id = canonical_id(database_id)?;
        let keys = E2eKeys::from_ed25519_seed(node_seed).map_err(|_| LogEnvelopeError::Key)?;
        let bytes = Zeroizing::new(keys.encryption_key());
        if material(&bytes) == node_seed || is_zero(material(&bytes)) {
            return Err(LogEnvelopeError::Key);
        }
        Ok(Self {
            bytes,
            database_id,
            invite: false,
        })
    }

    /// Owned sibling. Uses A's derivation and returns only the AES key.
    pub fn owned(node_seed: &[u8; 32], database_id: &str) -> Result<Self, LogEnvelopeError> {
        let database_id = canonical_id(database_id)?;
        let bytes = crate::host::owned::content_encryption_key(node_seed, &database_id)
            .map_err(|_| LogEnvelopeError::Key)?;
        refuse_owner_material(node_seed, material(&bytes))?;
        Ok(Self {
            bytes,
            database_id,
            invite: true,
        })
    }

    /// Receiver key from an opened invitation. It cannot be sealed again.
    pub fn from_invitation(opened: &OpenedShareInvitation) -> Result<Self, LogEnvelopeError> {
        if is_zero(opened.publication_key.deref()) {
            return Err(LogEnvelopeError::Key);
        }
        Ok(Self {
            bytes: Zeroizing::new(*opened.publication_key.deref()),
            database_id: opened.permission.stream().stream_id.as_str().to_owned(),
            invite: false,
        })
    }

    /// Database this key opens.
    #[must_use]
    pub fn database_id(&self) -> &str {
        &self.database_id
    }

    /// AES key for `accepted_writes`. Callers do not log these bytes.
    #[must_use]
    pub(crate) fn encryption_bytes(&self) -> &[u8; 32] {
        self.bytes.deref()
    }
}

/// Seal the canonical record bytes. Empty input is refused.
pub fn seal(
    key: &DatabaseContentKey,
    binding: &LogBinding<'_>,
    plaintext: &[u8],
) -> Result<Vec<u8>, LogEnvelopeError> {
    if plaintext.is_empty() {
        return Err(LogEnvelopeError::PlaintextRefused);
    }
    let (aes, aad) = bound(key, binding, MUTATION_LOG_AAD_DOMAIN)?;
    encrypt_envelope_with_aad(aes, &aad, plaintext).map_err(|_| LogEnvelopeError::Seal)
}

/// Open one envelope. Bytes that are not envelope version 1 are refused.
pub fn open(
    key: &DatabaseContentKey,
    binding: &LogBinding<'_>,
    ciphertext: &[u8],
) -> Result<Vec<u8>, LogEnvelopeError> {
    let plain = open_bound(key, binding, MUTATION_LOG_AAD_DOMAIN, ciphertext)?;
    if plain.is_empty() {
        return Err(LogEnvelopeError::Open);
    }
    Ok(plain)
}

/// Seal the bytes of the external file for this log entry.
pub(crate) fn seal_blob(
    key: &DatabaseContentKey,
    binding: &LogBinding<'_>,
    plaintext: &[u8],
) -> Result<Vec<u8>, LogEnvelopeError> {
    let (aes, aad) = bound(key, binding, MUTATION_LOG_BLOB_AAD_DOMAIN)?;
    encrypt_envelope_with_aad(aes, &aad, plaintext).map_err(|_| LogEnvelopeError::Seal)
}

/// Open the external file for this log entry. An empty file is valid.
pub(crate) fn open_blob(
    key: &DatabaseContentKey,
    binding: &LogBinding<'_>,
    ciphertext: &[u8],
) -> Result<Vec<u8>, LogEnvelopeError> {
    open_bound(key, binding, MUTATION_LOG_BLOB_AAD_DOMAIN, ciphertext)
}

fn open_bound(
    key: &DatabaseContentKey,
    binding: &LogBinding<'_>,
    domain: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, LogEnvelopeError> {
    if ciphertext.first() != Some(&ENVELOPE_VERSION) {
        return Err(LogEnvelopeError::PlaintextRefused);
    }
    let (aes, aad) = bound(key, binding, domain)?;
    decrypt_envelope_with_aad(aes, &aad, ciphertext).map_err(|_| LogEnvelopeError::Open)
}

/// Seal only the shareable database content key to one recipient.
///
/// The node seed and the personal content key are not arguments. A personal
/// key and a key taken from an invitation are refused.
pub fn seal_content_key_invitation(
    contact: &ShareContact,
    permission: &GuestReadPermission,
    key: &DatabaseContentKey,
    publisher: &Ed25519KeyPair,
) -> Result<Vec<u8>, LogEnvelopeError> {
    if !key.invite {
        return Err(LogEnvelopeError::NotInvitable);
    }
    if permission.stream().stream_id.as_str() != key.database_id {
        return Err(LogEnvelopeError::InvalidBinding);
    }
    seal_invitation(contact, permission, material(&key.bytes), publisher)
        .map_err(|_| LogEnvelopeError::Invitation)
}

fn bound<'a>(
    key: &'a DatabaseContentKey,
    binding: &LogBinding<'_>,
    domain: &[u8],
) -> Result<(&'a [u8; 32], Vec<u8>), LogEnvelopeError> {
    if binding.database_id != key.database_id {
        return Err(LogEnvelopeError::InvalidBinding);
    }
    let aad = associated_data(binding, domain)?;
    Ok((key.bytes.deref(), aad))
}

fn associated_data(binding: &LogBinding<'_>, domain: &[u8]) -> Result<Vec<u8>, LogEnvelopeError> {
    validate_binding(binding)?;
    let mut writer = CanonicalWriter::new()
        .field(domain)
        .u64(u64::from(ENVELOPE_VERSION))
        .field(binding.database_id.as_bytes())
        .field(binding.writer_key.as_bytes())
        .field(binding.writer_device.as_bytes())
        .u64(binding.writer_seq)
        .u64(binding.authored_at_ms)
        .field(binding.namespace.as_bytes());
    writer = match binding.incorporated_from {
        None => writer.u64(0),
        Some(from) => writer
            .u64(1)
            .field(from.writer_key.as_bytes())
            .field(from.writer_device.as_bytes())
            .u64(from.writer_seq),
    };
    Ok(writer.finish())
}

fn validate_binding(binding: &LogBinding<'_>) -> Result<(), LogEnvelopeError> {
    database_id(binding.database_id)?;
    node_key(binding.writer_key)?;
    require_label(binding.writer_device)?;
    require_label(binding.namespace)?;
    if binding.writer_seq == 0 {
        return Err(LogEnvelopeError::InvalidBinding);
    }
    let Some(from) = binding.incorporated_from else {
        return Ok(());
    };
    node_key(from.writer_key)?;
    require_label(from.writer_device)?;
    if from.writer_seq == 0 {
        return Err(LogEnvelopeError::InvalidBinding);
    }
    Ok(())
}

fn refuse_owner_material(node_seed: &[u8; 32], key: &[u8; 32]) -> Result<(), LogEnvelopeError> {
    if key == node_seed || is_zero(key) {
        return Err(LogEnvelopeError::Key);
    }
    let personal = E2eKeys::from_ed25519_seed(node_seed).map_err(|_| LogEnvelopeError::Key)?;
    let personal_key = Zeroizing::new(personal.encryption_key());
    if key == material(&personal_key) {
        return Err(LogEnvelopeError::NotInvitable);
    }
    Ok(())
}

fn canonical_id(value: &str) -> Result<String, LogEnvelopeError> {
    database_id(value)?;
    Ok(value.to_owned())
}

fn database_id(value: &str) -> Result<(), LogEnvelopeError> {
    cloud_backup_protocol::Id::try_from(value.to_owned())
        .map(|_| ())
        .map_err(|_| LogEnvelopeError::InvalidBinding)
}

fn node_key(value: &str) -> Result<(), LogEnvelopeError> {
    cloud_backup_protocol::Digest::try_from(value.to_owned())
        .map(|_| ())
        .map_err(|_| LogEnvelopeError::InvalidBinding)
}

fn require_label(value: &str) -> Result<(), LogEnvelopeError> {
    let legal = (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if legal {
        Ok(())
    } else {
        Err(LogEnvelopeError::InvalidBinding)
    }
}

fn material(key: &Zeroizing<[u8; 32]>) -> &[u8; 32] {
    key.deref()
}

fn is_zero(key: &[u8; 32]) -> bool {
    key == &[0u8; 32]
}
