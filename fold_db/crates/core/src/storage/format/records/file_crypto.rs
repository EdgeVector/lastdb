//! The one file of a record (design `design-lastdb-no-shared-file-blobs`).
//!
//! The file lives in `cas_blobs`, the CAS run home that the backup sends to
//! B2, at a path made from the record key and originating mutation position.
//! Each record selects its live file version. Other records own their own
//! versions, even when the contents match.
//!
//! Every write that changes the marker changes the path in the same native
//! request. Native commit publishes new bytes before the record selects them
//! and removes the previous version after the record publication is durable.
//!
//! The row is the envelope `0x01 ‖ nonce ‖ AES-256-GCM ciphertext and tag`.
//! The key comes from the account key and the marker's plaintext SHA-256
//! (account-v2 derivation) and the nonce from the key, so no key is stored: a
//! reader derives it again from the marker. A new file at the same path has a
//! new hash and so a new key and nonce.

use super::RecordFormat;
use crate::error::FoldDbError;
use crate::schema::types::FileWrite;
use crate::schema::SchemaError;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use lastdb_model::hex::hex_lower;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use sha2::{Digest, Sha256};

use super::super::{
    FILE_DEK_LABEL, FILE_ENVELOPE_VERSION, FILE_KEY_SALT, FILE_NONCE_LABEL, FILE_NONCE_SALT,
    FILE_NONCE_SIZE, FILE_TAG_SIZE,
};

use crate::storage::current_keys::families::record_file_path;

/// The file paths that a drop of these stored rows releases: one for each
/// record row with a file. Other rows hold no file. A key that does not parse
/// or a record row that does not decode fails the drop.
pub(super) fn dropped_file_paths<'a>(
    rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<Vec<Vec<u8>>, SchemaError> {
    let mut paths = Vec::new();
    for (key, value) in rows {
        let key = std::str::from_utf8(key).map_err(|error| {
            SchemaError::InvalidData(format!("a dropped row key is not UTF-8: {error}"))
        })?;
        if !key.starts_with(RECORD_PREFIX) {
            continue;
        }
        let record = StoredRecord::decode(value).map_err(|error| {
            SchemaError::InvalidData(format!("dropped record row {key} is unreadable: {error}"))
        })?;
        if let Some(version) = record.file_version {
            paths.push(record_file_path(key, &version)?.into_bytes());
        }
    }
    Ok(paths)
}

fn file_crypto_error(detail: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::SecurityError(format!("record file encryption: {detail}"))
}

/// The file key of one plaintext hash, and its nonce.
fn file_key(
    account_key: &[u8; 32],
    sha256: &str,
) -> Result<([u8; 32], [u8; FILE_NONCE_SIZE]), FoldDbError> {
    let hash = lastdb_model::hex::hex_decode_array::<32>(sha256)
        .ok_or_else(|| file_crypto_error("the file hash is not 64 hex digits"))?;
    let mut key = [0u8; 32];
    Hkdf::<Sha256>::new(Some(FILE_KEY_SALT), account_key)
        .expand(&[FILE_DEK_LABEL, hash.as_slice()].concat(), &mut key)
        .map_err(file_crypto_error)?;
    let mut nonce = [0u8; FILE_NONCE_SIZE];
    Hkdf::<Sha256>::new(Some(FILE_NONCE_SALT), &key)
        .expand(FILE_NONCE_LABEL, &mut nonce)
        .map_err(file_crypto_error)?;
    Ok((key, nonce))
}

impl RecordFormat {
    fn account_key(&self) -> Result<&[u8; 32], FoldDbError> {
        self.content_key.as_ref().ok_or_else(|| {
            FoldDbError::SecurityError("local file encryption requires the account key".into())
        })
    }

    /// Seal a new file under the account key and bind the immutable output
    /// to this format instance. The issuer never leaves this writer.
    pub(crate) fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        if plaintext.len() as u64
            > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
        {
            return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
                detail: "local file input exceeds native pending memory allowance".into(),
            }));
        }
        let sha256 = hex_lower(Sha256::digest(plaintext));
        let (key, nonce) = file_key(self.account_key()?, &sha256)?;
        let ciphertext = Aes256Gcm::new_from_slice(&key)
            .map_err(file_crypto_error)?
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(file_crypto_error)?;
        let mut row = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
        row.push(FILE_ENVELOPE_VERSION);
        row.extend_from_slice(&nonce);
        row.extend_from_slice(&ciphertext);
        let marker = FileMarker {
            name,
            media_type,
            size_bytes: plaintext.len() as u64,
            sha256,
        };
        Ok(self.file_issuer.issue(marker, row))
    }

    /// The plaintext of the file of the record at `record_key`, checked
    /// against its marker. `None` when the path holds no row.
    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = record_file_path(record_key, version)?;
        let Some(row) = self.file_store().await?.get(path.as_bytes()).await? else {
            return Ok(None);
        };
        let (key, _) = file_key(self.account_key()?, &marker.sha256)?;
        let plaintext =
            crate::crypto::envelope::decrypt_envelope(&key, &row).map_err(file_crypto_error)?;
        if plaintext.len() as u64 != marker.size_bytes
            || hex_lower(Sha256::digest(&plaintext)) != marker.sha256
        {
            return Err(FoldDbError::SecurityError(
                "record file differs from its marker".into(),
            ));
        }
        Ok(Some(plaintext))
    }
}

/// Validate the stored envelope at the only file-row write boundary. This
/// checks framing and the configured deterministic nonce, not the GCM tag;
/// authenticated opening and plaintext-hash verification remain on reads.
pub(super) fn require_row(
    content_key: Option<&[u8; 32]>,
    marker: &FileMarker,
    row: &[u8],
) -> Result<(), SchemaError> {
    let invalid =
        || SchemaError::InvalidData("record file row differs from its format or marker".into());
    let overhead = (1 + FILE_NONCE_SIZE + FILE_TAG_SIZE) as u64;
    if marker
        .sha256
        .bytes()
        .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        || marker.size_bytes > lastdb_storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
        || marker.size_bytes.checked_add(overhead) != Some(row.len() as u64)
        || row.first() != Some(&FILE_ENVELOPE_VERSION)
    {
        return Err(invalid());
    }
    let account_key = content_key.ok_or_else(invalid)?;
    let (_, nonce) = file_key(account_key, &marker.sha256).map_err(|_| invalid())?;
    if row.get(1..1 + FILE_NONCE_SIZE) != Some(nonce.as_slice()) {
        return Err(invalid());
    }
    Ok(())
}
