//! The one file of a record (design `design-lastdb-no-shared-file-blobs`).
//!
//! The file lives in `cas_blobs`, the CAS run home that the backup sends to
//! B2, at a path made from the record key:
//! `fb:{S}:{sha256(record storage key)}`. One record has one path, so
//! two records never share bytes and nothing counts references. The marker of
//! the record (`StoredRecord::file`) says whether the path holds a file.
//!
//! Every write that changes the marker changes the path in the same native
//! request: `put-blob` puts the sealed row, and `delete-blob`, a record
//! delete and a schema release delete it.
//!
//! The row is the envelope `0x01 ‖ nonce ‖ AES-256-GCM ciphertext and tag`.
//! The key comes from the account key and the marker's plaintext SHA-256
//! (account-v2 derivation) and the nonce from the key, so no key is stored: a
//! reader derives it again from the marker. A new file at the same path has a
//! new hash and so a new key and nonce.

use super::RecordStore;
use crate::error::FoldDbError;
use crate::schema::SchemaError;
use crate::storage::{KvMutation, KvStore};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use lastdb_model::hex::hex_lower;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// The namespace of the file rows: its own run home, sent to B2.
pub(crate) const CAS_BLOB_TREE: &str = crate::storage::disk_pack::CAS_COLLECTION;

const FILE_KEY_SALT: &[u8] = b"lastdb-file-blob-account-v2";
const FILE_NONCE_SALT: &[u8] = b"lastdb-file-blob-account-v2:nonce";
const ENVELOPE_VERSION: u8 = 0x01;

/// The file path of the record at one storage key.
pub(crate) fn file_path(record: &str) -> Result<String, SchemaError> {
    Ok(crate::storage::current_keys::families::record_file_path(
        record,
    )?)
}

/// The file paths that a drop of these stored rows releases: one for each
/// record row with a file. Other rows hold no file. A key that does not parse
/// or a record row that does not decode fails the drop.
pub(crate) fn dropped_file_paths<'a>(
    rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<Vec<Vec<u8>>, SchemaError> {
    let mut paths = Vec::new();
    for (key, value) in rows {
        let key = std::str::from_utf8(key).map_err(|error| {
            SchemaError::InvalidData(format!("a dropped row key is not UTF-8: {error}"))
        })?;
        let Some(path) = record_row_path(key)? else {
            continue;
        };
        let record = StoredRecord::decode(value).map_err(|error| {
            SchemaError::InvalidData(format!("dropped record row {key} is unreadable: {error}"))
        })?;
        if record.file.is_some() {
            paths.push(path.into_bytes());
        }
    }
    Ok(paths)
}

/// The file path of a record row key, or `None` for a row of another family.
fn record_row_path(key: &str) -> Result<Option<String>, SchemaError> {
    if !key.starts_with(RECORD_PREFIX) {
        return Ok(None);
    }
    file_path(key).map(Some)
}

fn file_crypto_error(detail: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::SecurityError(format!("record file encryption: {detail}"))
}

/// The file key of one plaintext hash, and its nonce.
fn file_key(account_key: &[u8; 32], sha256: &str) -> Result<([u8; 32], [u8; 12]), FoldDbError> {
    let hash = lastdb_model::hex::hex_decode_array::<32>(sha256)
        .ok_or_else(|| file_crypto_error("the file hash is not 64 hex digits"))?;
    let mut key = [0u8; 32];
    Hkdf::<Sha256>::new(Some(FILE_KEY_SALT), account_key)
        .expand(
            &[b"file-dek\0".as_slice(), hash.as_slice()].concat(),
            &mut key,
        )
        .map_err(file_crypto_error)?;
    let mut nonce = [0u8; 12];
    Hkdf::<Sha256>::new(Some(FILE_NONCE_SALT), &key)
        .expand(b"aes-256-gcm-nonce", &mut nonce)
        .map_err(file_crypto_error)?;
    Ok((key, nonce))
}

impl RecordStore {
    fn account_key(&self) -> Result<&[u8; 32], FoldDbError> {
        self.content_key.as_ref().ok_or_else(|| {
            FoldDbError::SecurityError("local file encryption requires the account key".into())
        })
    }

    async fn file_store(&self) -> Result<Arc<dyn KvStore>, SchemaError> {
        self.namespaced_store
            .open_namespace(CAS_BLOB_TREE)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("open {CAS_BLOB_TREE}: {error}")))
    }

    /// Stage file row puts and deletes. Inside a native write scope they join
    /// the request's one batch with the records that name them.
    pub(crate) async fn stage_file_rows(&self, rows: Vec<KvMutation>) -> Result<(), SchemaError> {
        if rows.is_empty() {
            return Ok(());
        }
        self.file_store().await?.batch_mutate(rows).await?;
        Ok(())
    }

    /// Seal a new file under the account key. Returns its marker and the row
    /// that a record write puts at the record's file path.
    pub(crate) fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<(FileMarker, Vec<u8>), FoldDbError> {
        let sha256 = hex_lower(Sha256::digest(plaintext));
        let (key, nonce) = file_key(self.account_key()?, &sha256)?;
        let ciphertext = Aes256Gcm::new_from_slice(&key)
            .map_err(file_crypto_error)?
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(file_crypto_error)?;
        let mut row = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
        row.push(ENVELOPE_VERSION);
        row.extend_from_slice(&nonce);
        row.extend_from_slice(&ciphertext);
        let marker = FileMarker {
            name,
            media_type,
            size_bytes: plaintext.len() as u64,
            sha256,
        };
        Ok((marker, row))
    }

    /// The plaintext of the file of the record at `record_key`, checked
    /// against its marker. `None` when the path holds no row.
    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = file_path(record_key)?;
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
