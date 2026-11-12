//! Record file encryption, verification, and on-demand blob installation.

use super::super::{
    FILE_DEK_LABEL, FILE_ENVELOPE_VERSION, FILE_KEY_SALT, FILE_NONCE_LABEL, FILE_NONCE_SALT,
    FILE_NONCE_SIZE, FILE_TAG_SIZE,
};
use super::RecordFormat;
use crate::error::FoldDbError;
use crate::schema::types::FileWrite;
use crate::schema::SchemaError;
use crate::storage::current_keys::families::{
    record_blob_path, record_file_path, record_file_uploaded_path,
};
use crate::storage::disk_pack::transaction;
use crate::storage::KvMutation;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use cloud_backup_protocol::v2::blobs::BlobPath;
use hkdf::Hkdf;
use lastdb_model::hex::hex_lower;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use sha2::{Digest, Sha256};

/// The file paths that a drop of these stored rows releases: one for each
/// record row with a file.
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

fn differs(detail: &str) -> FoldDbError {
    FoldDbError::SecurityError(format!("downloaded record file {detail}"))
}

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
    pub(crate) fn record_blob_path(
        &self,
        record_key: &str,
        version: &Position,
    ) -> Result<BlobPath, FoldDbError> {
        let database =
            cloud_backup_protocol::Id::try_from(self.runtime.native_home_id().to_string())
                .map_err(|_| FoldDbError::Database("the home identity is not a cloud id".into()))?;
        Ok(record_blob_path(&database, record_key, version)?)
    }

    pub(crate) fn sealed_file_bytes(marker: &FileMarker) -> Result<u64, FoldDbError> {
        marker
            .size_bytes
            .checked_add((1 + FILE_NONCE_SIZE + FILE_TAG_SIZE) as u64)
            .ok_or_else(|| differs("size overflows"))
    }

    pub(crate) async fn install_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
        row: Vec<u8>,
    ) -> Result<Vec<u8>, FoldDbError> {
        if Self::sealed_file_bytes(marker)? != row.len() as u64 {
            return Err(differs("has a different length than its marker"));
        }
        require_row(&self.content_key, marker, &row)
            .map_err(|_| differs("differs from its format or marker"))?;
        let plaintext = self.open_file_row(&row, marker)?;
        let path = record_file_path(record_key, version)?.into_bytes();
        let uploaded = record_file_uploaded_path(&path)?;
        transaction::request_scope(self.runtime(), async {
            let current = self
                .main_store
                .get(record_key.as_bytes())
                .await?
                .map(|bytes| StoredRecord::decode(&bytes))
                .transpose()
                .map_err(super::record_value_error)?;
            if !current.as_ref().is_some_and(|record| {
                record.file_version.as_ref() == Some(version)
                    && record.file.as_ref() == Some(marker)
            }) {
                return Ok::<_, FoldDbError>(());
            }
            let store = self.file_store().await?;
            if store.get(&path).await?.is_none() {
                store
                    .batch_mutate(vec![
                        KvMutation::put(path, row),
                        KvMutation::put(uploaded, vec![1]),
                    ])
                    .await?;
            }
            Ok(())
        })
        .await?;
        Ok(plaintext)
    }

    /// Seal a new file under the account key and bind the immutable output
    /// to this format instance. The issuer never leaves this writer.
    pub(crate) fn seal_record_file(
        &self,
        plaintext: &[u8],
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let (row, sha256) = seal_file_row(&self.content_key, plaintext)?;
        Ok(self.file_issuer.issue(
            FileMarker {
                name,
                media_type,
                size_bytes: plaintext.len() as u64,
                sha256,
            },
            row,
        ))
    }

    pub(crate) async fn reference_local_file(
        &self,
        path: &std::path::Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let runtime = std::sync::Arc::clone(&self.runtime);
        let path = path.to_path_buf();
        let described = crate::storage::disk_pack::run_blocking(move || {
            crate::storage::local_file_ref::describe(&runtime, &path)
        })
        .await?;
        let marker = FileMarker {
            name: name.or(described.file_name),
            media_type,
            size_bytes: described.size_bytes,
            sha256: described.sha256,
        };
        if !marker.has_valid_hash() {
            return Err(FoldDbError::Schema(SchemaError::InvalidData(
                "local file hash is not 64 hex digits".into(),
            )));
        }
        Ok(self.file_issuer.issue_local(marker, described.path))
    }

    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = record_file_path(record_key, version)?;
        match self.file_store().await?.get(path.as_bytes()).await {
            Ok(Some(row)) => self.open_file_row(&row, marker).map(Some),
            Ok(None) => {
                crate::storage::local_file_ref::read_referenced(&self.runtime, &path, marker).await
            }
            Err(error) => {
                match crate::storage::local_file_ref::read_referenced(&self.runtime, &path, marker)
                    .await?
                {
                    Some(bytes) => Ok(Some(bytes)),
                    None => Err(error.into()),
                }
            }
        }
    }

    fn open_file_row(&self, row: &[u8], marker: &FileMarker) -> Result<Vec<u8>, FoldDbError> {
        decrypt_file_row(&self.content_key, marker, row)
    }
}

/// Open one sealed file row with the native file key. The row is the accepted
/// entry or the file-store object for that marker, not a later record version.
pub(crate) fn decrypt_file_row(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<Vec<u8>, FoldDbError> {
    require_row(content_key, marker, row)
        .map_err(|_| FoldDbError::SecurityError("record file differs from its marker".into()))?;
    let (key, _) = file_key(content_key, &marker.sha256)?;
    let plaintext =
        crate::crypto::envelope::decrypt_envelope(&key, row).map_err(file_crypto_error)?;
    if plaintext.len() as u64 != marker.size_bytes
        || hex_lower(Sha256::digest(&plaintext)) != marker.sha256
    {
        return Err(FoldDbError::SecurityError(
            "record file differs from its marker".into(),
        ));
    }
    Ok(plaintext)
}

pub(crate) fn seal_file_row(
    account_key: &[u8; 32],
    plaintext: &[u8],
) -> Result<(Vec<u8>, String), FoldDbError> {
    if plaintext.len() as u64
        > lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    {
        return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
            detail: "local file input exceeds native pending memory allowance".into(),
        }));
    }
    let sha256 = hex_lower(Sha256::digest(plaintext));
    let (key, nonce) = file_key(account_key, &sha256)?;
    let ciphertext = Aes256Gcm::new_from_slice(&key)
        .map_err(file_crypto_error)?
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(file_crypto_error)?;
    let mut row = Vec::with_capacity(1 + nonce.len() + ciphertext.len());
    row.push(FILE_ENVELOPE_VERSION);
    row.extend_from_slice(&nonce);
    row.extend_from_slice(&ciphertext);
    Ok((row, sha256))
}

pub(crate) fn require_row(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<(), SchemaError> {
    let invalid =
        || SchemaError::InvalidData("record file row differs from its format or marker".into());
    let overhead = (1 + FILE_NONCE_SIZE + FILE_TAG_SIZE) as u64;
    if !marker.has_valid_hash()
        || marker.size_bytes
            > lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
        || marker.size_bytes.checked_add(overhead) != Some(row.len() as u64)
        || row.first() != Some(&FILE_ENVELOPE_VERSION)
    {
        return Err(invalid());
    }
    let (_, nonce) = file_key(content_key, &marker.sha256).map_err(|_| invalid())?;
    if row.get(1..1 + FILE_NONCE_SIZE) != Some(nonce.as_slice()) {
        return Err(invalid());
    }
    Ok(())
}
