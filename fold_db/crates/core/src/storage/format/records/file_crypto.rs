//! Record file encryption, inline/applied-delete families, and blob installation.

use super::super::{
    FILE_DEK_LABEL, FILE_ENVELOPE_VERSION, FILE_KEY_SALT, FILE_NONCE_LABEL, FILE_NONCE_SALT,
    FILE_NONCE_SIZE, FILE_TAG_SIZE,
};
use super::{RecordAddress, RecordFormat};
use crate::crypto::envelope::decrypt_envelope;
use crate::error::FoldDbError;
use crate::schema::types::FileWrite;
use crate::schema::SchemaError;
use crate::storage::current_keys::families::{
    record_blob_path, record_file_path, record_file_uploaded_path,
};
use crate::storage::disk_pack::{run_blocking, transaction};
use crate::storage::{local_file_ref, KvMutation};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use cloud_backup_protocol::{v2::blobs::BlobPath, Id};
use hkdf::Hkdf;
use lastdb_model::hex::{hex_decode_array, hex_lower};
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;

pub(in crate::storage) mod applied_deletes {
    use super::{RecordAddress, RecordFormat};
    use crate::storage::{error::StorageResult, KvMutation, StorageError};
    use lastdb_model::portable_mutation::{AppliedDelete, Stamp};

    pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";

    pub(in crate::storage::format::records) fn encode(
        key: &RecordAddress,
        stamp: &Stamp,
    ) -> StorageResult<KvMutation> {
        let value = serde_json::to_vec(&AppliedDelete::from(stamp.clone())).map_err(invalid)?;
        Ok(KvMutation::put(format!("ad:{}", key.as_str()), value))
    }

    pub(crate) fn record_key(key: &str) -> Option<&str> {
        key.strip_prefix("ad:")
    }

    impl RecordFormat {
        pub(crate) async fn applied_deletes(
            &self,
            keys: &[&RecordAddress],
        ) -> StorageResult<Vec<Option<Stamp>>> {
            let req = keys
                .iter()
                .map(|k| format!("ad:{}", k.as_str()).into_bytes())
                .collect();
            let rows = self.main_store.get_many(req).await?;
            if rows.len() != keys.len() {
                return Err(invalid("incomplete applied-delete read"));
            }
            rows.iter()
                .map(|r| {
                    let Some(b) = r.as_deref() else {
                        return Ok(None);
                    };
                    if b.len() > 512 {
                        return Err(invalid("encoded value exceeds its bound"));
                    }
                    let e: AppliedDelete = serde_json::from_slice(b).map_err(invalid)?;
                    e.delete_position.validate().map_err(invalid)?;
                    Ok(Some(e.into()))
                })
                .collect()
        }
    }

    fn invalid(error: impl std::fmt::Display) -> StorageError {
        StorageError::InvalidOperation(format!("applied delete: {error}"))
    }
}

pub(in crate::storage) mod inline_files {
    use super::RecordAddress;
    use crate::storage::{error::StorageResult, KvMutation, StorageError};
    use lastdb_model::record_value::{FileMarker, InlineFile};

    pub(in crate::storage) const INLINE_FILE_FAMILY: &str = "fi";

    pub(in crate::storage::format::records) fn put(
        key: &RecordAddress,
        file: &InlineFile,
    ) -> StorageResult<KvMutation> {
        Ok(KvMutation::put(
            address(key.as_str()),
            file.encode().map_err(StorageError::InvalidOperation)?,
        ))
    }

    pub(in crate::storage::format::records) fn delete(key: &RecordAddress) -> KvMutation {
        KvMutation::delete(address(key.as_str()))
    }

    pub(crate) fn record_key(key: &str) -> Option<&str> {
        key.strip_prefix("fi:")
    }

    pub(in crate::storage) fn address(record: &str) -> String {
        format!("fi:{record}")
    }

    pub(in crate::storage) fn decode_for(
        bytes: &[u8],
        marker: &FileMarker,
    ) -> StorageResult<InlineFile> {
        let file = InlineFile::decode(bytes).map_err(StorageError::InvalidOperation)?;
        (&file.marker == marker).then_some(file).ok_or_else(|| {
            StorageError::InvalidOperation(
                "the inline file row differs from its record marker".into(),
            )
        })
    }
}

fn crypto_err(detail: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::SecurityError(format!("record file encryption: {detail}"))
}

fn differs(detail: &str) -> FoldDbError {
    FoldDbError::SecurityError(format!("downloaded record file {detail}"))
}

fn file_key(
    account_key: &[u8; 32],
    sha256: &str,
) -> Result<([u8; 32], [u8; FILE_NONCE_SIZE]), FoldDbError> {
    let hash = hex_decode_array::<32>(sha256)
        .ok_or_else(|| crypto_err("the file hash is not 64 hex digits"))?;
    let mut key = [0u8; 32];
    Hkdf::<Sha256>::new(Some(FILE_KEY_SALT), account_key)
        .expand(&[FILE_DEK_LABEL, hash.as_slice()].concat(), &mut key)
        .map_err(crypto_err)?;
    let mut nonce = [0u8; FILE_NONCE_SIZE];
    Hkdf::<Sha256>::new(Some(FILE_NONCE_SALT), &key)
        .expand(FILE_NONCE_LABEL, &mut nonce)
        .map_err(crypto_err)?;
    Ok((key, nonce))
}

impl RecordFormat {
    pub(crate) async fn open_inline_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let Some(row) = self
            .main_store
            .get(inline_files::address(record_key).as_bytes())
            .await?
        else {
            return Ok(None);
        };
        inline_files::decode_for(&row, marker)?
            .bytes()
            .map(Some)
            .map_err(FoldDbError::Database)
    }

    pub(crate) fn record_blob_path(
        &self,
        record_key: &str,
        version: &Position,
    ) -> Result<BlobPath, FoldDbError> {
        let db = Id::try_from(self.runtime.native_home_id().to_string())
            .map_err(|_| FoldDbError::Database("the home identity is not a cloud id".into()))?;
        Ok(record_blob_path(&db, record_key, version)?)
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
        let plaintext = decrypt_file_row(&self.content_key, marker, &row)?;
        let path = record_file_path(record_key, version)?.into_bytes();
        let uploaded = record_file_uploaded_path(&path)?;
        transaction::request_scope(self.runtime(), async {
            let current = self
                .main_store
                .get(record_key.as_bytes())
                .await?
                .map(|b| StoredRecord::decode(&b))
                .transpose()
                .map_err(super::record_value_error)?;
            if current.as_ref().is_some_and(|r| {
                r.file_version.as_ref() == Some(version) && r.file.as_ref() == Some(marker)
            }) {
                let store = self.runtime.open_cas().await?;
                if store.get(&path).await?.is_none() {
                    store
                        .batch_mutate(vec![
                            KvMutation::put(path, row),
                            KvMutation::put(uploaded, vec![1]),
                        ])
                        .await?;
                }
            }
            Ok::<_, FoldDbError>(())
        })
        .await?;
        Ok(plaintext)
    }

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
        path: &Path,
        name: Option<String>,
        media_type: Option<String>,
    ) -> Result<FileWrite, FoldDbError> {
        let (rt, path) = (Arc::clone(&self.runtime), path.to_path_buf());
        let d = run_blocking(move || local_file_ref::describe(&rt, &path)).await?;
        let marker = FileMarker {
            name: name.or(d.file_name),
            media_type,
            size_bytes: d.size_bytes,
            sha256: d.sha256,
        };
        marker
            .has_valid_hash()
            .then(|| self.file_issuer.issue_local(marker, d.path))
            .ok_or_else(|| {
                FoldDbError::Schema(SchemaError::InvalidData(
                    "local file hash is not 64 hex digits".into(),
                ))
            })
    }

    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        version: &Position,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = record_file_path(record_key, version)?;
        match self.runtime.open_cas().await?.get(path.as_bytes()).await {
            Ok(Some(row)) => decrypt_file_row(&self.content_key, marker, &row).map(Some),
            res => match local_file_ref::read_referenced(&self.runtime, &path, marker).await? {
                Some(bytes) => Ok(Some(bytes)),
                None => res.map(|_| None).map_err(Into::into),
            },
        }
    }
}

pub(crate) fn decrypt_file_row(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<Vec<u8>, FoldDbError> {
    let mismatch = || FoldDbError::SecurityError("record file differs from its marker".into());
    require_row(content_key, marker, row).map_err(|_| mismatch())?;
    let (key, _) = file_key(content_key, &marker.sha256)?;
    let plain = decrypt_envelope(&key, row).map_err(crypto_err)?;
    (plain.len() as u64 == marker.size_bytes && hex_lower(Sha256::digest(&plain)) == marker.sha256)
        .then_some(plain)
        .ok_or_else(mismatch)
}

pub(crate) fn seal_file_row(
    account_key: &[u8; 32],
    plaintext: &[u8],
) -> Result<(Vec<u8>, String), FoldDbError> {
    if plaintext.len() as u64 > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
            detail: "local file input exceeds native pending memory allowance".into(),
        }));
    }
    let sha256 = hex_lower(Sha256::digest(plaintext));
    let (key, nonce) = file_key(account_key, &sha256)?;
    let ciphertext = Aes256Gcm::new_from_slice(&key)
        .map_err(crypto_err)?
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(crypto_err)?;
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
        || marker.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES
        || marker.size_bytes.checked_add(overhead) != Some(row.len() as u64)
        || row.first() != Some(&FILE_ENVELOPE_VERSION)
    {
        return Err(invalid());
    }
    let (_, nonce) = file_key(content_key, &marker.sha256).map_err(|_| invalid())?;
    (row.get(1..1 + FILE_NONCE_SIZE) == Some(nonce.as_slice()))
        .then_some(())
        .ok_or_else(invalid)
}
