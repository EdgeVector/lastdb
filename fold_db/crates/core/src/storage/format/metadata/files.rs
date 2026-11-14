//! Record file row encryption and file reference tracking without redundant preimage reads.

use super::{schema_inv, Metadata};
use crate::crypto::envelope::{decrypt_envelope, ENVELOPE_VERSION, NONCE_SIZE, TAG_SIZE};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, FileWriteIssuer, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::format::RecordAddress;
use crate::storage::local_file_ref::{SidecarAddition, SidecarPlan};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use lastdb_model::hex::{hex_decode_array, hex_lower};
use lastdb_model::record_value::{FileMarker, StoredRecord};
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

const FILE_REFERENCE_PREFIX: &str = "portable-file-reference:";
pub(in crate::storage) const FILE_RETENTION_KEY: &str = "portable-file-retention";
pub(in crate::storage) const FILE_RETENTION_COMPLETE: &[u8] = b"\"complete\"";

pub(in crate::storage::format) struct FileChange {
    pub(in crate::storage::format) address: RecordAddress,
    pub(in crate::storage::format) before: Option<Arc<StoredRecord>>,
    pub(in crate::storage::format) after: Arc<StoredRecord>,
    pub(in crate::storage::format) write: Option<FileWrite>,
}

#[derive(Default)]
struct StagedFiles {
    cas_puts: Vec<KvMutation>,
    references: BTreeMap<String, bool>,
    sidecar: SidecarPlan,
}

type EncodedFilesOut = (Vec<KvMutation>, Vec<Vec<u8>>, SidecarPlan);

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    live: bool,
    pending_through: u64,
}

fn crypto_err(detail: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::SecurityError(format!("record file encryption: {detail}"))
}

fn file_key(
    content_key: &[u8; 32],
    sha256: &str,
) -> Result<([u8; 32], [u8; NONCE_SIZE]), FoldDbError> {
    let hash = hex_decode_array::<32>(sha256)
        .ok_or_else(|| crypto_err("the file hash is not 64 hex digits"))?;
    let (mut key, mut nonce) = ([0u8; 32], [0u8; NONCE_SIZE]);
    Hkdf::<Sha256>::new(Some(b"lastdb-file-blob-account-v2"), content_key)
        .expand(&[b"file-dek\0", hash.as_slice()].concat(), &mut key)
        .map_err(crypto_err)?;
    Hkdf::<Sha256>::new(Some(b"lastdb-file-blob-account-v2:nonce"), &key)
        .expand(b"aes-256-gcm-nonce", &mut nonce)
        .map_err(crypto_err)?;
    Ok((key, nonce))
}

pub(crate) fn seal_row(
    content_key: &[u8; 32],
    plain: &[u8],
) -> Result<(String, Vec<u8>), FoldDbError> {
    if plain.len() as u64 > LOCAL_FILE_INPUT_MEMORY_BYTES {
        let detail = "local file input exceeds native pending memory allowance".into();
        return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
            detail,
        }));
    }
    let sha256 = hex_lower(Sha256::digest(plain));
    let (key, nonce) = file_key(content_key, &sha256)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(crypto_err)?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plain)
        .map_err(crypto_err)?;
    Ok((
        sha256,
        [&[ENVELOPE_VERSION][..], &nonce, &ciphertext].concat(),
    ))
}

fn verify_row_key(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<[u8; 32], SchemaError> {
    let bad = || schema_inv("record file row differs from its format or marker");
    let expected = marker
        .size_bytes
        .checked_add((1 + NONCE_SIZE + TAG_SIZE) as u64);
    if !marker.has_valid_hash()
        || marker.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES
        || expected != Some(row.len() as u64)
        || row.first() != Some(&ENVELOPE_VERSION)
    {
        return Err(bad());
    }
    let (key, nonce) = file_key(content_key, &marker.sha256).map_err(|_| bad())?;
    (row.get(1..1 + NONCE_SIZE) == Some(nonce.as_slice()))
        .then_some(key)
        .ok_or_else(bad)
}

pub(crate) fn require_row(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<(), SchemaError> {
    verify_row_key(content_key, marker, row).map(|_| ())
}

pub(crate) fn decrypt_file_row(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<Vec<u8>, FoldDbError> {
    let mismatch = || FoldDbError::SecurityError("record file differs from its marker".into());
    let key = verify_row_key(content_key, marker, row).map_err(|_| mismatch())?;
    let plain = decrypt_envelope(&key, row).map_err(crypto_err)?;
    let valid = plain.len() as u64 == marker.size_bytes
        && hex_lower(Sha256::digest(&plain)) == marker.sha256;
    valid.then_some(plain).ok_or_else(mismatch)
}

impl Metadata {
    pub(in crate::storage::format) async fn stage_file_references(
        &self,
        changes: BTreeMap<String, bool>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        self.require_scope()?;
        let bad = |detail: &str| StorageError::invalid(format!("file reference index: {detail}"));
        let keys: Vec<_> = changes
            .keys()
            .map(|path| format!("{FILE_REFERENCE_PREFIX}{path}"))
            .collect();
        let requests = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let rows = self.store.get_many(requests).await?;
        if rows.len() != keys.len() {
            return Err(bad("incomplete file reference read"));
        }
        let (mut ops, mut retired) = (Vec::new(), Vec::new());
        for (((path, live), key), row) in changes.into_iter().zip(keys).zip(rows) {
            if let Some(bytes) = row.as_deref() {
                serde_json::from_slice::<Reference>(bytes).map_err(|e| bad(&e.to_string()))?;
            }
            if live {
                let reference = Reference {
                    live: true,
                    pending_through: 0,
                };
                let value = serde_json::to_vec(&reference).map_err(|e| bad(&e.to_string()))?;
                ops.push((key, Some(value)));
            } else {
                if row.is_some() {
                    ops.push((key, None));
                }
                retired.push(path.into_bytes());
            }
        }
        self.stage(ops).await.map(|()| retired)
    }

    pub(in crate::storage::format) async fn encode_files(
        &self,
        issuer: &FileWriteIssuer,
        content_key: &[u8; 32],
        changes: Vec<FileChange>,
    ) -> Result<EncodedFilesOut, SchemaError> {
        if changes.is_empty() {
            return Ok((Vec::new(), Vec::new(), SidecarPlan::default()));
        }
        let mut staged = StagedFiles::default();
        for change in changes {
            encode_file_change(change, issuer, content_key, &mut staged)?;
        }
        let retired = self.stage_file_references(staged.references).await?;
        Ok((staged.cas_puts, retired, staged.sidecar))
    }
}

fn encode_file_change(
    change: FileChange,
    issuer: &FileWriteIssuer,
    content_key: &[u8; 32],
    staged: &mut StagedFiles,
) -> Result<(), SchemaError> {
    let record_key = change.address.as_str();
    let before = change.before.as_deref();
    let prev = before.and_then(|record| record.file_version.as_ref());
    let next = change.after.file_version.as_ref();
    if prev == next {
        return (before.and_then(|record| record.file.as_ref()) == change.after.file.as_ref())
            .then_some(())
            .ok_or_else(|| schema_inv("a retained file version has a different marker"));
    }
    if let Some(version) = prev {
        let path = record_file_path(record_key, version)?;
        staged.references.insert(path.clone(), false);
        staged.sidecar.removals.push(path);
    }
    let Some(version) = next else { return Ok(()) };
    let path = record_file_path(record_key, version)?;
    staged.references.insert(path.clone(), true);
    let marker = change
        .after
        .file
        .as_ref()
        .ok_or_else(|| schema_inv("file row requires its marker"))?;
    match change.write {
        Some(FileWrite::Put(file)) if file.marker() == marker => {
            let row = issuer
                .admit(file)
                .ok_or_else(|| schema_inv("foreign file payload"))?;
            require_row(content_key, marker, &row)?;
            staged
                .cas_puts
                .push(KvMutation::put(path.into_bytes(), row));
            Ok(())
        }
        Some(FileWrite::Put(_)) => Err(schema_inv("file payload differs from its record marker")),
        Some(FileWrite::LocalPath(local)) if local.marker() == marker => {
            let local_path = issuer
                .admit_local(&local)
                .ok_or_else(|| schema_inv("foreign local file ref"))?;
            staged.sidecar.additions.push(SidecarAddition {
                cas_key: path,
                path: local_path.to_path_buf(),
                sha256: marker.sha256.clone(),
                size_bytes: marker.size_bytes,
            });
            Ok(())
        }
        Some(FileWrite::LocalPath(_)) => Err(schema_inv("local file ref differs from marker")),
        Some(FileWrite::Clear) | None => {
            Err(schema_inv("selected file needs payload or local ref"))
        }
    }
}
