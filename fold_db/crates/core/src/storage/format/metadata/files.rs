//! Record file row encryption and file retirement planning without metadata disk I/O.

use super::schema_inv;
use crate::crypto::envelope::{decrypt_envelope, ENVELOPE_VERSION, NONCE_SIZE, TAG_SIZE};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, FileWriteIssuer, SchemaError};
use crate::storage::current_keys::families::record_file_path;
use crate::storage::format::RecordAddress;
use crate::storage::local_file_ref::{SidecarAddition, SidecarPlan};
use crate::storage::KvMutation;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use lastdb_model::hex::{hex_decode_array, sha256_hex};
use lastdb_model::record_value::{FileMarker, StoredRecord};
use sha2::Sha256;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::storage::{StorageError, MAX_RECORD_FILE_BYTES};

const _: () = assert!(
    MAX_RECORD_FILE_BYTES as usize + 1 + NONCE_SIZE + TAG_SIZE
        == laststore::disk_run::MAX_RUN_VALUE_BYTES
);

pub(in crate::storage::format) struct FileChange {
    pub(in crate::storage::format) address: RecordAddress,
    pub(in crate::storage::format) before: Option<Arc<StoredRecord>>,
    pub(in crate::storage::format) after: Arc<StoredRecord>,
    pub(in crate::storage::format) write: Option<FileWrite>,
}

#[derive(Default)]
struct StagedFiles {
    cas_puts: Vec<KvMutation>,
    retired: BTreeSet<String>,
    sidecar: SidecarPlan,
}

type EncodedFilesOut = (Vec<KvMutation>, Vec<Vec<u8>>, SidecarPlan);

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
    if plain.len() as u64 > MAX_RECORD_FILE_BYTES {
        return Err(StorageError::InvalidOperation(format!(
            "record file exceeds the {MAX_RECORD_FILE_BYTES}-byte limit"
        ))
        .into());
    }
    let sha256 = sha256_hex(plain);
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
        || marker.size_bytes > MAX_RECORD_FILE_BYTES
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
    let valid = plain.len() as u64 == marker.size_bytes && sha256_hex(&plain) == marker.sha256;
    valid.then_some(plain).ok_or_else(mismatch)
}

pub(in crate::storage::format) fn encode_files(
    issuer: &FileWriteIssuer,
    content_key: &[u8; 32],
    changes: Vec<FileChange>,
    keep: &BTreeSet<String>,
) -> Result<EncodedFilesOut, SchemaError> {
    if changes.is_empty() {
        return Ok((Vec::new(), Vec::new(), SidecarPlan::default()));
    }
    let mut staged = StagedFiles::default();
    for change in changes {
        encode_file_change(change, issuer, content_key, keep, &mut staged)?;
    }
    let retired = staged.retired.into_iter().map(String::into_bytes).collect();
    Ok((staged.cas_puts, retired, staged.sidecar))
}

fn encode_file_change(
    change: FileChange,
    issuer: &FileWriteIssuer,
    content_key: &[u8; 32],
    keep: &BTreeSet<String>,
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
        // A saved replay winner still names this version. A later local edit may
        // retire its own file, but not the confirmed baseline or candidate.
        if !keep.contains(&path) {
            staged.retired.insert(path.clone());
            staged.sidecar.removals.push(path);
        }
    }
    let Some(version) = next else { return Ok(()) };
    let path = record_file_path(record_key, version)?;
    staged.retired.remove(&path);
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
