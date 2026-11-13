//! Metadata namespace capability, file row encryption, and file reference tracking.

mod replay;

pub use replay::ReplayStamp;
pub(crate) use replay::{ReplayTarget, SchemaReplayDrop};

use super::RecordAddress;
use crate::crypto::envelope::{decrypt_envelope, ENVELOPE_VERSION, NONCE_SIZE, TAG_SIZE};
use crate::error::FoldDbError;
use crate::schema::types::{FileWrite, FileWriteIssuer, MutationAuthorClockState};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::local_file_ref::SidecarPlan;
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use lastdb_model::hex::{hex_decode_array, hex_lower};
use lastdb_model::portable_mutation::{Position, Stamp, StreamId, StreamProgress};
use lastdb_model::record_value::{FileMarker, RecordValueError, StoredRecord, MAX_RECORD_BYTES};
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(in crate::storage) const FILE_REFERENCE_PREFIX: &str = "portable-file-reference:";
pub(in crate::storage) const FILE_RETENTION_KEY: &str = "portable-file-retention";
pub(in crate::storage) const FILE_RETENTION_COMPLETE: &[u8] = b"\"complete\"";
pub(in crate::storage) const LOCAL_AUTHOR_KEY: &str = "portable-local-author";
const MAX_LOCAL_AUTHOR_BYTES: usize = 8192;

pub(in crate::storage::format) type EncodedFile =
    (RecordAddress, Arc<StoredRecord>, Option<FileWrite>);
pub(in crate::storage::format) type StagedFiles =
    (Vec<KvMutation>, BTreeMap<String, bool>, SidecarPlan);
type EncodedOut = (Vec<KvMutation>, Vec<Vec<u8>>, SidecarPlan);

fn crypto_err(d: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::SecurityError(format!("record file encryption: {d}"))
}
fn schema_inv(msg: impl Into<String>) -> SchemaError {
    SchemaError::InvalidData(msg.into())
}
fn read_err(e: impl std::fmt::Display) -> SchemaError {
    SchemaError::StorageRead {
        detail: e.to_string(),
    }
}
fn invalid_author(e: impl std::fmt::Display) -> StorageError {
    StorageError::invalid(format!("portable author state: {e}"))
}

fn file_key(k: &[u8; 32], sha256: &str) -> Result<([u8; 32], [u8; NONCE_SIZE]), FoldDbError> {
    let hash = hex_decode_array::<32>(sha256)
        .ok_or_else(|| crypto_err("the file hash is not 64 hex digits"))?;
    let (mut key, mut nonce) = ([0u8; 32], [0u8; NONCE_SIZE]);
    Hkdf::<Sha256>::new(Some(b"lastdb-file-blob-account-v2"), k)
        .expand(&[b"file-dek\0", hash.as_slice()].concat(), &mut key)
        .map_err(crypto_err)?;
    Hkdf::<Sha256>::new(Some(b"lastdb-file-blob-account-v2:nonce"), &key)
        .expand(b"aes-256-gcm-nonce", &mut nonce)
        .map_err(crypto_err)?;
    Ok((key, nonce))
}

pub(crate) fn seal_row(k: &[u8; 32], plain: &[u8]) -> Result<(String, Vec<u8>), FoldDbError> {
    if plain.len() as u64 > LOCAL_FILE_INPUT_MEMORY_BYTES {
        let d = "local file input exceeds native pending memory allowance".into();
        return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
            detail: d,
        }));
    }
    let sha256 = hex_lower(Sha256::digest(plain));
    let (key, nonce) = file_key(k, &sha256)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(crypto_err)?;
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), plain)
        .map_err(crypto_err)?;
    Ok((sha256, [&[ENVELOPE_VERSION][..], &nonce, &ct].concat()))
}

fn verify_row_key(k: &[u8; 32], m: &FileMarker, row: &[u8]) -> Result<[u8; 32], SchemaError> {
    let bad = || schema_inv("record file row differs from its format or marker");
    let exp = m.size_bytes.checked_add((1 + NONCE_SIZE + TAG_SIZE) as u64);
    if !m.has_valid_hash()
        || m.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES
        || exp != Some(row.len() as u64)
        || row.first() != Some(&ENVELOPE_VERSION)
    {
        return Err(bad());
    }
    let (key, nonce) = file_key(k, &m.sha256).map_err(|_| bad())?;
    (row.get(1..1 + NONCE_SIZE) == Some(nonce.as_slice()))
        .then_some(key)
        .ok_or_else(bad)
}

pub(crate) fn require_row(k: &[u8; 32], m: &FileMarker, row: &[u8]) -> Result<(), SchemaError> {
    verify_row_key(k, m, row).map(|_| ())
}

pub(crate) fn decrypt_file_row(
    k: &[u8; 32],
    m: &FileMarker,
    row: &[u8],
) -> Result<Vec<u8>, FoldDbError> {
    let mismatch = || FoldDbError::SecurityError("record file differs from its marker".into());
    let key = verify_row_key(k, m, row).map_err(|_| mismatch())?;
    let plain = decrypt_envelope(&key, row).map_err(crypto_err)?;
    let ok = plain.len() as u64 == m.size_bytes && hex_lower(Sha256::digest(&plain)) == m.sha256;
    ok.then_some(plain).ok_or_else(mismatch)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    live: bool,
    pending_through: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalAuthor {
    stream: StreamId,
    progress: StreamProgress,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clock_floor_ms: Option<u64>,
}

pub(in crate::storage) fn initial_local_author(home: uuid::Uuid) -> StorageResult<Vec<u8>> {
    let stream = StreamId::try_from(home.to_string()).map_err(invalid_author)?;
    let progress = StreamProgress::default();
    let st = LocalAuthor {
        stream,
        progress,
        clock_floor_ms: None,
    };
    serde_json::to_vec(&st).map_err(invalid_author)
}

pub(in crate::storage) fn validate_local_author(bytes: Option<&[u8]>) -> StorageResult<()> {
    decode_author(bytes).map(|_| ())
}

fn decode_author(bytes: Option<&[u8]>) -> StorageResult<LocalAuthor> {
    let b = bytes
        .filter(|b| b.len() <= MAX_LOCAL_AUTHOR_BYTES)
        .ok_or_else(|| invalid_author("local stream state missing or exceeds bound"))?;
    let s: LocalAuthor = serde_json::from_slice(b).map_err(invalid_author)?;
    s.progress.validate().map_err(invalid_author).map(|()| s)
}

pub(in crate::storage::format) fn record_value_error(error: &RecordValueError) -> SchemaError {
    match *error {
        RecordValueError::TooLarge { bytes } => SchemaError::RecordTooLarge {
            size: bytes,
            limit: MAX_RECORD_BYTES,
        },
        _ => schema_inv(format!("the record write is refused: {error}")),
    }
}

#[derive(Clone)]
pub(crate) struct Metadata(PackKvStore, Arc<PackRuntime>);

type ClockRes = Result<Option<MutationAuthorClockState>, SchemaError>;

impl Metadata {
    pub(in crate::storage::format) fn new(s: PackKvStore, r: Arc<PackRuntime>) -> Self {
        Self(s, r)
    }
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.1.flush().await
    }
    pub(crate) async fn author_clock(&self, w: &str) -> ClockRes {
        let key = super::mutation_author_clock_key(w);
        self.0
            .get_item(&key)
            .await
            .map_err(|e| schema_inv(format!("metadata get {key}: {e}")))
    }
    pub(crate) async fn stage_author_clock(
        &self,
        w: &str,
        st: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        let key = super::mutation_author_clock_key(w);
        let bytes = serde_json::to_vec(st)
            .map_err(|e| schema_inv(format!("metadata put {key}: Serialization error: {e}")))?;
        self.stage(vec![(key.clone(), Some(bytes))])
            .await
            .map_err(|e| match e {
                StorageError::WriteResourceLimit { detail } => {
                    SchemaError::WriteResourceLimit { detail }
                }
                _ => schema_inv(format!("metadata put {key}: {e}")),
            })
    }
    pub(crate) async fn stage_contribution_receipt(
        &self,
        id: &[u8; 32],
        dig: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        let key = format!("share-contribution-receipt:{}", hex_lower(id));
        self.stage_receipt(key, dig, "share mutation ID reused")
            .await
    }
    pub(in crate::storage::format) async fn stage_file_references(
        &self,
        changes: BTreeMap<String, bool>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        self.require_scope()?;
        let bad = |d: &str| StorageError::invalid(format!("file reference index: {d}"));
        let keys: Vec<_> = changes
            .keys()
            .map(|p| format!("{FILE_REFERENCE_PREFIX}{p}"))
            .collect();
        let req = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        let rows = self.0.get_many(req).await?;
        if rows.len() != keys.len() {
            return Err(bad("incomplete file reference read"));
        }
        let (mut ops, mut retired) = (Vec::new(), Vec::new());
        for (((path, live), key), row) in changes.into_iter().zip(keys).zip(rows) {
            if let Some(b) = row.as_deref() {
                serde_json::from_slice::<Reference>(b).map_err(|e| bad(&e.to_string()))?;
            }
            if live {
                let r = Reference {
                    live: true,
                    pending_through: 0,
                };
                let val = serde_json::to_vec(&r).map_err(|e| bad(&e.to_string()))?;
                ops.push((key, Some(val)));
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
        main: &PackKvStore,
        iss: &FileWriteIssuer,
        ckey: &[u8; 32],
        changes: Vec<EncodedFile>,
    ) -> Result<EncodedOut, SchemaError> {
        if changes.is_empty() {
            return Ok((Vec::new(), Vec::new(), SidecarPlan::default()));
        }
        let keys = changes.iter().map(|c| c.0 .0.as_bytes().to_vec()).collect();
        let stored = main.get_many(keys).await?;
        if stored.len() != changes.len() {
            return Err(schema_inv("incomplete file record read"));
        }
        let mut s = StagedFiles::default();
        for ((k, after, w), bytes) in changes.into_iter().zip(stored) {
            let before = bytes.as_deref().map(StoredRecord::decode).transpose();
            let before = before.map_err(|e| record_value_error(&e))?;
            encode_file_change((k.as_str(), &after, w), before.as_ref(), iss, ckey, &mut s)?;
        }
        Ok((s.0, self.stage_file_references(s.1).await?, s.2))
    }
    pub(crate) async fn stage_local_stamps(&self, schemas: &[&str]) -> StorageResult<Vec<Stamp>> {
        self.require_scope()?;
        if schemas.is_empty() {
            return Ok(Vec::new());
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(invalid_author)
            .and_then(|d| u64::try_from(d.as_millis()).map_err(invalid_author))?;
        let mut st = decode_author(self.0.get(LOCAL_AUTHOR_KEY.as_bytes()).await?.as_deref())?;
        let mut stamps = Vec::with_capacity(schemas.len());
        for _ in schemas {
            let seq = st.progress.applied_through.checked_add(1);
            let seq = seq.ok_or_else(|| invalid_author("local stream sequence exhausted"))?;
            let fl = st
                .clock_floor_ms
                .unwrap_or(0)
                .max(st.progress.applied_through_ms.unwrap_or(0));
            let at = now_ms.max(fl);
            (st.progress.applied_through, st.progress.applied_through_ms) = (seq, Some(at));
            let source_stream = st.stream.clone();
            let position = Position {
                source_stream,
                sequence: seq,
            };
            stamps.push(Stamp {
                authored_at_ms: at,
                position,
            });
        }
        let bytes = serde_json::to_vec(&st).map_err(invalid_author)?;
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid_author("local stream state exceeds its bound"));
        }
        self.stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(bytes))])
            .await
            .map(|()| stamps)
    }
    fn require_scope(&self) -> StorageResult<()> {
        transaction::PendingBatch::current(&self.1)
            .map(|_| ())
            .ok_or_else(|| StorageError::invalid("metadata write requires native write scope"))
    }
    async fn stage(&self, entries: Vec<(String, Option<Vec<u8>>)>) -> StorageResult<()> {
        self.require_scope()?;
        if !entries.is_empty() {
            let ops = entries
                .into_iter()
                .map(|(k, v)| match v {
                    Some(b) => KvMutation::put(k, b),
                    None => KvMutation::delete(k),
                })
                .collect();
            self.0.batch_mutate(ops).await?;
        }
        Ok(())
    }
}

fn encode_file_change(
    (rkey, after, write): (&str, &StoredRecord, Option<FileWrite>),
    before: Option<&StoredRecord>,
    issuer: &FileWriteIssuer,
    ckey: &[u8; 32],
    staged: &mut StagedFiles,
) -> Result<(), SchemaError> {
    let prev = before.and_then(|r| r.file_version.as_ref());
    let next = after.file_version.as_ref();
    if prev == next {
        return (before.and_then(|r| r.file.as_ref()) == after.file.as_ref())
            .then_some(())
            .ok_or_else(|| schema_inv("a retained file version has a different marker"));
    }
    if let Some(v) = prev {
        let path = record_file_path(rkey, v)?;
        staged.1.insert(path.clone(), false);
        staged.2 .1.push(path);
    }
    let Some(v) = next else { return Ok(()) };
    let path = record_file_path(rkey, v)?;
    staged.1.insert(path.clone(), true);
    let m = after
        .file
        .as_ref()
        .ok_or_else(|| schema_inv("file row requires its marker"))?;
    match write {
        Some(FileWrite::Put(f)) if f.marker() == m => {
            let row = issuer
                .admit(f)
                .ok_or_else(|| schema_inv("foreign file payload"))?;
            require_row(ckey, m, &row)?;
            staged.0.push(KvMutation::put(path.into_bytes(), row));
            Ok(())
        }
        Some(FileWrite::Put(_)) => Err(schema_inv("file payload differs from its record marker")),
        Some(FileWrite::LocalPath(l)) if l.marker() == m => {
            let p = issuer
                .admit_local(&l)
                .ok_or_else(|| schema_inv("foreign local file ref"))?;
            let item = (path, p.to_path_buf(), m.sha256.clone(), m.size_bytes);
            staged.2 .0.push(item);
            Ok(())
        }
        Some(FileWrite::LocalPath(_)) => Err(schema_inv("local file ref differs from marker")),
        Some(FileWrite::Clear) | None => {
            Err(schema_inv("selected file needs payload or local ref"))
        }
    }
}
