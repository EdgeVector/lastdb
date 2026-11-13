//! Metadata namespace capability, applied-delete framing, and file-row crypto.

mod replay;

pub use replay::ReplayStamp;
pub(crate) use replay::{ReplayTarget, SchemaReplayDrop};

use super::{
    FILE_DEK_LABEL, FILE_ENVELOPE_VERSION, FILE_KEY_SALT, FILE_NONCE_LABEL, FILE_NONCE_SALT,
    FILE_NONCE_SIZE, FILE_TAG_SIZE,
};
use crate::crypto::envelope::decrypt_envelope;
use crate::error::FoldDbError;
use crate::schema::types::MutationAuthorClockState;
use crate::schema::SchemaError;
use crate::storage::disk_pack::{transaction, PackKvStore, PackRuntime};
use crate::storage::{error::StorageResult, KvMutation, StorageError};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use lastdb_model::hex::{hex_decode_array, hex_lower};
use lastdb_model::portable_mutation::{Position, Stamp, StreamId, StreamProgress};
use lastdb_model::record_value::FileMarker;
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

#[derive(Deserialize, Serialize, Default)]
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
    let author = LocalAuthor {
        stream,
        progress: StreamProgress::default(),
        clock_floor_ms: None,
    };
    serde_json::to_vec(&author).map_err(invalid_author)
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

#[derive(Clone)]
pub(crate) struct Metadata(PackKvStore, Arc<PackRuntime>);

impl Metadata {
    pub(in crate::storage::format) fn new(s: PackKvStore, r: Arc<PackRuntime>) -> Self {
        Self(s, r)
    }

    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.1.flush().await
    }

    pub(crate) async fn author_clock(
        &self,
        writer: &str,
    ) -> Result<Option<MutationAuthorClockState>, SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        self.0
            .get_item(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("metadata get {key}: {e}")))
    }

    pub(crate) async fn stage_author_clock(
        &self,
        writer: &str,
        state: &MutationAuthorClockState,
    ) -> Result<(), SchemaError> {
        let key = super::mutation_author_clock_key(writer);
        let bytes = serde_json::to_vec(state).map_err(|e| {
            SchemaError::InvalidData(format!("metadata put {key}: Serialization error: {e}"))
        })?;
        self.stage(vec![(key.clone(), Some(bytes))])
            .await
            .map_err(|e| {
                if let StorageError::WriteResourceLimit { detail } = e {
                    SchemaError::WriteResourceLimit { detail }
                } else {
                    SchemaError::InvalidData(format!("metadata put {key}: {e}"))
                }
            })
    }

    pub(crate) async fn stage_contribution_receipt(
        &self,
        receipt_id: &[u8; 32],
        intent_digest: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        let key = format!("share-contribution-receipt:{}", hex_lower(receipt_id));
        self.stage_receipt(key, intent_digest, "share mutation ID reused")
            .await
    }

    pub(crate) async fn stage_replay_receipt(
        &self,
        receipt_id: &[u8; 32],
        payload_digest: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        let key = format!("signed-replay-receipt:{}", hex_lower(receipt_id));
        self.stage_receipt(key, payload_digest, "signed replay ID reused")
            .await
    }

    pub(crate) async fn replay_receipt_exists(
        &self,
        receipt_id: &[u8; 32],
        payload_digest: &[u8; 32],
    ) -> Result<bool, SchemaError> {
        let key = format!("signed-replay-receipt:{}", hex_lower(receipt_id));
        self.receipt_exists(&key, payload_digest, "signed replay ID reused")
            .await
    }

    async fn stage_receipt(
        &self,
        key: String,
        digest: &[u8; 32],
        reused: &str,
    ) -> Result<bool, SchemaError> {
        if self.receipt_exists(&key, digest, reused).await? {
            return Ok(true);
        }
        self.stage(vec![(key, Some(digest.to_vec()))])
            .await
            .map_err(read_err)?;
        Ok(false)
    }

    pub(in crate::storage::format) async fn stage_file_references(
        &self,
        changes: BTreeMap<String, bool>,
    ) -> StorageResult<Vec<Vec<u8>>> {
        if changes.is_empty() {
            return Ok(Vec::new());
        }
        self.require_scope()?;
        let keys: Vec<_> = changes
            .keys()
            .map(|p| format!("{FILE_REFERENCE_PREFIX}{p}"))
            .collect();
        let rows = self
            .0
            .get_many(keys.iter().map(|k| k.as_bytes().to_vec()).collect())
            .await?;
        if rows.len() != keys.len() {
            return Err(invalid_file_ref("incomplete file reference read"));
        }
        let (mut entries, mut retired) = (Vec::new(), Vec::new());
        for (((path, live), key), row) in changes.into_iter().zip(keys).zip(rows) {
            if let Some(b) = row.as_deref() {
                let _: Reference = serde_json::from_slice(b).map_err(invalid_file_ref)?;
            }
            if live {
                let r = Reference {
                    live: true,
                    pending_through: 0,
                };
                entries.push((key, Some(serde_json::to_vec(&r).map_err(invalid_file_ref)?)));
            } else {
                if row.is_some() {
                    entries.push((key, None));
                }
                retired.push(path.into_bytes());
            }
        }
        self.stage(entries).await?;
        Ok(retired)
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
        let mut state = decode_author(self.0.get(LOCAL_AUTHOR_KEY.as_bytes()).await?.as_deref())?;
        let mut stamps = Vec::with_capacity(schemas.len());
        for _ in schemas {
            let seq = state
                .progress
                .applied_through
                .checked_add(1)
                .ok_or_else(|| invalid_author("local stream sequence exhausted"))?;
            let at = now_ms
                .max(state.clock_floor_ms.unwrap_or_default())
                .max(state.progress.applied_through_ms.unwrap_or_default());
            (
                state.progress.applied_through,
                state.progress.applied_through_ms,
            ) = (seq, Some(at));
            stamps.push(Stamp {
                authored_at_ms: at,
                position: Position {
                    source_stream: state.stream.clone(),
                    sequence: seq,
                },
            });
        }
        let bytes = serde_json::to_vec(&state).map_err(invalid_author)?;
        if bytes.len() > MAX_LOCAL_AUTHOR_BYTES {
            return Err(invalid_author("local stream state exceeds its bound"));
        }
        self.stage(vec![(LOCAL_AUTHOR_KEY.to_owned(), Some(bytes))])
            .await?;
        Ok(stamps)
    }

    fn require_scope(&self) -> StorageResult<()> {
        transaction::PendingBatch::current(&self.1)
            .map(|_| ())
            .ok_or_else(|| {
                StorageError::invalid("a metadata write requires a complete native write scope")
            })
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

fn invalid_file_ref(detail: impl std::fmt::Display) -> StorageError {
    StorageError::invalid(format!("file reference index: {detail}"))
}

fn invalid_author(error: impl std::fmt::Display) -> StorageError {
    StorageError::invalid(format!("portable author state: {error}"))
}

fn read_err(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::StorageRead {
        detail: error.to_string(),
    }
}

pub(crate) mod applied_deletes {
    use super::super::records::RecordAddress;
    use crate::storage::{error::StorageResult, KvMutation, StorageError};
    use lastdb_model::portable_mutation::{AppliedDelete, Stamp};

    pub(in crate::storage) const APPLIED_DELETE_FAMILY: &str = "ad";

    pub(in crate::storage::format) fn encode(
        key: &RecordAddress,
        stamp: &Stamp,
    ) -> StorageResult<KvMutation> {
        let val = serde_json::to_vec(&AppliedDelete::from(stamp.clone())).map_err(inv)?;
        Ok(KvMutation::put(format!("ad:{}", key.as_str()), val))
    }

    pub(crate) fn record_key(key: &str) -> Option<&str> {
        key.strip_prefix("ad:")
    }

    pub(in crate::storage::format) fn decode(bytes: &[u8]) -> StorageResult<Stamp> {
        if bytes.len() > 512 {
            return Err(inv("encoded value exceeds its bound"));
        }
        let e: AppliedDelete = serde_json::from_slice(bytes).map_err(inv)?;
        e.delete_position.validate().map_err(inv).map(|()| e.into())
    }

    pub(in crate::storage::format) fn inv(error: impl std::fmt::Display) -> StorageError {
        StorageError::invalid(format!("applied delete: {error}"))
    }
}

fn crypto_err(detail: impl std::fmt::Display) -> FoldDbError {
    FoldDbError::SecurityError(format!("record file encryption: {detail}"))
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

pub(in crate::storage::format) fn seal_row(
    content_key: &[u8; 32],
    plaintext: &[u8],
) -> Result<(String, Vec<u8>), FoldDbError> {
    if plaintext.len() as u64 > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(FoldDbError::Schema(SchemaError::WriteResourceLimit {
            detail: "local file input exceeds native pending memory allowance".into(),
        }));
    }
    let sha256 = hex_lower(Sha256::digest(plaintext));
    let (key, nonce) = file_key(content_key, &sha256)?;
    let ct = Aes256Gcm::new_from_slice(&key)
        .map_err(crypto_err)?
        .encrypt(Nonce::from_slice(&nonce), plaintext)
        .map_err(crypto_err)?;
    Ok((sha256, [&[FILE_ENVELOPE_VERSION][..], &nonce, &ct].concat()))
}

fn verify_row_key(
    content_key: &[u8; 32],
    marker: &FileMarker,
    row: &[u8],
) -> Result<[u8; 32], SchemaError> {
    let inv =
        || SchemaError::InvalidData("record file row differs from its format or marker".into());
    let expected = marker
        .size_bytes
        .checked_add((1 + FILE_NONCE_SIZE + FILE_TAG_SIZE) as u64);
    if !marker.has_valid_hash()
        || marker.size_bytes > LOCAL_FILE_INPUT_MEMORY_BYTES
        || expected != Some(row.len() as u64)
        || row.first() != Some(&FILE_ENVELOPE_VERSION)
    {
        return Err(inv());
    }
    let (key, nonce) = file_key(content_key, &marker.sha256).map_err(|_| inv())?;
    (row.get(1..1 + FILE_NONCE_SIZE) == Some(nonce.as_slice()))
        .then_some(key)
        .ok_or_else(inv)
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
    (plain.len() as u64 == marker.size_bytes && hex_lower(Sha256::digest(&plain)) == marker.sha256)
        .then_some(plain)
        .ok_or_else(mismatch)
}
