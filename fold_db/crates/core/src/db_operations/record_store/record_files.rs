//! The one file of a record (design `design-lastdb-no-shared-file-blobs`).
//!
//! The file lives in `cas_blobs`, the CAS run home that the backup sends to
//! B2, at a path made from the record key:
//! `{scope:}fb:{S}:{sha256(record storage key)}`. One record has one path, so
//! two records never share bytes and nothing counts references. The marker of
//! the record (`StoredRecord::file`) says whether the path holds a file.
//!
//! Every write that changes the marker changes the path in the same native
//! request: `put-blob` puts the sealed row, and `delete-blob`, a record
//! delete, a schema release and a database drop delete it.
//!
//! The row is a sealed `ContentAddressedBlob`: the account-v2 envelope of the
//! plaintext. Its key comes from the account key and the marker's plaintext
//! hash, so no key is stored. A new file at the same path has a new hash and
//! so a new key and nonce.

use super::RecordStore;
use crate::error::FoldDbError;
use crate::schema::SchemaError;
use crate::sharing::delivery_wire::{
    content_addressed_blob_from_envelope, decode_blob_bytes_with_dek, ContentAddressedBlob,
};
use crate::storage::{KvMutation, KvStore};
use lastdb_model::hex::hex_lower;
use lastdb_model::record_space::RECORD_PREFIX;
use lastdb_model::record_value::{FileMarker, StoredRecord};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// The namespace of the file rows: its own run home, sent to B2.
pub(crate) const CAS_BLOB_TREE: &str = "cas_blobs";

/// The file path of the record at one storage key, scope included.
pub(crate) fn file_path(record: &str) -> Result<String, SchemaError> {
    Ok(crate::storage::current_keys::families::record_file_path(record)?)
}

/// The file row change of one record write: the new row when the write set the
/// file, a delete of the path when the record lost its file. `row` is the
/// sealed row of the mutation that wrote the record, if it set a file.
pub(crate) fn file_row_change(
    key: &str,
    before: Option<&StoredRecord>,
    after: &StoredRecord,
    row: Option<Arc<Vec<u8>>>,
) -> Result<Option<KvMutation>, SchemaError> {
    let had = before.is_some_and(|record| record.file.is_some());
    let stamp_of = |record: Option<&StoredRecord>| {
        record.and_then(|record| record.file.as_ref().map(|file| file.stamp.clone()))
    };
    let set = after.file.is_some() && stamp_of(before) != stamp_of(Some(after));
    Ok(match row {
        Some(row) if set => Some(KvMutation::put(
            file_path(key)?.into_bytes(),
            Arc::try_unwrap(row).unwrap_or_else(|row| row.as_ref().clone()),
        )),
        _ if had && after.file.is_none() => Some(KvMutation::delete(file_path(key)?.into_bytes())),
        _ => None,
    })
}

/// The file paths that a drop of these stored rows releases: each record row
/// with a file, and each record row that does not decode (its file, if any,
/// goes too). Other rows hold no file.
pub(crate) fn dropped_file_paths<'a>(
    rows: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
) -> Vec<Vec<u8>> {
    let mut paths = Vec::new();
    for (key, value) in rows {
        let Ok(key) = std::str::from_utf8(key) else {
            continue;
        };
        let Ok(Some(path)) = record_row_path(key) else {
            continue;
        };
        let no_file = matches!(StoredRecord::decode(value), Ok(record) if record.file.is_none());
        if !no_file {
            paths.push(path.into_bytes());
        }
    }
    paths
}

/// The file path of a record row key, or `None` for a row of another family.
fn record_row_path(key: &str) -> Result<Option<String>, SchemaError> {
    let scope = crate::storage::current_keys::exact_scope(key)?;
    let bare = scope.map_or(key, |scope| key.get(scope.len() + 1..).unwrap_or(key));
    if !bare.starts_with(RECORD_PREFIX) {
        return Ok(None);
    }
    file_path(key).map(Some)
}

impl RecordStore {
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
        let (sealed, envelope) = self.seal_local_file_blob(plaintext)?;
        let size_bytes = plaintext.len() as u64;
        let mut row = content_addressed_blob_from_envelope(
            size_bytes,
            None,
            None,
            &sealed.to_access(),
            &envelope,
        )?;
        drop(envelope);
        row.stored_at = Some(chrono::Utc::now().to_rfc3339());
        // One buffer sized up front: the sealed text is the whole row.
        let mut bytes = Vec::with_capacity(row.bytes_b64.len() + 1024);
        serde_json::to_writer(&mut bytes, &row)?;
        let marker = FileMarker {
            name,
            media_type,
            size_bytes,
            sha256: sealed.file_hash,
        };
        Ok((marker, bytes))
    }

    /// The plaintext of the file of the record at `record_key`, checked
    /// against its marker. `None` when the path holds no row.
    pub(crate) async fn open_record_file(
        &self,
        record_key: &str,
        marker: &FileMarker,
    ) -> Result<Option<Vec<u8>>, FoldDbError> {
        let path = file_path(record_key)?;
        let Some(bytes) = self.file_store().await?.get(path.as_bytes()).await? else {
            return Ok(None);
        };
        let row: ContentAddressedBlob = serde_json::from_slice(&bytes)?;
        drop(bytes);
        let plaintext = decode_blob_bytes_with_dek(&row, &self.local_file_dek_hex(&marker.sha256)?)?;
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
