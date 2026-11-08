//! Local state of personal record blobs for cloud backup (design
//! `design-lastdb-blob-files-on-demand-20261107`).
//!
//! Each record file version (`fb:<suffix>` in `cas_blobs`) is one B2 object at
//! a path derived from the record. Three marker families in `cas_blobs` share
//! the same suffix:
//!
//! - `fp:` the version was created here and is not uploaded yet. A commit that
//!   adds an `fb:` row without an `fu:` row adds it.
//! - `fu:` the version is in the cloud: the service accepted the job that
//!   uploaded it (or the version was downloaded from its path).
//! - `fr:` the version was dropped: the next job releases its cloud object.
//!   The CAS delete commit that retires the `fb:` row adds it, after the main
//!   publication that dropped the version, so a crash cannot release a
//!   version that a record still uses. A release for a path that was never
//!   uploaded is harmless.
//!
//! Marker values are not read, only their presence, so they skip the
//! storage envelope that the record format puts on file rows.
//!
//! A shared database keeps its eager CAS backup and uses none of this.

use super::super::traits::{KvMutation, KvStore};
use super::physical::CAS_COLLECTION;
use super::*;
use crate::storage::format::{
    PENDING_UPLOAD_PREFIX, RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX, RELEASED_FILE_PREFIX,
};
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::Id;
use std::collections::BTreeMap;

/// Marker rows hold no data; the key is the fact.
const MARKER: &[u8] = b"1";
/// Rows read per scan page. A `fb:` row holds a whole sealed file.
const FILE_PAGE: usize = 4;
const MARKER_PAGE: usize = 256;
/// Marker writes per native write request.
const WRITE_CHUNK: usize = 512;

/// One record file version and its cloud path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRow {
    /// The `fb:` key of the version (uploads) or its `fr:` key (releases).
    pub key: String,
    pub path: BlobPath,
}

/// The cloud path of a record file version from any key of its families.
pub fn blob_path_of_key(database: &Id, key: &str) -> StorageResult<BlobPath> {
    let invalid = || StorageError::InvalidOperation("record file key has no blob path".into());
    let suffix = suffix(key).ok_or_else(invalid)?;
    let mut parts = suffix.splitn(3, ':');
    let (Some(space), Some(record), Some(write_id)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    let path = BlobPath {
        database: database.clone(),
        space: space.to_owned().try_into().map_err(|_| invalid())?,
        record: record.to_owned().try_into().map_err(|_| invalid())?,
        write_id: write_id.to_owned().try_into().map_err(|_| invalid())?,
    };
    path.validate().map_err(|_| invalid())?;
    Ok(path)
}

fn suffix(key: &str) -> Option<&str> {
    [
        RECORD_FILE_PREFIX,
        RECORD_FILE_UPLOADED_PREFIX,
        PENDING_UPLOAD_PREFIX,
        RELEASED_FILE_PREFIX,
    ]
    .into_iter()
    .find_map(|prefix| key.strip_prefix(prefix))
}

fn raw(op: &HomeMutation) -> Option<String> {
    let key = native_routing::decode(&op.key).ok()?;
    String::from_utf8(key).ok()
}

fn marker(prefix: &str, suffix: &str, put: bool) -> HomeMutation {
    HomeMutation {
        collection: CAS_COLLECTION.into(),
        key: native_routing::encode(format!("{prefix}{suffix}").as_bytes()),
        body: put.then(|| MARKER.to_vec()),
    }
}

/// A new `fb:` row without an `fu:` row in the same batch was created here:
/// queue its upload. A download writes `fb:` and `fu:` together.
pub(super) fn queue_new_uploads(cas: &mut BTreeMap<String, HomeMutation>) {
    let mut queued = Vec::new();
    for op in cas.values().filter(|op| op.body.is_some()) {
        let Some(key) = raw(op) else { continue };
        let Some(suffix) = key.strip_prefix(RECORD_FILE_PREFIX) else {
            continue;
        };
        let uploaded = marker(RECORD_FILE_UPLOADED_PREFIX, suffix, true);
        if cas.get(&uploaded.key).is_some_and(|op| op.body.is_some()) {
            continue;
        }
        queued.push(marker(PENDING_UPLOAD_PREFIX, suffix, true));
    }
    for op in queued {
        cas.entry(op.key.clone()).or_insert(op);
    }
}

/// The CAS delete commit: each retired `fb:` row also loses its upload
/// markers and queues a cloud release. Explicit operations of the batch win.
pub(super) fn with_releases(deletes: Vec<HomeMutation>) -> Vec<HomeMutation> {
    let mut out = BTreeMap::new();
    let mut derived = Vec::new();
    for op in deletes {
        if let Some(suffix) = raw(&op)
            .as_deref()
            .and_then(|key| key.strip_prefix(RECORD_FILE_PREFIX))
        {
            derived.push(marker(RECORD_FILE_UPLOADED_PREFIX, suffix, false));
            derived.push(marker(PENDING_UPLOAD_PREFIX, suffix, false));
            derived.push(marker(RELEASED_FILE_PREFIX, suffix, true));
        }
        out.insert(op.key.clone(), op);
    }
    for op in derived {
        out.entry(op.key.clone()).or_insert(op);
    }
    out.into_values().collect()
}

impl PackRuntime {
    fn blob_store(self: &Arc<Self>) -> StorageResult<PackKvStore> {
        if self.physical.shared.is_some() {
            return Err(StorageError::InvalidOperation(
                "a shared database backs up its CAS home, not blobs by path".into(),
            ));
        }
        Ok(PackKvStore {
            runtime: Arc::clone(self),
            namespace: CAS_COLLECTION.into(),
        })
    }

    /// Queue every local `fb:` row that has neither an `fu:` nor an `fp:` row.
    /// A snapshot job runs this first, so versions that predate the upload
    /// markers (or lost them) upload once. It reads every file row.
    pub async fn queue_unuploaded_blobs(self: &Arc<Self>) -> StorageResult<u64> {
        let store = self.blob_store()?;
        let mut after = None;
        let mut puts = Vec::new();
        let mut queued = 0u64;
        loop {
            let rows = page(&store, RECORD_FILE_PREFIX, after.take(), FILE_PAGE).await?;
            let done = rows.len() < FILE_PAGE;
            let suffixes: Vec<String> = rows
                .iter()
                .filter_map(|(key, _)| std::str::from_utf8(key).ok())
                .filter_map(|key| key.strip_prefix(RECORD_FILE_PREFIX))
                .map(str::to_owned)
                .collect();
            after = rows.last().map(|(key, _)| key.clone());
            drop(rows);
            let keys = suffixes
                .iter()
                .flat_map(|suffix| {
                    [RECORD_FILE_UPLOADED_PREFIX, PENDING_UPLOAD_PREFIX]
                        .map(|prefix| format!("{prefix}{suffix}").into_bytes())
                })
                .collect();
            let markers = store.get_many(keys).await?;
            for (suffix, pair) in suffixes.iter().zip(markers.chunks(2)) {
                if pair.iter().all(Option::is_none) {
                    puts.push(KvMutation::put(
                        format!("{PENDING_UPLOAD_PREFIX}{suffix}").into_bytes(),
                        MARKER.to_vec(),
                    ));
                }
            }
            if puts.len() >= WRITE_CHUNK || (done && !puts.is_empty()) {
                queued += puts.len() as u64;
                store.batch_mutate(std::mem::take(&mut puts)).await?;
            }
            if done || after.is_none() {
                return Ok(queued);
            }
        }
    }

    /// Up to `limit` queued versions after the `fb:` key `after` that are not
    /// in the cloud, in key order, with their paths under `database`. A queued
    /// version that is already uploaded loses its stale `fp:` row.
    pub async fn pending_blob_uploads(
        self: &Arc<Self>,
        database: &Id,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<BlobRow>> {
        let store = self.blob_store()?;
        let mut uploads = Vec::new();
        let mut stale = Vec::new();
        let mut after = after
            .map(|key| {
                key.strip_prefix(RECORD_FILE_PREFIX)
                    .map(|suffix| format!("{PENDING_UPLOAD_PREFIX}{suffix}").into_bytes())
                    .ok_or_else(|| StorageError::InvalidOperation("upload cursor".into()))
            })
            .transpose()?;
        while uploads.len() < limit {
            let rows = page(&store, PENDING_UPLOAD_PREFIX, after.take(), MARKER_PAGE).await?;
            let done = rows.len() < MARKER_PAGE;
            after = rows.last().map(|(key, _)| key.clone());
            let suffixes: Vec<String> = rows
                .iter()
                .filter_map(|(key, _)| std::str::from_utf8(key).ok())
                .filter_map(|key| key.strip_prefix(PENDING_UPLOAD_PREFIX))
                .map(str::to_owned)
                .collect();
            let uploaded = store
                .get_many(
                    suffixes
                        .iter()
                        .map(|suffix| format!("{RECORD_FILE_UPLOADED_PREFIX}{suffix}").into_bytes())
                        .collect(),
                )
                .await?;
            for (suffix, uploaded) in suffixes.iter().zip(uploaded) {
                if uploaded.is_some() {
                    stale.push(KvMutation::delete(
                        format!("{PENDING_UPLOAD_PREFIX}{suffix}").into_bytes(),
                    ));
                } else if uploads.len() < limit {
                    let key = format!("{RECORD_FILE_PREFIX}{suffix}");
                    let path = blob_path_of_key(database, &key)?;
                    uploads.push(BlobRow { key, path });
                }
            }
            if done || after.is_none() {
                break;
            }
        }
        for chunk in stale.chunks(WRITE_CHUNK) {
            store.batch_mutate(chunk.to_vec()).await?;
        }
        Ok(uploads)
    }

    /// Up to `limit` queued releases (`fr:` keys) after the `fr:` key
    /// `after`, in key order, with their paths. A caller pages through every
    /// queued release by passing the last key it received.
    pub async fn pending_blob_releases(
        self: &Arc<Self>,
        database: &Id,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<BlobRow>> {
        let store = self.blob_store()?;
        let after = after
            .map(|key| {
                key.starts_with(RELEASED_FILE_PREFIX)
                    .then(|| key.as_bytes().to_vec())
                    .ok_or_else(|| StorageError::InvalidOperation("release cursor".into()))
            })
            .transpose()?;
        let rows = page(&store, RELEASED_FILE_PREFIX, after, limit).await?;
        rows.into_iter()
            .map(|(key, _)| {
                let key = String::from_utf8(key).map_err(|_| {
                    StorageError::InvalidOperation("record file release key".into())
                })?;
                let path = blob_path_of_key(database, &key)?;
                Ok(BlobRow { key, path })
            })
            .collect()
    }

    /// The sealed row of one local record file version, as the record format
    /// reads it (the storage envelope of `cas_blobs` removed), or None when
    /// the version is gone. A download installs exactly these bytes.
    pub async fn record_blob_bytes(
        self: &Arc<Self>,
        key: &str,
        content_key: &[u8; 32],
    ) -> StorageResult<Option<Vec<u8>>> {
        if !key.starts_with(RECORD_FILE_PREFIX) {
            return Err(StorageError::InvalidOperation(
                "a blob upload reads a record file key".into(),
            ));
        }
        let files = crate::storage::encrypting_store::EncryptingKvStore::new(
            Arc::new(self.blob_store()?),
            Arc::new(crate::crypto::LocalCryptoProvider::from_key(*content_key)),
        );
        files.get(key.as_bytes()).await
    }

    /// After the service accepted a job: its uploaded versions are in the
    /// cloud and its releases are recorded. Repeating this is harmless.
    pub async fn blobs_accepted(
        self: &Arc<Self>,
        uploaded: &[String],
        released: &[String],
    ) -> StorageResult<()> {
        let store = self.blob_store()?;
        let mut mutations = Vec::new();
        for key in uploaded {
            let suffix = key
                .strip_prefix(RECORD_FILE_PREFIX)
                .ok_or_else(|| StorageError::InvalidOperation("uploaded file key".into()))?;
            mutations.push(KvMutation::put(
                format!("{RECORD_FILE_UPLOADED_PREFIX}{suffix}").into_bytes(),
                MARKER.to_vec(),
            ));
            mutations.push(KvMutation::delete(
                format!("{PENDING_UPLOAD_PREFIX}{suffix}").into_bytes(),
            ));
        }
        for key in released {
            if !key.starts_with(RELEASED_FILE_PREFIX) {
                return Err(StorageError::InvalidOperation("released file key".into()));
            }
            mutations.push(KvMutation::delete(key.as_bytes().to_vec()));
        }
        for chunk in mutations.chunks(WRITE_CHUNK) {
            store.batch_mutate(chunk.to_vec()).await?;
        }
        Ok(())
    }
}

/// One page of a key family, strictly after `after`.
async fn page(
    store: &PackKvStore,
    prefix: &str,
    after: Option<Vec<u8>>,
    limit: usize,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut end = prefix.as_bytes().to_vec();
    *end.last_mut().expect("family separator") = b';';
    let start = match after {
        Some(after) => [after, vec![0]].concat(),
        None => prefix.as_bytes().to_vec(),
    };
    store.scan_range_paged(&start, &end, limit).await
}
