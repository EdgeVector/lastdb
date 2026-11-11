//! Local state of personal record blobs for cloud backup (design
//! `design-lastdb-blob-files-on-demand-20261107`).

use super::super::traits::KvMutation;
use super::physical::CAS_COLLECTION;
use super::*;
use crate::storage::format::{
    PENDING_UPLOAD_PREFIX, RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX, RELEASED_FILE_PREFIX,
};
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::Id;
use std::collections::BTreeMap;

const MARKER: &[u8] = b"1";
const FILE_PAGE: usize = 4;
const MARKER_PAGE: usize = 256;
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
    let suffix = [
        RECORD_FILE_PREFIX,
        RECORD_FILE_UPLOADED_PREFIX,
        PENDING_UPLOAD_PREFIX,
        RELEASED_FILE_PREFIX,
    ]
    .into_iter()
    .find_map(|prefix| key.strip_prefix(prefix))
    .ok_or_else(invalid)?;
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

fn raw_suffix<'a>(op: &'a HomeMutation, decoded: &'a mut String) -> Option<&'a str> {
    *decoded = String::from_utf8(native_routing::decode(&op.key).ok()?).ok()?;
    decoded.strip_prefix(RECORD_FILE_PREFIX)
}

fn marker(prefix: &str, suffix: &str, put: bool) -> HomeMutation {
    HomeMutation {
        collection: CAS_COLLECTION.into(),
        key: native_routing::encode(format!("{prefix}{suffix}").as_bytes()),
        body: put.then(|| MARKER.to_vec()),
    }
}

/// Queue upload markers for new `fb:` rows that lack an `fu:` row in the same batch.
pub(in crate::storage::disk_pack) fn queue_new_uploads(cas: &mut BTreeMap<String, HomeMutation>) {
    let mut queued = Vec::new();
    let mut buf = String::new();
    for op in cas.values().filter(|op| op.body.is_some()) {
        let Some(suffix) = raw_suffix(op, &mut buf) else {
            continue;
        };
        let uploaded = marker(RECORD_FILE_UPLOADED_PREFIX, suffix, true);
        if cas.get(&uploaded.key).is_none_or(|op| op.body.is_none()) {
            queued.push(marker(PENDING_UPLOAD_PREFIX, suffix, true));
        }
    }
    for op in queued {
        cas.entry(op.key.clone()).or_insert(op);
    }
}

/// Each retired `fb:` row also loses its upload markers and queues a cloud release.
pub(in crate::storage::disk_pack) fn with_releases(
    deletes: Vec<HomeMutation>,
) -> Vec<HomeMutation> {
    let (mut out, mut derived, mut buf) = (BTreeMap::new(), Vec::new(), String::new());
    for op in deletes {
        if let Some(suffix) = raw_suffix(&op, &mut buf) {
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

fn suffixes_of(rows: &[(Vec<u8>, Vec<u8>)], prefix: &str) -> Vec<String> {
    rows.iter()
        .filter_map(|(k, _)| std::str::from_utf8(k).ok()?.strip_prefix(prefix))
        .map(str::to_owned)
        .collect()
}

impl PackRuntime {
    /// Queue every local `fb:` row that has neither an `fu:` nor an `fp:` row.
    pub async fn queue_unuploaded_blobs(self: &Arc<Self>) -> StorageResult<u64> {
        let store = self.cas_kv();
        let (mut after, mut puts, mut queued) = (None, Vec::new(), 0u64);
        loop {
            let rows = page(&store, RECORD_FILE_PREFIX, after.take(), FILE_PAGE).await?;
            let done = rows.len() < FILE_PAGE;
            let suffixes = suffixes_of(&rows, RECORD_FILE_PREFIX);
            after = rows.last().map(|(key, _)| key.clone());
            drop(rows);
            let keys = suffixes
                .iter()
                .flat_map(|s| {
                    [RECORD_FILE_UPLOADED_PREFIX, PENDING_UPLOAD_PREFIX]
                        .map(|p| format!("{p}{s}").into_bytes())
                })
                .collect();
            let markers = store.get_many(keys).await?;
            for (suffix, pair) in suffixes.iter().zip(markers.chunks(2)) {
                if pair.iter().all(Option::is_none) {
                    puts.push(KvMutation::put(
                        format!("{PENDING_UPLOAD_PREFIX}{suffix}"),
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

    /// Up to `limit` queued versions after the `fb:` key `after` that are not in the cloud.
    pub async fn pending_blob_uploads(
        self: &Arc<Self>,
        database: &Id,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<BlobRow>> {
        let store = self.cas_kv();
        let (mut uploads, mut stale) = (Vec::new(), Vec::new());
        let mut after = after
            .map(|key| {
                key.strip_prefix(RECORD_FILE_PREFIX)
                    .map(|s| format!("{PENDING_UPLOAD_PREFIX}{s}").into_bytes())
                    .ok_or_else(|| StorageError::InvalidOperation("upload cursor".into()))
            })
            .transpose()?;
        while uploads.len() < limit {
            let rows = page(&store, PENDING_UPLOAD_PREFIX, after.take(), MARKER_PAGE).await?;
            let done = rows.len() < MARKER_PAGE;
            after = rows.last().map(|(key, _)| key.clone());
            let suffixes = suffixes_of(&rows, PENDING_UPLOAD_PREFIX);
            let uploaded = store
                .get_many(
                    suffixes
                        .iter()
                        .map(|s| format!("{RECORD_FILE_UPLOADED_PREFIX}{s}").into_bytes())
                        .collect(),
                )
                .await?;
            for (suffix, uploaded) in suffixes.iter().zip(uploaded) {
                if uploaded.is_some() {
                    stale.push(KvMutation::delete(format!(
                        "{PENDING_UPLOAD_PREFIX}{suffix}"
                    )));
                } else if uploads.len() < limit {
                    let key = format!("{RECORD_FILE_PREFIX}{suffix}");
                    uploads.push(BlobRow {
                        path: blob_path_of_key(database, &key)?,
                        key,
                    });
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

    /// Up to `limit` queued releases (`fr:` keys) after `after`, in key order.
    pub async fn pending_blob_releases(
        self: &Arc<Self>,
        database: &Id,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<BlobRow>> {
        let store = self.cas_kv();
        let after = after
            .map(|key| {
                key.starts_with(RELEASED_FILE_PREFIX)
                    .then(|| key.as_bytes().to_vec())
                    .ok_or_else(|| StorageError::InvalidOperation("release cursor".into()))
            })
            .transpose()?;
        page(&store, RELEASED_FILE_PREFIX, after, limit)
            .await?
            .into_iter()
            .map(|(key, _)| {
                let key = String::from_utf8(key).map_err(|_| {
                    StorageError::InvalidOperation("record file release key".into())
                })?;
                Ok(BlobRow {
                    path: blob_path_of_key(database, &key)?,
                    key,
                })
            })
            .collect()
    }

    /// The sealed row of one local record file version, or None when gone.
    pub async fn record_blob_bytes(self: &Arc<Self>, key: &str) -> StorageResult<Option<Vec<u8>>> {
        if !key.starts_with(RECORD_FILE_PREFIX) {
            return Err(StorageError::InvalidOperation(
                "a blob upload reads a record file key".into(),
            ));
        }
        self.cas_kv().get(key.as_bytes()).await
    }

    /// Record uploaded versions and completed releases after a backup job.
    pub async fn blobs_accepted(
        self: &Arc<Self>,
        uploaded: &[String],
        released: &[String],
    ) -> StorageResult<()> {
        let store = self.cas_kv();
        let mut mutations = Vec::with_capacity(uploaded.len() * 2 + released.len());
        for key in uploaded {
            let suffix = key
                .strip_prefix(RECORD_FILE_PREFIX)
                .ok_or_else(|| StorageError::InvalidOperation("uploaded file key".into()))?;
            mutations.push(KvMutation::put(
                format!("{RECORD_FILE_UPLOADED_PREFIX}{suffix}"),
                MARKER.to_vec(),
            ));
            mutations.push(KvMutation::delete(format!(
                "{PENDING_UPLOAD_PREFIX}{suffix}"
            )));
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

async fn page(
    store: &PackKvStore,
    prefix: &str,
    after: Option<Vec<u8>>,
    limit: usize,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut end = prefix.as_bytes().to_vec();
    *end.last_mut().expect("family separator") = b';';
    let start = after.map_or_else(|| prefix.as_bytes().to_vec(), |a| [a, vec![0]].concat());
    store.scan_range_paged(&start, &end, limit).await
}
