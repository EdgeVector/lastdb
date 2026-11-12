//! Local blob backup markers, upload/release queues, and cloud blob path mapping.

use super::super::traits::KvMutation;
use super::physical::CAS_COLLECTION;
use super::*;
use crate::storage::format::metadata::FILE_REFERENCE_PREFIX;
use crate::storage::format::{
    METADATA_NAMESPACE, PENDING_UPLOAD_PREFIX, RECORD_FILE_PREFIX, RECORD_FILE_UPLOADED_PREFIX,
    RELEASED_FILE_PREFIX,
};
use cloud_backup_protocol::{v2::blobs::BlobPath, Id};
use std::collections::BTreeMap;

const MARKER: &[u8] = b"1";
const FILE_PAGE: usize = 4;
const MARKER_PAGE: usize = 256;
const WRITE_CHUNK: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRow {
    pub key: String,
    pub path: BlobPath,
}

fn invalid(message: &str) -> StorageError {
    StorageError::InvalidOperation(message.into())
}

pub fn blob_path_of_key(database: &Id, key: &str) -> StorageResult<BlobPath> {
    let err = || invalid("record file key has no blob path");
    let suffix = [
        RECORD_FILE_PREFIX,
        RECORD_FILE_UPLOADED_PREFIX,
        PENDING_UPLOAD_PREFIX,
        RELEASED_FILE_PREFIX,
    ]
    .into_iter()
    .find_map(|p| key.strip_prefix(p))
    .ok_or_else(err)?;
    let [space, record, write_id]: [&str; 3] = suffix
        .splitn(3, ':')
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| err())?;
    let path = BlobPath {
        database: database.clone(),
        space: space.to_owned().try_into().map_err(|_| err())?,
        record: record.to_owned().try_into().map_err(|_| err())?,
        write_id: write_id.to_owned().try_into().map_err(|_| err())?,
    };
    path.validate().map_err(|_| err())?;
    Ok(path)
}

fn blob_row(database: &Id, key: String) -> StorageResult<BlobRow> {
    Ok(BlobRow {
        path: blob_path_of_key(database, &key)?,
        key,
    })
}

fn raw_suffix(op: &HomeMutation) -> Option<String> {
    let decoded = String::from_utf8(native_routing::decode(&op.key).ok()?).ok()?;
    decoded.strip_prefix(RECORD_FILE_PREFIX).map(str::to_owned)
}

fn marker(prefix: &str, suffix: &str, put: bool) -> HomeMutation {
    HomeMutation {
        collection: CAS_COLLECTION.into(),
        key: native_routing::encode(format!("{prefix}{suffix}").as_bytes()),
        body: put.then(|| MARKER.to_vec()),
    }
}

pub(in crate::storage::disk_pack) fn queue_new_uploads(cas: &mut BTreeMap<String, HomeMutation>) {
    let queued: Vec<_> = cas
        .values()
        .filter(|op| op.body.is_some())
        .filter_map(raw_suffix)
        .filter(|s| {
            let up = marker(RECORD_FILE_UPLOADED_PREFIX, s, true);
            cas.get(&up.key).is_none_or(|op| op.body.is_none())
        })
        .map(|s| marker(PENDING_UPLOAD_PREFIX, &s, true))
        .collect();
    for op in queued {
        cas.entry(op.key.clone()).or_insert(op);
    }
}

pub(in crate::storage::disk_pack) fn with_releases(
    deletes: Vec<HomeMutation>,
) -> Vec<HomeMutation> {
    let mut out = BTreeMap::new();
    for op in deletes {
        if let Some(s) = raw_suffix(&op) {
            for d in [
                marker(RECORD_FILE_UPLOADED_PREFIX, &s, false),
                marker(PENDING_UPLOAD_PREFIX, &s, false),
                marker(RELEASED_FILE_PREFIX, &s, true),
            ] {
                out.entry(d.key.clone()).or_insert(d);
            }
        }
        out.insert(op.key.clone(), op);
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
    pub async fn has_external_backup_files(self: &Arc<Self>) -> StorageResult<bool> {
        let meta = PackKvStore {
            runtime: Arc::clone(self),
            namespace: METADATA_NAMESPACE.into(),
        };
        let cas = self.cas_kv();
        for (store, p) in [
            (&meta, FILE_REFERENCE_PREFIX),
            (&cas, PENDING_UPLOAD_PREFIX),
            (&cas, RECORD_FILE_UPLOADED_PREFIX),
            (&cas, RELEASED_FILE_PREFIX),
            (&cas, RECORD_FILE_PREFIX),
        ] {
            if !store.prefix_rows(p.as_bytes(), 1, false).await?.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn queue_unuploaded_blobs(self: &Arc<Self>) -> StorageResult<u64> {
        let store = self.cas_kv();
        let (mut after, mut puts, mut queued) = (None, Vec::new(), 0u64);
        let pair_prefixes = [RECORD_FILE_UPLOADED_PREFIX, PENDING_UPLOAD_PREFIX];
        loop {
            let rows = page(&store, RECORD_FILE_PREFIX, after.take(), FILE_PAGE).await?;
            let (done, suffixes) = (
                rows.len() < FILE_PAGE,
                suffixes_of(&rows, RECORD_FILE_PREFIX),
            );
            after = rows.last().map(|(k, _)| k.clone());
            drop(rows);
            let keys = suffixes
                .iter()
                .flat_map(|s| pair_prefixes.map(|p| format!("{p}{s}").into_bytes()))
                .collect();
            for (s, pair) in suffixes.iter().zip(store.get_many(keys).await?.chunks(2)) {
                if pair.iter().all(Option::is_none) {
                    puts.push(KvMutation::put(
                        format!("{PENDING_UPLOAD_PREFIX}{s}"),
                        MARKER,
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

    pub async fn pending_blob_uploads(
        self: &Arc<Self>,
        database: &Id,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<BlobRow>> {
        let store = self.cas_kv();
        let (mut uploads, mut stale) = (Vec::new(), Vec::new());
        let mut after = after
            .map(|k| {
                k.strip_prefix(RECORD_FILE_PREFIX)
                    .map(|s| format!("{PENDING_UPLOAD_PREFIX}{s}").into_bytes())
                    .ok_or_else(|| invalid("upload cursor"))
            })
            .transpose()?;
        while uploads.len() < limit {
            let rows = page(&store, PENDING_UPLOAD_PREFIX, after.take(), MARKER_PAGE).await?;
            let (done, suffixes) = (
                rows.len() < MARKER_PAGE,
                suffixes_of(&rows, PENDING_UPLOAD_PREFIX),
            );
            after = rows.last().map(|(k, _)| k.clone());
            let req = suffixes
                .iter()
                .map(|s| format!("{RECORD_FILE_UPLOADED_PREFIX}{s}").into_bytes())
                .collect();
            for (s, up) in suffixes.iter().zip(store.get_many(req).await?) {
                if up.is_some() {
                    stale.push(KvMutation::delete(format!("{PENDING_UPLOAD_PREFIX}{s}")));
                } else if uploads.len() < limit {
                    uploads.push(blob_row(database, format!("{RECORD_FILE_PREFIX}{s}"))?);
                }
            }
            if done || after.is_none() {
                break;
            }
        }
        mutate_chunks(&store, &stale).await?;
        Ok(uploads)
    }

    pub async fn pending_blob_releases(
        self: &Arc<Self>,
        database: &Id,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<BlobRow>> {
        let after = match after {
            Some(k) if k.starts_with(RELEASED_FILE_PREFIX) => Some(k.as_bytes().to_vec()),
            Some(_) => return Err(invalid("release cursor")),
            None => None,
        };
        page(&self.cas_kv(), RELEASED_FILE_PREFIX, after, limit)
            .await?
            .into_iter()
            .map(|(k, _)| {
                let s = String::from_utf8(k).map_err(|_| invalid("record file release key"))?;
                blob_row(database, s)
            })
            .collect()
    }

    pub async fn record_blob_bytes(self: &Arc<Self>, key: &str) -> StorageResult<Option<Vec<u8>>> {
        if !key.starts_with(RECORD_FILE_PREFIX) {
            return Err(invalid("a blob upload reads a record file key"));
        }
        self.cas_kv().get(key.as_bytes()).await
    }

    pub async fn blobs_accepted(
        self: &Arc<Self>,
        uploaded: &[String],
        released: &[String],
    ) -> StorageResult<()> {
        let mut mutations = Vec::with_capacity(uploaded.len() * 2 + released.len());
        for key in uploaded {
            let s = key
                .strip_prefix(RECORD_FILE_PREFIX)
                .ok_or_else(|| invalid("uploaded file key"))?;
            mutations.extend([
                KvMutation::put(format!("{RECORD_FILE_UPLOADED_PREFIX}{s}"), MARKER.to_vec()),
                KvMutation::delete(format!("{PENDING_UPLOAD_PREFIX}{s}")),
            ]);
        }
        for key in released {
            if !key.starts_with(RELEASED_FILE_PREFIX) {
                return Err(invalid("released file key"));
            }
            mutations.push(KvMutation::delete(key.as_bytes().to_vec()));
        }
        mutate_chunks(&self.cas_kv(), &mutations).await
    }
}

async fn mutate_chunks(store: &PackKvStore, mutations: &[KvMutation]) -> StorageResult<()> {
    for chunk in mutations.chunks(WRITE_CHUNK) {
        store.batch_mutate(chunk.to_vec()).await?;
    }
    Ok(())
}

async fn page(
    store: &PackKvStore,
    prefix: &str,
    after: Option<Vec<u8>>,
    limit: usize,
) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut end = prefix.as_bytes().to_vec();
    if let Some(last) = end.last_mut() {
        *last = b';';
    }
    let start = after.map_or_else(|| prefix.as_bytes().to_vec(), |a| [a, vec![0]].concat());
    store.scan_range_paged(&start, &end, limit).await
}
