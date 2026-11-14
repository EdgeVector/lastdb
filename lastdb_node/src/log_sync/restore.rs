//! Fetch authenticated file ciphertext ranges for log replay.

use async_trait::async_trait;
use base64::Engine as _;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{canonical_bytes, CloudScope, DecimalU64};
use serde::{Deserialize, Serialize};

use crate::database_replay::{RestoreError, SnapshotFileRequest, SnapshotFileSource};

use super::cloud::CloudLog;
use super::paths;

const OBJECT_RANGE: u64 = 180 * 1024;

pub(super) struct Files {
    log: CloudLog,
}

pub(super) fn file_source(log: CloudLog) -> Files {
    Files { log }
}

#[async_trait]
impl SnapshotFileSource for Files {
    async fn read_blob(&mut self, request: &SnapshotFileRequest) -> Result<Vec<u8>, RestoreError> {
        if request.database_id != self.log.database_id() {
            return Err(RestoreError::File("snapshot file"));
        }
        let mut offset = 0u64;
        let mut buffer = Vec::new();
        let mut total = None;
        loop {
            let page = one_range(&self.log, request, offset, total).await?;
            if page.got == 0 || page.start != offset {
                return Err(RestoreError::File("snapshot file"));
            }
            let size = *total.get_or_insert(page.total);
            if size != page.total || offset.saturating_add(page.got) > size {
                return Err(RestoreError::File("snapshot file"));
            }
            buffer.extend_from_slice(&page.bytes);
            offset += page.got;
            if offset == size {
                return Ok(buffer);
            }
        }
    }
}

struct RangeBytes {
    total: u64,
    start: u64,
    got: u64,
    bytes: Vec<u8>,
}

async fn one_range(
    log: &CloudLog,
    request: &SnapshotFileRequest,
    offset: u64,
    total: Option<u64>,
) -> Result<RangeBytes, RestoreError> {
    let length = total.map(|size| size.saturating_sub(offset).min(OBJECT_RANGE));
    let body =
        object_body(request, offset, length).map_err(|_| RestoreError::File("snapshot file"))?;
    let raw = log
        .post(paths::DELIVERY_OBJECT, body)
        .await
        .map_err(|_| RestoreError::File("snapshot file"))?;
    decode_range(&raw, &request.database_id)
}

fn object_body(
    request: &SnapshotFileRequest,
    offset: u64,
    length: Option<u64>,
) -> Result<Vec<u8>, ()> {
    canonical_bytes(&ObjectRequest {
        database_id: &request.database_id,
        scope: &request.scope,
        path: &request.path,
        offset: DecimalU64::new(offset),
        length: length.map(DecimalU64::new),
    })
    .map_err(|_| ())
}

fn decode_range(raw: &[u8], database_id: &str) -> Result<RangeBytes, RestoreError> {
    let page: ObjectBody =
        serde_json::from_slice(raw).map_err(|_| RestoreError::File("snapshot file"))?;
    if page.database_id != database_id || page.result != "bytes" {
        return Err(RestoreError::File("snapshot file"));
    }
    let total = page.bytes.ok_or(RestoreError::File("snapshot file"))?.get();
    let start = page
        .offset
        .ok_or(RestoreError::File("snapshot file"))?
        .get();
    let got = page
        .length
        .ok_or(RestoreError::File("snapshot file"))?
        .get();
    let text = page.ciphertext.ok_or(RestoreError::File("snapshot file"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| RestoreError::File("snapshot file"))?;
    if got == 0 || got > OBJECT_RANGE || bytes.len() as u64 != got {
        return Err(RestoreError::File("snapshot file"));
    }
    Ok(RangeBytes {
        total,
        start,
        got,
        bytes,
    })
}

#[derive(Serialize)]
struct ObjectRequest<'a> {
    database_id: &'a str,
    scope: &'a CloudScope,
    path: &'a BlobPath,
    offset: DecimalU64,
    #[serde(skip_serializing_if = "Option::is_none")]
    length: Option<DecimalU64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectBody {
    database_id: String,
    result: String,
    #[serde(default)]
    bytes: Option<DecimalU64>,
    #[serde(default)]
    offset: Option<DecimalU64>,
    #[serde(default)]
    length: Option<DecimalU64>,
    #[serde(default)]
    ciphertext: Option<String>,
}
