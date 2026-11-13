//! Fetch and install one signed snapshot chain by database id.
//!
//! The tip request omits `snapshot_ref`. Each ancestor and each part has
//! its own request. File objects are fetched as authenticated ciphertext.
//! The sync loop acknowledges the cursor after the installed prefix is durable.

use async_trait::async_trait;
use base64::Engine as _;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{canonical_bytes, CloudScope, DecimalU64};
use mutation_log::{LogError, MutationLog, Recovery};
use serde::{Deserialize, Serialize};

use crate::database_replay::{
    self, RestoreError, SnapshotFileRequest, SnapshotFileSource, SnapshotInstall, SnapshotManifest,
    SnapshotObject, SnapshotPart,
};

use super::cloud::CloudLog;
use super::paths;

const OBJECT_RANGE: u64 = 180 * 1024;
/// Same bound as `database_replay` chain order.
const CHAIN_LIMIT: usize = 1024;

pub(super) fn recover(log: &CloudLog, reader: &mutation_log::Writer) -> Result<Recovery, LogError> {
    if log.principal() != &reader.key || log.device() != &reader.device {
        return Err(LogError::Unauthorized);
    }
    let through = log.wait(install_for_sync(log.clone()))??;
    match log.list(&reader.key, through, mutation_log::LIST_PAGE_LIMIT, 0) {
        Ok(page) => Ok(Recovery {
            reset: None,
            entries: page.entries,
            next: page.next,
        }),
        Err(LogError::CursorExpired { .. }) => Err(LogError::SnapshotRequired),
        Err(error) => Err(error),
    }
}

async fn install_for_sync(log: CloudLog) -> Result<u64, LogError> {
    install_with_mode(log, true).await
}

pub(super) async fn install(log: CloudLog) -> Result<u64, LogError> {
    install_with_mode(log, false).await
}

async fn install_with_mode(log: CloudLog, live: bool) -> Result<u64, LogError> {
    let chain = walk(&log).await?;
    let tip = chain
        .first()
        .ok_or(LogError::UnexpectedOutcome)?
        .snapshot_ref
        .clone();
    let request = SnapshotInstall {
        database_id: log.database_id().to_owned(),
        snapshot_ref: tip,
        objects: objects(&log, &chain).await?,
    };
    let mut files = Files { log: log.clone() };
    let device = live.then(|| log.device().as_str());
    match database_replay::install(log.host(), &request, &mut files, device).await {
        Ok(through) => Ok(through),
        Err(RestoreError::Scope(_)) => Err(LogError::SnapshotRequired),
        Err(_) => Err(LogError::UnexpectedOutcome),
    }
}

/// First sealed entry of a trimmed log. The caller uses it to select the content key.
pub(super) async fn sample_entry(
    log: &CloudLog,
) -> Result<database_replay::SnapshotEntry, LogError> {
    let manifest = one_manifest(log, None).await?;
    if manifest.parts == 0 {
        return Err(LogError::UnexpectedOutcome);
    }
    let loaded = part(log, &manifest, 0).await?;
    loaded
        .entries
        .into_iter()
        .next()
        .ok_or(LogError::UnexpectedOutcome)
}

async fn walk(log: &CloudLog) -> Result<Vec<SnapshotManifest>, LogError> {
    let mut chain = Vec::new();
    let mut next: Option<String> = None;
    loop {
        if chain.len() >= CHAIN_LIMIT {
            return Err(LogError::UnexpectedOutcome);
        }
        let manifest = one_manifest(log, next.as_deref()).await?;
        if chain
            .iter()
            .any(|seen: &SnapshotManifest| seen.snapshot_ref == manifest.snapshot_ref)
        {
            return Err(LogError::UnexpectedOutcome);
        }
        let previous = manifest.previous_ref.clone();
        chain.push(manifest);
        match previous {
            Some(reference) if !reference.is_empty() => next = Some(reference),
            _ => return Ok(chain),
        }
    }
}

async fn one_manifest(
    log: &CloudLog,
    snapshot_ref: Option<&str>,
) -> Result<SnapshotManifest, LogError> {
    let body = canonical_bytes(&ManifestRequest {
        database_id: log.database_id(),
        snapshot_ref,
    })
    .map_err(|_| LogError::UnexpectedOutcome)?;
    let raw = log
        .post(paths::DELIVERY_SNAPSHOT, body)
        .await
        .map_err(|_| LogError::UnexpectedOutcome)?;
    let manifest =
        database_replay::manifest_from_json(&raw).map_err(|_| LogError::UnexpectedOutcome)?;
    if snapshot_ref.is_some_and(|wanted| manifest.snapshot_ref != wanted) {
        return Err(LogError::UnexpectedOutcome);
    }
    Ok(manifest)
}

async fn objects(
    log: &CloudLog,
    chain: &[SnapshotManifest],
) -> Result<Vec<SnapshotObject>, LogError> {
    let mut objects = Vec::with_capacity(chain.len());
    for manifest in chain {
        objects.push(SnapshotObject {
            parts: parts(log, manifest).await?,
            manifest: manifest.clone(),
        });
    }
    Ok(objects)
}

async fn parts(log: &CloudLog, manifest: &SnapshotManifest) -> Result<Vec<SnapshotPart>, LogError> {
    let mut parts = Vec::with_capacity(manifest.parts as usize);
    for index in 0..manifest.parts {
        parts.push(part(log, manifest, index).await?);
    }
    Ok(parts)
}

async fn part(
    log: &CloudLog,
    manifest: &SnapshotManifest,
    index: u32,
) -> Result<SnapshotPart, LogError> {
    let body = canonical_bytes(&PartRequest {
        database_id: log.database_id(),
        snapshot_ref: &manifest.snapshot_ref,
        part: DecimalU64::new(u64::from(index)),
    })
    .map_err(|_| LogError::UnexpectedOutcome)?;
    let raw = log
        .post(paths::DELIVERY_SNAPSHOT, body)
        .await
        .map_err(|_| LogError::UnexpectedOutcome)?;
    let part = database_replay::part_from_json(&raw).map_err(|_| LogError::UnexpectedOutcome)?;
    if part.database_id != log.database_id()
        || part.snapshot_ref != manifest.snapshot_ref
        || part.index != index
    {
        return Err(LogError::UnexpectedOutcome);
    }
    Ok(part)
}

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
struct ManifestRequest<'a> {
    database_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_ref: Option<&'a str>,
}

#[derive(Serialize)]
struct PartRequest<'a> {
    database_id: &'a str,
    snapshot_ref: &'a str,
    part: DecimalU64,
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
