//! Fetch one signed snapshot chain and install it.
//!
//! `delivery/snapshot` returns one manifest. The tip request omits
//! `snapshot_ref`. Each ancestor is its own request. A part request sends
//! `snapshot_ref` and `part`. `delivery/object` returns standard-base64
//! ciphertext ranges. `named_snapshot_restore::install` writes the sibling
//! store and does not acknowledge the cloud cursor. After that install, an
//! owned database records snapshot coverage. A personal database does not
//! call these routes. This module does not edit `registry.json`.

use async_trait::async_trait;
use base64::Engine as _;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{canonical_bytes, CloudScope, DecimalU64};
use mutation_log::{LogError, MutationLog, Recovery};
use serde::{Deserialize, Serialize};

use crate::accepted_writes;
use crate::host::owned::Resolved;
use crate::named_snapshot_restore::{
    self, RestoreError, SnapshotFileRequest, SnapshotFileSource, SnapshotInstall, SnapshotManifest,
    SnapshotObject, SnapshotPart,
};

use super::cloud::CloudLog;
use super::paths;

const OBJECT_RANGE: u64 = 180 * 1024;
/// Same bound as `named_snapshot_restore` chain order.
const CHAIN_LIMIT: usize = 1024;

pub(super) fn recover(log: &CloudLog, reader: &mutation_log::Writer) -> Result<Recovery, LogError> {
    if !log.named() {
        return Err(LogError::SnapshotRequired);
    }
    if log.principal() != &reader.key || log.device() != &reader.device {
        return Err(LogError::Unauthorized);
    }
    let through = log.wait(install(log.clone()))??;
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

async fn install(log: CloudLog) -> Result<u64, LogError> {
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
    match named_snapshot_restore::install(log.host(), &request, &mut files).await {
        Ok(prefix) => {
            note_owned(&log).await;
            Ok(prefix.through_delivery_seq)
        }
        Err(RestoreError::Scope(_)) => Err(LogError::SnapshotRequired),
        Err(_) => Err(LogError::UnexpectedOutcome),
    }
}

async fn note_owned(log: &CloudLog) {
    let Ok((Resolved::Owned, Some(store))) = log.host().owned.resolve(log.database_id()) else {
        return;
    };
    let runtime = store.db_ops().native_runtime();
    let noted = tokio::task::spawn_blocking(move || {
        let status = accepted_writes::status(&runtime);
        accepted_writes::note_snapshot(&runtime, status.opened_frontier)
    })
    .await;
    if !matches!(noted, Ok(Ok(()))) {
        tracing::warn!(target: "lastdbd::log_sync", "accepted snapshot coverage was not recorded");
    }
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
        .post_named(paths::DELIVERY_SNAPSHOT, body)
        .await
        .map_err(|_| LogError::UnexpectedOutcome)?;
    let manifest = named_snapshot_restore::manifest_from_json(&raw)
        .map_err(|_| LogError::UnexpectedOutcome)?;
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
        .post_named(paths::DELIVERY_SNAPSHOT, body)
        .await
        .map_err(|_| LogError::UnexpectedOutcome)?;
    let part =
        named_snapshot_restore::part_from_json(&raw).map_err(|_| LogError::UnexpectedOutcome)?;
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
        .post_named(paths::DELIVERY_OBJECT, body)
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
