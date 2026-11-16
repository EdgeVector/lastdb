//! Sign the snapshot inventory and transfer its original files directly.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use cloud_backup_protocol::v2::share_access::MUTATION_LOG_SNAPSHOT_BYTES_PATH as ROUTE;
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, Digest, Id};
use fold_db::storage::disk_pack::snapshot::DatabaseSnapshot;
use futures::TryStreamExt;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::{bootstrap as files, Manifest};
use crate::cloud_backup_client::build_shared_http_client;
use crate::log_sync::{cloud::CloudLog, http::Proof};

const PAGE: usize = 32;
const PARALLEL: usize = 6;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    url: String,
    headers: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    database_id: Id,
    snapshot_id: Option<Digest>,
    snapshot: Option<Manifest>,
}

impl Manifest {
    pub(in crate::log_sync) fn id(&self) -> Result<Digest, String> {
        canonical_bytes(self)
            .map(|body| bytes_sha256(&body))
            .map_err(|error| error.to_string())
    }
}

pub(super) async fn current(log: &CloudLog) -> Result<Option<(Digest, Manifest)>, String> {
    read(log, None, false).await
}

pub(in crate::log_sync) async fn exact(log: &CloudLog, id: &Digest) -> Result<Manifest, String> {
    let (_, manifest) = read(log, Some(id), false)
        .await?
        .ok_or("the requested signed snapshot is unavailable")?;
    Ok(manifest)
}

/// Reserve the chosen manifest before requesting its first provider page.
pub(super) async fn for_restore(
    log: &CloudLog,
    expected: Option<&Digest>,
) -> Result<Option<(Digest, Manifest)>, String> {
    read(log, expected, true).await
}

async fn read(
    log: &CloudLog,
    expected: Option<&Digest>,
    restore: bool,
) -> Result<Option<(Digest, Manifest)>, String> {
    let mut request = json!({"action": "read", "database_id": log.database_id()});
    if let Some(id) = expected {
        request["snapshot_id"] = json!(id);
    }
    if restore {
        request["action"] = json!("read-for-restore");
    }
    let (status, raw) = match raw_call(log, &request).await? {
        (400, body) if restore && super::super::wire::unsupported_reset(&body) => {
            request["action"] = json!("read");
            raw_call(log, &request).await?
        }
        reply => reply,
    };
    if status == 404 {
        return Ok(None);
    }
    if status != 200 {
        return Err(protocol_failure("signed snapshot lookup", status, &raw));
    }
    let value: Read = serde_json::from_slice(&raw).map_err(|_| "invalid signed snapshot reply")?;
    if value.database_id.as_str() != log.database_id() {
        return Err("snapshot reply names a different database".into());
    }
    if expected.is_some() && value.snapshot_id.as_ref() != expected {
        return Err("signed snapshot reply does not identify the requested snapshot".into());
    }
    match (value.snapshot_id, value.snapshot) {
        (None, None) => Ok(None),
        (Some(id), Some(manifest)) if manifest.id()? == id => {
            files::validate(&manifest.files)?;
            Ok(Some((id, manifest)))
        }
        _ => Err("signed snapshot manifest does not match its digest".into()),
    }
}

pub(super) async fn publish(
    log: &CloudLog,
    previous: Option<&(Digest, Manifest)>,
    cut: &DatabaseSnapshot,
    manifest: &Manifest,
    advance_cutoff: bool,
) -> Result<Digest, String> {
    let id = manifest.id()?;
    let previous_id = previous.map(|(saved_id, _)| saved_id);
    if previous_id != Some(&id) {
        let previous_files = previous.map_or(&[][..], |(_, saved)| saved.files.as_slice());
        upload_files(log, cut, manifest, previous_files).await?;
    } else if !advance_cutoff {
        return Ok(id);
    }
    let mut request = json!({
        "action": "publish",
        "database_id": log.database_id(),
        "previous": previous_id,
        "snapshot": manifest,
    });
    if advance_cutoff {
        request["advance_cutoff"] = json!(true);
    }
    let value = match raw_call(log, &request).await? {
        (400, body) if advance_cutoff && super::super::wire::unsupported_reset(&body) => {
            // This signed service accepts an ordinary backup but cannot publish a cutoff.
            request
                .as_object_mut()
                .ok_or("invalid snapshot publication")?
                .remove("advance_cutoff");
            call(log, request).await?
        }
        (status, body) => decode_reply(status, &body)?,
    };
    if value.get("snapshot_id").and_then(serde_json::Value::as_str) != Some(id.as_str()) {
        return Err("snapshot commit returned a different manifest digest".into());
    }
    Ok(id)
}

fn object_key(entry: &files::FileEntry) -> (u8, u64, &str) {
    let destination = match entry.destination {
        cloud_backup_protocol::v2::Destination::R2 => 0,
        cloud_backup_protocol::v2::Destination::B2 => 1,
    };
    (destination, entry.bytes.get(), entry.sha256.as_str())
}

async fn upload_files(
    log: &CloudLog,
    cut: &DatabaseSnapshot,
    manifest: &Manifest,
    previous_files: &[files::FileEntry],
) -> Result<(), String> {
    let mut covered: BTreeSet<(u8, u64, &str)> = previous_files.iter().map(object_key).collect();
    let mut pending = Vec::new();
    for entry in &manifest.files {
        if covered.insert(object_key(entry)) {
            pending.push(entry);
        }
    }
    drop(covered);
    if pending.is_empty() {
        return Ok(());
    }
    let client = build_shared_http_client().map_err(|_| "cannot build snapshot transfers")?;
    for page in pending.chunks(PAGE) {
        let grants = grants(
            log,
            json!({
                "action": "prepare", "database_id": log.database_id(),
                "upload_id": manifest.upload_id, "files": page,
            }),
            page.len(),
        )
        .await?;
        for (entries, grants) in page.chunks(PARALLEL).zip(grants.chunks(PARALLEL)) {
            let mut transfers = Vec::with_capacity(entries.len());
            for (&entry, grant) in entries.iter().zip(grants) {
                transfers.push(upload(&client, cut, entry, grant));
            }
            futures::future::try_join_all(transfers).await?;
        }
    }
    Ok(())
}

async fn upload(
    client: &Client,
    cut: &DatabaseSnapshot,
    entry: &files::FileEntry,
    grant: &Grant,
) -> Result<(), String> {
    let file = cut
        .open_file(Path::new(entry.path.as_str()))
        .map_err(|error| error.to_string())?;
    let body = reqwest::Body::wrap_stream(ReaderStream::new(tokio::fs::File::from_std(file)));
    let response = request(client.put(&grant.url), &grant.headers)
        .header(reqwest::header::CONTENT_LENGTH, entry.bytes.get())
        .body(body)
        .send()
        .await
        .map_err(|_| "snapshot upload did not complete")?;
    if response.status().is_success()
        || (entry.destination == cloud_backup_protocol::v2::Destination::R2
            && response.status() == StatusCode::PRECONDITION_FAILED)
    {
        return Ok(());
    }
    Err(format!(
        "snapshot file upload returned HTTP {}",
        response.status()
    ))
}

pub(super) async fn download(
    log: &CloudLog,
    id: &Digest,
    manifest: &Manifest,
    destination: &Path,
) -> Result<(), String> {
    let client = build_shared_http_client().map_err(|_| "cannot build snapshot transfers")?;
    files::create_layout(destination).map_err(|error| error.to_string())?;
    for (page, entries) in manifest.files.chunks(PAGE).enumerate() {
        let grants = grants(
            log,
            json!({
                "action": "download", "database_id": log.database_id(),
                "snapshot_id": id, "offset": page * PAGE,
            }),
            entries.len(),
        )
        .await?;
        for (entries, grants) in entries.chunks(PARALLEL).zip(grants.chunks(PARALLEL)) {
            let mut transfers = Vec::with_capacity(entries.len());
            for (entry, grant) in entries.iter().zip(grants) {
                transfers.push(receive(
                    &client,
                    destination.join(entry.path.as_str()),
                    entry,
                    grant,
                ));
            }
            futures::future::try_join_all(transfers).await?;
        }
    }
    files::sync_layout(destination).map_err(|error| error.to_string())
}

async fn receive(
    client: &Client,
    path: std::path::PathBuf,
    entry: &files::FileEntry,
    grant: &Grant,
) -> Result<(), String> {
    let response = request(client.get(&grant.url), &grant.headers)
        .send()
        .await
        .map_err(|_| "snapshot download did not complete")?;
    if !response.status().is_success() || response.content_length() != Some(entry.bytes.get()) {
        return Err(format!(
            "snapshot file download returned HTTP {} or a different length",
            response.status()
        ));
    }
    let mut input = response.bytes_stream();
    let mut output =
        tokio::fs::File::from_std(files::create_file(&path).map_err(|error| error.to_string())?);
    let mut hash = Sha256::new();
    let mut length = 0;
    while let Some(chunk) = input
        .try_next()
        .await
        .map_err(|_| "snapshot download stopped")?
    {
        length += chunk.len() as u64;
        if length > entry.bytes.get() {
            return Err("snapshot download exceeded its saved length".into());
        }
        hash.update(&chunk);
        output
            .write_all(&chunk)
            .await
            .map_err(|error| error.to_string())?;
    }
    if length != entry.bytes.get() || files::digest(hash)? != entry.sha256 {
        return Err("snapshot download differs from its saved digest or length".into());
    }
    output.sync_all().await.map_err(|error| error.to_string())
}

fn request(
    mut request: reqwest::RequestBuilder,
    headers: &BTreeMap<String, String>,
) -> reqwest::RequestBuilder {
    for (name, value) in headers {
        if !name.eq_ignore_ascii_case("content-length") {
            request = request.header(name, value);
        }
    }
    request.timeout(Duration::from_secs(900))
}

async fn grants(
    log: &CloudLog,
    body: serde_json::Value,
    expected: usize,
) -> Result<Vec<Grant>, String> {
    let value = call(log, body).await?;
    let grants: Vec<Grant> =
        serde_json::from_value(value).map_err(|_| "invalid snapshot transfer grants")?;
    if grants.len() != expected {
        return Err("snapshot transfer grants do not cover every file".into());
    }
    Ok(grants)
}

pub(in crate::log_sync) async fn call(
    log: &CloudLog,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let (status, response) = raw_call(log, &body).await?;
    decode_reply(status, &response)
}

pub(in crate::log_sync) async fn call_if_supported(
    log: &CloudLog,
    body: serde_json::Value,
) -> Result<Option<serde_json::Value>, String> {
    let (status, response) = raw_call(log, &body).await?;
    if status == 400 && super::super::wire::unsupported_reset(&response) {
        return Ok(None);
    }
    decode_reply(status, &response).map(Some)
}

async fn raw_call(log: &CloudLog, body: &serde_json::Value) -> Result<(u16, Vec<u8>), String> {
    let body = canonical_bytes(body).map_err(|error| error.to_string())?;
    log.post_status_limit(ROUTE, body, Proof::Data, 512 * 1024)
        .await
        .map_err(|_| "signed snapshot request did not complete".to_owned())
}

fn decode_reply(status: u16, response: &[u8]) -> Result<serde_json::Value, String> {
    if !(200..300).contains(&status) {
        return Err(protocol_failure(
            "signed snapshot request",
            status,
            response,
        ));
    }
    serde_json::from_slice(response).map_err(|_| "invalid snapshot response".into())
}

fn protocol_failure(action: &str, status: u16, response: &[u8]) -> String {
    if crate::cloud::is_payment_subscription_error(status, response) {
        return crate::cloud::PAYMENT_REQUIRED_MESSAGE.to_owned();
    }
    let detail = serde_json::from_slice::<cloud_backup_protocol::ProtocolError>(response)
        .ok()
        .map_or_else(
            || "unknown failure".to_owned(),
            |error| format!("{:?} ({})", error.code, error.field),
        );
    format!("{action} returned HTTP {status} {detail}")
}
