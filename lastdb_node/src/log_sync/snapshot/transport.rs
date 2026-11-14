//! Sign the snapshot inventory and transfer its original files directly.

use std::collections::BTreeMap;
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

use super::{files, Manifest};
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
    fn id(&self) -> Result<Digest, String> {
        canonical_bytes(self)
            .map(|body| bytes_sha256(&body))
            .map_err(|error| error.to_string())
    }
}

pub(super) async fn current(log: &CloudLog) -> Result<Option<(Digest, Manifest)>, String> {
    let body = canonical_bytes(&json!({"action": "read", "database_id": log.database_id()}))
        .map_err(|error| error.to_string())?;
    let (status, raw) = log
        .post_status_limit(ROUTE, body, Proof::Data, 512 * 1024)
        .await
        .map_err(|_| "snapshot lookup did not complete")?;
    if status == 404 {
        return Ok(None);
    }
    if status != 200 {
        return Err(format!("signed snapshot lookup returned HTTP {status}"));
    }
    let value: Read = serde_json::from_slice(&raw).map_err(|_| "invalid signed snapshot reply")?;
    if value.database_id.as_str() != log.database_id() {
        return Err("snapshot reply names a different database".into());
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
    previous: Option<&Digest>,
    cut: &DatabaseSnapshot,
    manifest: &Manifest,
) -> Result<Digest, String> {
    let id = manifest.id()?;
    if previous == Some(&id) {
        return Ok(id);
    }
    let client = client()?;
    for page in manifest.files.chunks(PAGE) {
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
            for (entry, grant) in entries.iter().zip(grants) {
                transfers.push(upload(&client, cut, entry, grant));
            }
            futures::future::try_join_all(transfers).await?;
        }
    }
    let value = call(
        log,
        json!({
            "action": "publish",
            "database_id": log.database_id(),
            "previous": previous,
            "snapshot": manifest,
        }),
    )
    .await?;
    if value.get("snapshot_id").and_then(serde_json::Value::as_str) != Some(id.as_str()) {
        return Err("snapshot commit returned a different manifest digest".into());
    }
    Ok(id)
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
    let client = client()?;
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

fn client() -> Result<Client, String> {
    Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(900))
        .build()
        .map_err(|_| "cannot build snapshot transfers".to_owned())
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
    request
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

async fn call(log: &CloudLog, body: serde_json::Value) -> Result<serde_json::Value, String> {
    let body = canonical_bytes(&body).map_err(|error| error.to_string())?;
    let (status, response) = log
        .post_status_limit(ROUTE, body, Proof::Data, 512 * 1024)
        .await
        .map_err(|_| "signed snapshot request did not complete")?;
    if !(200..300).contains(&status) {
        let protocol = serde_json::from_slice::<cloud_backup_protocol::ProtocolError>(&response)
            .ok()
            .map_or_else(
                || "unknown failure".to_owned(),
                |error| format!("{:?}", error.code),
            );
        return Err(format!(
            "signed snapshot request returned HTTP {status} {protocol}"
        ));
    }
    serde_json::from_slice(&response).map_err(|_| "invalid snapshot response".into())
}
