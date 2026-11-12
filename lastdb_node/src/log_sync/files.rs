//! Address one sealed file by its accepted write.
//!
//! The path is the database id, the schema namespace, the SHA-256 of the
//! canonical record key, and the mutation id. This module does not scan
//! backup rows, open a backup job, or call a provider. `delivery/object`
//! is a ranged read of that path, using the account scope on the open log.
//! Bytes that are not already there stop the page. The authenticated upload
//! onto this same path is the file seam owned outside this module. The log
//! payload does not carry the bytes.

use base64::Engine as _;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{canonical_bytes, CloudScope, DecimalU64, Digest, Id};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use super::cloud::CloudLog;
use super::paths;

const OBJECT_RANGE: u64 = 180 * 1024;

pub(super) enum ObjectState {
    Present,
    Absent,
}

/// Stable object path for one accepted file write.
pub(super) fn path_for(
    database_id: &str,
    mutation_id: &str,
    canonical: &[u8],
) -> Result<BlobPath, ()> {
    let value: serde_json::Value = serde_json::from_slice(canonical).map_err(|_| ())?;
    if value.get("mutation_id").and_then(|item| item.as_str()) != Some(mutation_id) {
        return Err(());
    }
    let schema = value
        .get("schema")
        .and_then(|item| item.as_str())
        .ok_or(())?;
    let space = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
    let key = value.get("key").ok_or(())?;
    let key_bytes = canonical_bytes(key).map_err(|_| ())?;
    let record = fold_db::hex::hex_lower(Sha256::digest(key_bytes));
    let path = BlobPath {
        database: Id::try_from(database_id.to_owned()).map_err(|_| ())?,
        space: space.to_owned().try_into().map_err(|_| ())?,
        record: Digest::try_from(record).map_err(|_| ())?,
        write_id: mutation_id.to_owned().try_into().map_err(|_| ())?,
    };
    path.validate().map_err(|_| ())?;
    if path.database.as_str() != database_id {
        return Err(());
    }
    Ok(path)
}

pub(super) async fn confirm(
    log: &CloudLog,
    path: &BlobPath,
    ciphertext: &[u8],
) -> Result<ObjectState, ()> {
    if path.database.as_str() != log.database_id() {
        return Err(());
    }
    read_object(log, path, ciphertext).await
}

async fn read_object(
    log: &CloudLog,
    path: &BlobPath,
    ciphertext: &[u8],
) -> Result<ObjectState, ()> {
    let mut offset = 0u64;
    let mut matched = Vec::new();
    loop {
        let Some(page) = one_range(log, path, offset).await? else {
            return Ok(ObjectState::Absent);
        };
        if page.start != offset || page.got == 0 {
            return Err(());
        }
        matched.extend_from_slice(&page.bytes);
        offset += page.got;
        if offset == page.total {
            return Ok(if matched == ciphertext {
                ObjectState::Present
            } else {
                ObjectState::Absent
            });
        }
        if offset > page.total || matched.len() > ciphertext.len() {
            return Err(());
        }
    }
}

async fn one_range(log: &CloudLog, path: &BlobPath, offset: u64) -> Result<Option<RangeBytes>, ()> {
    let body = canonical_bytes(&ObjectRequest {
        database_id: log.database_id(),
        scope: &log.scope,
        path,
        offset: DecimalU64::new(offset),
        length: Some(DecimalU64::new(OBJECT_RANGE)),
    })
    .map_err(|_| ())?;
    let raw = log
        .post_named(paths::DELIVERY_OBJECT, body)
        .await
        .map_err(|_| ())?;
    decode_range(&raw, log.database_id())
}

fn decode_range(raw: &[u8], database_id: &str) -> Result<Option<RangeBytes>, ()> {
    let page: ObjectBody = serde_json::from_slice(raw).map_err(|_| ())?;
    if page.database_id != database_id {
        return Err(());
    }
    if page.result == "absent" {
        return Ok(None);
    }
    if page.result != "bytes" {
        return Err(());
    }
    let total = page.bytes.ok_or(())?.get();
    let start = page.offset.ok_or(())?.get();
    let got = page.length.ok_or(())?.get();
    let text = page.ciphertext.ok_or(())?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| ())?;
    if got == 0 || got > OBJECT_RANGE || bytes.len() as u64 != got {
        return Err(());
    }
    Ok(Some(RangeBytes {
        total,
        start,
        got,
        bytes,
    }))
}

struct RangeBytes {
    total: u64,
    start: u64,
    got: u64,
    bytes: Vec<u8>,
}

#[derive(serde::Serialize)]
struct ObjectRequest<'a> {
    database_id: &'a str,
    scope: &'a CloudScope,
    path: &'a BlobPath,
    offset: DecimalU64,
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
