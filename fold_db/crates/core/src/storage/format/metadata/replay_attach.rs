//! Bounded inline pins, local-file paths, and the schema index of saved baselines.

use super::replay::{record_key, ReplayStamp};
use super::{read_err, Metadata};
use crate::schema::SchemaError;
use crate::storage::current_keys::families::record_file_path;
use crate::storage::disk_pack::transaction;
use crate::storage::format::replay_pin;
use lastdb_model::hex::sha256_hex;
use lastdb_model::kind_partition::exact_prefix_bounds;
use lastdb_model::portable_mutation::Position;
use lastdb_model::record_value::{MAX_INLINE_STORED_BYTES, MAX_RECORD_BYTES};
use serde::Deserialize;
use std::sync::Arc;

pub(super) const INLINE_PREFIX: &str = "signed-replay-baseline-inline:";
pub(super) const FILE_REF_PREFIX: &str = "signed-replay-baseline-file-ref:";
pub(super) const INDEX_PREFIX: &str = "signed-replay-baseline-index:";

const MAX_BASELINE_BYTES: usize = MAX_RECORD_BYTES + 2048;
const INDEX_MAX: usize = laststore::disk_run::MAX_RUN_KEY_BYTES;
const CAPTURE_FIELDS: usize = 6;
const PAGE: usize = 64;

pub(crate) struct ReplayCapture {
    pub pending: bool,
    pub pending_attachment: bool,
    pub winner: Option<ReplayStamp>,
    pub baseline: Option<Vec<u8>>,
    pub inline_pin: Option<Vec<u8>>,
    pub file_ref: Option<Vec<u8>>,
}

pub(crate) fn inline_key(addr: &str) -> String {
    format!("{INLINE_PREFIX}{}", sha256_hex(addr))
}

pub(crate) fn file_ref_key(addr: &str) -> String {
    format!("{FILE_REF_PREFIX}{}", sha256_hex(addr))
}

pub(crate) fn index_key(schema: &str, addr: &str) -> String {
    format!("{INDEX_PREFIX}{}:{}", sha256_hex(schema), sha256_hex(addr))
}

fn index_prefix(schema: &str) -> String {
    format!("{INDEX_PREFIX}{}:", sha256_hex(schema))
}

impl Metadata {
    pub(crate) async fn read_replay_captures(
        &self,
        addresses: &[&str],
    ) -> Result<Vec<ReplayCapture>, SchemaError> {
        self.require_replay_scope()?;
        let mut out = Vec::with_capacity(addresses.len());
        for chunk in addresses.chunks(1000) {
            let keys: Vec<Vec<u8>> = chunk
                .iter()
                .flat_map(|addr| capture_keys(addr).map(String::into_bytes))
                .collect();
            let rows = self.store.get_many(keys).await.map_err(read_err)?;
            if rows.len() != chunk.len() * CAPTURE_FIELDS {
                return Err(SchemaError::InvalidData(
                    "incomplete replay baseline read".into(),
                ));
            }
            for group in rows.chunks(CAPTURE_FIELDS) {
                out.push(decode_capture(group)?);
            }
        }
        Ok(out)
    }

    pub(crate) async fn stage_replay_rows(
        &self,
        rows: Vec<(String, Option<Vec<u8>>)>,
    ) -> Result<(), SchemaError> {
        for (key, value) in &rows {
            if let Some(bytes) = value {
                if bytes.is_empty() || bytes.len() > row_max(key) {
                    return Err(SchemaError::InvalidData(
                        "replay attachment row exceeds its bound".into(),
                    ));
                }
            }
        }
        self.stage(rows).await.map_err(read_err)
    }

    /// Drop saved baseline pins for one schema, including records already removed from `rk`.
    pub(crate) async fn release_schema_replay_pins(&self, schema: &str) -> Result<(), SchemaError> {
        let prefix = index_prefix(schema);
        self.release_indexed_pins(&prefix, schema).await
    }

    async fn release_indexed_pins(&self, prefix: &str, schema: &str) -> Result<(), SchemaError> {
        let (lower, upper) = exact_prefix_bounds(prefix);
        let mut start = lower.into_bytes();
        loop {
            let scan = self
                .store
                .scan_range_yielding(&start, upper.as_bytes(), PAGE, 1)
                .await
                .map_err(read_err)?;
            if !scan.rows.is_empty() {
                let runtime = Arc::clone(&self.runtime);
                transaction::scope(runtime, self.release_pin_page(schema, scan.rows)).await?;
            }
            let Some(mut previous) = scan.resume else {
                break;
            };
            previous.push(0);
            if previous <= start || previous.as_slice() >= upper.as_bytes() {
                return Err(SchemaError::StorageRead {
                    detail: "invalid replay index continuation".into(),
                });
            }
            start = previous;
        }
        Ok(())
    }

    async fn release_pin_page(
        &self,
        schema: &str,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), SchemaError> {
        let parsed = parse_index_page(schema, &rows)?;
        let keys: Vec<Vec<u8>> = parsed
            .iter()
            .map(|addr| record_key(addr, "baseline").into_bytes())
            .collect();
        let baselines = self.store.get_many(keys).await.map_err(read_err)?;
        if baselines.len() != parsed.len() {
            return Err(SchemaError::InvalidData(
                "incomplete replay baseline read".into(),
            ));
        }
        let mut deletes = Vec::new();
        let mut paths = Vec::new();
        for ((addr, baseline), (index, _)) in parsed.iter().zip(baselines).zip(rows) {
            deletes.push((String::from_utf8(index).map_err(read_err)?, None));
            deletes.extend(cleared_rows(addr));
            if let Some(bytes) = baseline.as_deref() {
                if let Some(path) = pinned_path(addr, bytes)? {
                    paths.push(path);
                }
            }
        }
        self.stage(deletes).await.map_err(read_err)?;
        replay_pin::retire_files(&self.runtime, paths)
    }
}

fn capture_keys(addr: &str) -> [String; CAPTURE_FIELDS] {
    [
        record_key(addr, "pending"),
        record_key(addr, "winner"),
        record_key(addr, "baseline"),
        record_key(addr, "pending-attachment"),
        inline_key(addr),
        file_ref_key(addr),
    ]
}

fn decode_capture(group: &[Option<Vec<u8>>]) -> Result<ReplayCapture, SchemaError> {
    let winner = group[1]
        .as_deref()
        .map(|bytes| serde_json::from_slice(bytes).map_err(read_err))
        .transpose()?;
    Ok(ReplayCapture {
        pending: group[0].is_some(),
        pending_attachment: group[3].is_some(),
        winner,
        baseline: bounded(group[2].clone(), MAX_BASELINE_BYTES)?,
        inline_pin: bounded(group[4].clone(), MAX_INLINE_STORED_BYTES)?,
        file_ref: bounded(group[5].clone(), replay_pin::FILE_REF_MAX)?,
    })
}

fn bounded(bytes: Option<Vec<u8>>, max: usize) -> Result<Option<Vec<u8>>, SchemaError> {
    if bytes
        .as_ref()
        .is_some_and(|row| row.is_empty() || row.len() > max)
    {
        return Err(SchemaError::InvalidData(
            "replay attachment row exceeds its bound".into(),
        ));
    }
    Ok(bytes)
}

fn row_max(key: &str) -> usize {
    if key.starts_with(INLINE_PREFIX) {
        MAX_INLINE_STORED_BYTES
    } else if key.starts_with(FILE_REF_PREFIX) {
        replay_pin::FILE_REF_MAX
    } else if key.starts_with("signed-replay-baseline:") {
        MAX_BASELINE_BYTES
    } else {
        INDEX_MAX
    }
}

fn cleared_rows(addr: &str) -> [(String, Option<Vec<u8>>); 6] {
    [
        (inline_key(addr), None),
        (file_ref_key(addr), None),
        (record_key(addr, "baseline"), None),
        (record_key(addr, "pending"), None),
        (record_key(addr, "pending-attachment"), None),
        (record_key(addr, "deferred"), None),
    ]
}

fn parse_index_page(schema: &str, rows: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<String>, SchemaError> {
    let mut addresses = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let key = std::str::from_utf8(key).map_err(read_err)?;
        let addr = std::str::from_utf8(value).map_err(read_err)?;
        if addr.len() > INDEX_MAX || key != index_key(schema, addr) {
            return Err(SchemaError::InvalidData(
                "replay baseline index row does not match its address".into(),
            ));
        }
        addresses.push(addr.to_owned());
    }
    Ok(addresses)
}

#[derive(Deserialize)]
struct BaselinePins {
    record: Option<PinRecord>,
}

#[derive(Deserialize)]
struct PinRecord {
    #[serde(default)]
    file_version: Option<Position>,
}

fn pinned_path(addr: &str, bytes: &[u8]) -> Result<Option<String>, SchemaError> {
    if bytes.len() > MAX_BASELINE_BYTES {
        return Err(read_err("replay baseline exceeds its bound"));
    }
    let saved: BaselinePins = serde_json::from_slice(bytes).map_err(read_err)?;
    let Some(version) = saved.record.and_then(|record| record.file_version) else {
        return Ok(None);
    };
    record_file_path(addr, &version).map(Some).map_err(read_err)
}
