//! The single definition of the spike's home and record representation.
use super::Result;
use lastdb_model::record_replacement::RecordValues;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Serialize)]
pub(super) struct HomeFormat {
    pub marker: &'static str,
    pub identity: &'static str,
    pub data: &'static str,
    pub collection: &'static str,
    pub record_prefix: &'static str,
    pub version: u32,
    pub max_record_bytes: usize,
    pub max_key_bytes: usize,
}

pub(super) const HOME: HomeFormat = HomeFormat {
    marker: "record-format-spike.json",
    identity: "identity.key",
    data: "data",
    collection: "records",
    record_prefix: "record:",
    version: 1,
    max_record_bytes: 64 * 1024,
    max_key_bytes: 4096,
};

/// One wire shape, shared by encoding and decoding.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecord {
    version: u32,
    fields: Map<String, Value>,
}

/// Only the configured encoder can construct this complete output.
pub(super) struct EncodedRecord {
    key: String,
    body: Vec<u8>,
}

impl EncodedRecord {
    pub(super) fn into_parts(self) -> (String, Vec<u8>) {
        (self.key, self.body)
    }
}

pub(super) fn marker() -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&HOME)?)
}

pub(super) fn key(kind: &str, key: &str) -> Result<String> {
    if kind.is_empty() || key.is_empty() {
        return Err("record kind and key must be nonempty".into());
    }
    // JSON tuple framing keeps separators and Unicode in either component safe.
    let framed = serde_json::to_string(&(kind, key))?;
    let encoded = format!("{}{framed}", HOME.record_prefix);
    if encoded.len() > HOME.max_key_bytes {
        return Err("encoded key exceeds the configured limit".into());
    }
    Ok(encoded)
}

/// The top assembles ordinary functions; their types are resolved at compile time.
pub(super) fn encode<R: RecordValues>(values: &R) -> Result<EncodedRecord> {
    encode_with(values, record_key, record_body)
}

fn encode_with<R: RecordValues>(
    values: &R,
    encode_key: impl FnOnce(&R) -> Result<String>,
    encode_value: impl FnOnce(&R) -> Result<Vec<u8>>,
) -> Result<EncodedRecord> {
    let key = encode_key(values)?;
    let body = encode_value(values)?;
    if body.len() > HOME.max_record_bytes {
        return Err("encoded record exceeds the configured limit".into());
    }
    Ok(EncodedRecord { key, body })
}

fn record_key(values: &impl RecordValues) -> Result<String> {
    key(values.kind(), values.key())
}

fn record_body(values: &impl RecordValues) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&StoredRecord {
        version: HOME.version,
        fields: values.fields().clone(),
    })?)
}

pub(super) fn decode(bytes: &[u8]) -> Result<Map<String, Value>> {
    let record: StoredRecord = serde_json::from_slice(bytes)?;
    if record.version != HOME.version {
        return Err("record version differs from the selected format".into());
    }
    Ok(record.fields)
}
