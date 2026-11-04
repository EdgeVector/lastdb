//! One injected definition assembles keys, nested encoding and write admission.
mod alternates;
mod keys;
mod records;

use super::Result;
use lastdb_model::whole_record::Record;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Serialize)]
pub(super) struct Definition {
    pub marker: &'static str,
    pub identity: &'static str,
    pub data: &'static str,
    pub records: &'static str,
    pub schemas: &'static str,
    pub alternates: &'static str,
    pub version: u32,
    pub max_key_bytes: usize,
    pub max_record_bytes: usize,
    #[serde(skip)]
    encode_payload: fn(Map<String, Value>) -> Result<Value>,
    #[serde(skip)]
    decode_payload: fn(Value) -> Result<Map<String, Value>>,
}

pub(super) const PLAIN: Definition = Definition {
    marker: "whole-record-spike.json",
    identity: "identity.key",
    data: "data",
    records: "records",
    schemas: "schemas",
    alternates: "alternates",
    version: 1,
    max_key_bytes: 4096,
    max_record_bytes: 64 * 1024,
    encode_payload: |value| Ok(Value::Object(value)),
    decode_payload: |value| match value {
        Value::Object(value) => Ok(value),
        _ => Err("plain payload must be an object".into()),
    },
};

pub(super) const NESTED: Definition = Definition {
    data: "nested-data",
    records: "nested-records",
    version: 2,
    encode_payload: |value| {
        Ok(serde_json::to_value(Payload {
            version: 1,
            values: value,
        })?)
    },
    decode_payload: |value| {
        let child: Payload = serde_json::from_value(value)?;
        if child.version != 1 {
            return Err("unsupported nested payload version".into());
        }
        Ok(child.values)
    },
    ..PLAIN
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    version: u32,
    values: Map<String, Value>,
}

pub(super) struct Address {
    collection: String,
    key: String,
}

impl Address {
    pub(super) fn into_parts(self) -> (String, String) {
        (self.collection, self.key)
    }
}

struct Operation {
    address: Address,
    body: Option<Vec<u8>>,
}

/// Only the format can mint operations; the owner can only consume the batch.
pub(super) struct Batch(Vec<Operation>);

impl Batch {
    pub(super) fn into_parts(self) -> impl Iterator<Item = (String, String, Option<Vec<u8>>)> {
        self.0.into_iter().map(|op| {
            let (collection, key) = op.address.into_parts();
            (collection, key, op.body)
        })
    }
}

impl Definition {
    pub(super) fn marker(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn admit(&self, operations: Vec<Operation>) -> Result<Batch> {
        let mut total = 0_usize;
        let mut seen = std::collections::HashSet::new();
        if operations.is_empty() || operations.len() > 17 {
            return Err("invalid mutation operation count".into());
        }
        for op in &operations {
            let body = op.body.as_ref().map_or(0, Vec::len);
            if op.address.key.len() > self.max_key_bytes || body > self.max_record_bytes {
                return Err("encoded key or record exceeds the format limit".into());
            }
            if !seen.insert((&op.address.collection, &op.address.key)) {
                return Err("duplicate mutation address".into());
            }
            total = total.saturating_add(op.address.collection.len() + op.address.key.len() + body);
        }
        if total > 256 * 1024 {
            return Err("encoded mutation exceeds the batch byte limit".into());
        }
        Ok(Batch(operations))
    }
}

#[derive(Serialize)]
pub(super) struct LookupRecord {
    pub key: lastdb_model::whole_record::Key,
    #[serde(flatten)]
    pub value: Record,
}
