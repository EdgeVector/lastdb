//! Injected format contract. Only child definitions can construct encoded output.
mod alternate;
mod fields;
mod records;

pub(crate) use records::{NESTED, PLAIN};

use super::Result;
use lastdb_model::record_replacement::RecordValues;
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Clone, Copy, Serialize)]
pub(crate) struct HomeLayout {
    pub marker: &'static str,
    pub identity: &'static str,
    pub data: &'static str,
    pub max_record_bytes: usize,
}

/// The writer knows this contract, never a concrete definition or codec function.
pub(crate) trait Format {
    fn layout(&self) -> &HomeLayout;
    fn marker(&self) -> Result<Vec<u8>>;
    fn address(&self, kind: &str, key: &str) -> Result<EncodedAddress>;
    fn encode(&self, values: &impl RecordValues, previous: Option<&[u8]>) -> Result<EncodedBatch>;
    fn decode(&self, bytes: &[u8]) -> Result<Map<String, Value>>;
    fn alternate_range(&self, kind: &str, value: &str) -> Result<EncodedRange>;
    fn decode_pointer(&self, kind: &str, value: &str, key: &str, body: &[u8]) -> Result<String>;
    fn decode_lookup(&self, value: &str, body: &[u8]) -> Result<Map<String, Value>>;
}

/// Private fields restrict construction to this module and its format definitions.
pub(crate) struct EncodedAddress {
    collection: String,
    key: String,
}

impl EncodedAddress {
    pub(crate) fn into_parts(self) -> (String, String) {
        (self.collection, self.key)
    }
}

pub(crate) struct EncodedMutation {
    address: EncodedAddress,
    body: Option<Vec<u8>>,
}

impl EncodedMutation {
    pub(crate) fn into_parts(self) -> (String, String, Option<Vec<u8>>) {
        let (collection, key) = self.address.into_parts();
        (collection, key, self.body)
    }
}

/// Only an admitted format batch reaches the final native writer.
pub(crate) struct EncodedBatch {
    operations: Vec<EncodedMutation>,
}

impl EncodedBatch {
    fn checked(
        operations: Vec<EncodedMutation>,
        max_operations: usize,
        max_key_bytes: usize,
        max_body_bytes: usize,
        max_batch_bytes: usize,
    ) -> Result<Self> {
        if operations.is_empty() || operations.len() > max_operations {
            return Err("batch exceeds the configured operation limit".into());
        }
        let mut addresses = std::collections::HashSet::new();
        let mut total = 0_usize;
        for operation in &operations {
            let address = &operation.address;
            let body_bytes = operation.body.as_ref().map_or(0, Vec::len);
            if address.key.is_empty()
                || address.key.len() > max_key_bytes
                || body_bytes > max_body_bytes
            {
                return Err("operation exceeds the configured key or record limit".into());
            }
            if !addresses.insert((&address.collection, &address.key)) {
                return Err("format produced duplicate batch addresses".into());
            }
            total = total
                .saturating_add(address.collection.len())
                .saturating_add(address.key.len())
                .saturating_add(body_bytes);
        }
        if total > max_batch_bytes {
            return Err("encoded batch exceeds the configured byte limit".into());
        }
        Ok(Self { operations })
    }

    pub(crate) fn into_operations(self) -> Vec<EncodedMutation> {
        self.operations
    }
}

pub(crate) struct EncodedRange {
    collection: String,
    prefix: String,
}

impl EncodedRange {
    pub(crate) fn into_parts(self) -> (String, String) {
        (self.collection, self.prefix)
    }
}
