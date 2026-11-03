//! Injected format contract. Only child definitions can construct encoded output.
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
    fn encode(&self, values: &impl RecordValues) -> Result<EncodedRecord>;
    fn decode(&self, bytes: &[u8]) -> Result<Map<String, Value>>;
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

pub(crate) struct EncodedRecord {
    address: EncodedAddress,
    body: Vec<u8>,
}

impl EncodedRecord {
    pub(crate) fn into_parts(self) -> (String, String, Vec<u8>) {
        let (collection, key) = self.address.into_parts();
        (collection, key, self.body)
    }
}
