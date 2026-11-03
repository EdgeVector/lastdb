//! Data-only input for the isolated whole-record format writer spike.
use serde_json::{Map, Value};

/// The format requests these logical values; the provider has no storage API.
pub trait RecordValues {
    fn kind(&self) -> &str;
    fn key(&self) -> &str;
    fn fields(&self) -> &Map<String, Value>;
}

/// A complete replacement. An omitted field is absent from the new record.
pub struct RecordReplacement {
    kind: String,
    key: String,
    fields: Map<String, Value>,
}

impl RecordReplacement {
    #[must_use]
    pub fn new(kind: String, key: String, fields: Map<String, Value>) -> Self {
        Self { kind, key, fields }
    }
}

impl RecordValues for RecordReplacement {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn key(&self) -> &str {
        &self.key
    }

    fn fields(&self) -> &Map<String, Value> {
        &self.fields
    }
}
