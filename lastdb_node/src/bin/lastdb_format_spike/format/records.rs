//! The single composition point for both concrete example formats.
use super::alternate::AlternateFormat;
use super::fields::{self, FieldsBlock, FieldsCodec};
use super::{
    EncodedAddress, EncodedBatch, EncodedMutation, EncodedRange, Format, HomeLayout, Result,
};
use lastdb_model::record_replacement::RecordValues;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};

const HOME: HomeLayout = HomeLayout {
    marker: "record-format-spike.json",
    identity: "identity.key",
    data: "data",
    max_record_bytes: 64 * 1024,
};

const BY_ALIAS: AlternateFormat = AlternateFormat {
    field: "alias",
    collection: "aliases",
    prefix: "by-alias:",
    version: 1,
};

pub(crate) const PLAIN: RecordFormat<Map<String, Value>> = RecordFormat {
    home: HOME,
    collection: "records",
    record_prefix: "record:",
    version: 4,
    max_key_bytes: 4096,
    max_batch_bytes: 128 * 1024,
    max_operations: 3,
    alternate: BY_ALIAS,
    fields: FieldsCodec {
        name: "plain-fields",
        version: 1,
        encode: fields::plain_encode,
        decode: fields::plain_decode,
    },
};

pub(crate) const NESTED: RecordFormat<FieldsBlock> = RecordFormat {
    home: HomeLayout {
        data: "nested-data",
        max_record_bytes: 32 * 1024,
        ..HOME
    },
    collection: "nested-records",
    record_prefix: "nested:",
    version: 5,
    max_key_bytes: 1024,
    max_batch_bytes: 64 * 1024,
    max_operations: 3,
    alternate: BY_ALIAS,
    fields: FieldsCodec {
        name: "versioned-fields",
        version: 1,
        encode: fields::nested_encode,
        decode: fields::nested_decode,
    },
};

#[derive(Serialize)]
pub(crate) struct RecordFormat<T> {
    home: HomeLayout,
    collection: &'static str,
    record_prefix: &'static str,
    version: u32,
    max_key_bytes: usize,
    max_batch_bytes: usize,
    max_operations: usize,
    alternate: AlternateFormat,
    fields: FieldsCodec<T>,
}

/// The parent owns its wire shape; the injected child owns the nested shape.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecord<T> {
    version: u32,
    fields: T,
}

impl<T: Serialize + DeserializeOwned> Format for RecordFormat<T> {
    fn layout(&self) -> &HomeLayout {
        &self.home
    }

    fn marker(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    fn address(&self, kind: &str, key: &str) -> Result<EncodedAddress> {
        if kind.is_empty() || key.is_empty() {
            return Err("record kind and key must be nonempty".into());
        }
        // JSON tuple framing keeps separators and Unicode in either component safe.
        let framed = serde_json::to_string(&(kind, key))?;
        let key = format!("{}{framed}", self.record_prefix);
        if key.len() > self.max_key_bytes {
            return Err("encoded key exceeds the configured limit".into());
        }
        Ok(EncodedAddress {
            collection: self.collection.to_owned(),
            key,
        })
    }

    fn encode(&self, values: &impl RecordValues, previous: Option<&[u8]>) -> Result<EncodedBatch> {
        let address = self.address(values.kind(), values.key())?;
        let child = (self.fields.encode)(self.fields.version, values.fields())?;
        let body = serde_json::to_vec(&StoredRecord {
            version: self.version,
            fields: child,
        })?;
        let previous = previous.map(|bytes| self.decode(bytes)).transpose()?;
        let mut operations = vec![EncodedMutation {
            address,
            body: Some(body),
        }];
        operations.extend(self.alternate.replacement(values, previous.as_ref())?);
        // Admit every put and tombstone together, including all nested bytes.
        EncodedBatch::checked(
            operations,
            self.max_operations,
            self.max_key_bytes,
            self.home.max_record_bytes,
            self.max_batch_bytes,
        )
    }

    fn decode(&self, bytes: &[u8]) -> Result<Map<String, Value>> {
        let record: StoredRecord<T> = serde_json::from_slice(bytes)?;
        if record.version != self.version {
            return Err("record version differs from the selected format".into());
        }
        (self.fields.decode)(self.fields.version, record.fields)
    }

    fn alternate_range(&self, kind: &str, value: &str) -> Result<EncodedRange> {
        let range = self.alternate.range(kind, value)?;
        if range.prefix.len() > self.max_key_bytes {
            return Err("alternate hash exceeds the configured key limit".into());
        }
        Ok(range)
    }

    fn decode_pointer(&self, kind: &str, value: &str, key: &str, body: &[u8]) -> Result<String> {
        self.alternate.decode(kind, value, key, body)
    }

    fn decode_lookup(&self, value: &str, body: &[u8]) -> Result<Map<String, Value>> {
        let fields = self.decode(body)?;
        if self.alternate.value(&fields)? != Some(value) {
            return Err("alternate pointer does not match the current record".into());
        }
        Ok(fields)
    }
}
