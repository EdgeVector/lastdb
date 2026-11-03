//! The single composition point for both concrete example formats.
use super::fields::{self, FieldsBlock, FieldsCodec};
use super::{EncodedAddress, EncodedRecord, Format, HomeLayout, Result};
use lastdb_model::record_replacement::RecordValues;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};

const HOME: HomeLayout = HomeLayout {
    marker: "record-format-spike.json",
    identity: "identity.key",
    data: "data",
    max_record_bytes: 64 * 1024,
};

pub(crate) const PLAIN: RecordFormat<Map<String, Value>> = RecordFormat {
    home: HOME,
    collection: "records",
    record_prefix: "record:",
    version: 2,
    max_key_bytes: 4096,
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
    version: 3,
    max_key_bytes: 1024,
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

    fn encode(&self, values: &impl RecordValues) -> Result<EncodedRecord> {
        let address = self.address(values.kind(), values.key())?;
        let child = (self.fields.encode)(self.fields.version, values.fields())?;
        let body = serde_json::to_vec(&StoredRecord {
            version: self.version,
            fields: child,
        })?;
        // The cap includes the parent envelope and every nested block.
        if body.len() > self.home.max_record_bytes {
            return Err("encoded record exceeds the configured limit".into());
        }
        Ok(EncodedRecord { address, body })
    }

    fn decode(&self, bytes: &[u8]) -> Result<Map<String, Value>> {
        let record: StoredRecord<T> = serde_json::from_slice(bytes)?;
        if record.version != self.version {
            return Err("record version differs from the selected format".into());
        }
        (self.fields.decode)(self.fields.version, record.fields)
    }
}
