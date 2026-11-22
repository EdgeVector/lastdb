//! The value of a history record: the whole record, as one JSON object.
//!
//! ```text
//! {
//!   "fields":     { "<field>": <value>, ... },
//!   "alternates": { "<name>": { "hash": "...", "range": "..." } | null, ... },
//!   "file":       { "blob": hex32, "sha256": hex32, "size_bytes": n,
//!                   "name": "...", "media_type": "..." }       // optional
//! }
//! ```
//!
//! `alternates` holds the address of the record under every alternate key its
//! schema declares, exactly as the store holds them. A client chooses these
//! addresses on every write, and no schema rule derives them from the fields, so
//! the history has to carry them: a record applied from the history then has
//! the same alternate addresses as on the node that wrote it. `file` is the
//! file the record holds, when it holds one: `sha256`, `size_bytes`, `name` and
//! `media_type` are the store's marker of the file, and `blob` is the digest of
//! the blob that carries its bytes (see `FileSource`).
//!
//! Every writer of a record value uses this one layout: the owner's own writes,
//! a publisher's writes, and a publisher's acceptance of a member's proposal.
//! An empty value is still a tombstone.

use std::collections::BTreeMap;

use fold_db::schema::types::KeyValue;
use mutation_log::Hex32;
use serde_json::{json, Map, Value};

/// The address of a record under each declared alternate key. `None` is an
/// alternate that the schema lets a record leave out.
pub type Alternates = BTreeMap<String, Option<KeyValue>>;

/// The file a record holds, as its history value names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    /// SHA-256 of the blob that carries the bytes of the file.
    pub blob: Hex32,
    /// SHA-256 of the plaintext of the file.
    pub sha256: Hex32,
    /// Size of the plaintext in bytes.
    pub size_bytes: u64,
    /// File name, as the record's marker holds it.
    pub name: Option<String>,
    /// Media type, as the record's marker holds it.
    pub media_type: Option<String>,
}

/// A record as the history carries it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordValue {
    /// The fields of the record.
    pub fields: BTreeMap<String, Value>,
    /// The record's address under each declared alternate key.
    pub alternates: Alternates,
    /// The file of the record, when it holds one.
    pub file: Option<FileRef>,
}

impl RecordValue {
    /// The bytes of the value.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut doc = Map::new();
        doc.insert(
            "fields".to_owned(),
            Value::Object(self.fields.clone().into_iter().collect()),
        );
        let alternates = self
            .alternates
            .iter()
            .map(|(name, address)| (name.clone(), address_json(address.as_ref())))
            .collect();
        doc.insert("alternates".to_owned(), Value::Object(alternates));
        if let Some(file) = &self.file {
            doc.insert("file".to_owned(), file_json(file));
        }
        Value::Object(doc).to_string().into_bytes()
    }

    /// The record of a non-empty value.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let Value::Object(mut doc) = serde_json::from_slice::<Value>(bytes)
            .map_err(|e| format!("record value is not JSON: {e}"))?
        else {
            return Err("record value is not a JSON object".into());
        };
        let Some(Value::Object(fields)) = doc.remove("fields") else {
            return Err("record value has no fields object".into());
        };
        let alternates = doc
            .remove("alternates")
            .ok_or("record value has no alternates object")?;
        let alternates = serde_json::from_value::<Alternates>(alternates)
            .map_err(|e| format!("record value alternates: {e}"))?;
        let file = doc.remove("file").map(file_from_json).transpose()?;
        if let Some(extra) = doc.keys().next() {
            return Err(format!("record value has an unknown part {extra:?}"));
        }
        Ok(Self {
            fields: fields.into_iter().collect(),
            alternates,
            file,
        })
    }
}

fn address_json(address: Option<&KeyValue>) -> Value {
    address.map_or(
        Value::Null,
        |key| json!({ "hash": key.hash, "range": key.range }),
    )
}

fn file_json(file: &FileRef) -> Value {
    let mut map = Map::new();
    map.insert("blob".to_owned(), Value::String(file.blob.to_hex()));
    map.insert("sha256".to_owned(), Value::String(file.sha256.to_hex()));
    map.insert("size_bytes".to_owned(), json!(file.size_bytes));
    if let Some(name) = &file.name {
        map.insert("name".to_owned(), Value::String(name.clone()));
    }
    if let Some(media_type) = &file.media_type {
        map.insert("media_type".to_owned(), Value::String(media_type.clone()));
    }
    Value::Object(map)
}

fn file_from_json(value: Value) -> Result<FileRef, String> {
    let Value::Object(mut map) = value else {
        return Err("record file is not an object".into());
    };
    let blob = take_hex(&mut map, "blob")?;
    let sha256 = take_hex(&mut map, "sha256")?;
    let size_bytes = map
        .remove("size_bytes")
        .and_then(|size| size.as_u64())
        .ok_or("record file has no size")?;
    let name = take_text(&mut map, "name")?;
    let media_type = take_text(&mut map, "media_type")?;
    if let Some(extra) = map.keys().next() {
        return Err(format!("record file has an unknown part {extra:?}"));
    }
    Ok(FileRef {
        blob,
        sha256,
        size_bytes,
        name,
        media_type,
    })
}

fn take_hex(map: &mut Map<String, Value>, key: &str) -> Result<Hex32, String> {
    let Some(Value::String(text)) = map.remove(key) else {
        return Err(format!("record file {key} is not text"));
    };
    Hex32::from_hex(&text).map_err(|e| format!("record file {key}: {e}"))
}

fn take_text(map: &mut Map<String, Value>, key: &str) -> Result<Option<String>, String> {
    match map.remove(key) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(format!("record file {key} is not text")),
    }
}
