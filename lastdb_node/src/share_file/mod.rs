//! Application records for transferring a file through an ordinary Share snapshot.
use cloud_backup_protocol::{Digest, Id};
use fold_db::schema::types::KeyValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;

mod extract;
mod pack;

pub use extract::{get_file, list_files};
pub use pack::{add_file, recover_pending};

pub const CATALOG_HASH: &str = "files-v1";
pub const CATALOG_ENTRIES_FIELD: &str = "entries";
pub const CHUNK_DATA_FIELD: &str = "data";
pub const MAX_TOTAL_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CATALOG_BYTES: usize = 32 * 1024;
const MAX_FILES: usize = 128;
const CHUNK_BYTES: usize = 32 * 1024;
const CHUNK_BATCH: usize = 16;

/// The app's installed catalog Hash schema and chunk HashRange(String) schema.
pub struct FileShareSchemas<'a> {
    pub catalog: &'a str,
    pub chunks: &'a str,
}

/// A live catalog record and its LastDB revision for conditional writes.
#[derive(Clone, Copy)]
pub struct CatalogRecord<'a> {
    pub revision: &'a str,
    pub entries: &'a str,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileManifest {
    pub id: Id,
    pub name: String,
    pub size_bytes: u64,
    pub chunks: u64,
    pub sha256: Digest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    version: u8,
    files: Vec<FileManifest>,
    pending: Option<Pending>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    id: Id,
    size_bytes: u64,
}

/// Check the catalog before releasing the publication's file-import hold.
pub fn require_complete_catalog(entries: Option<&str>) -> Result<(), String> {
    if Catalog::read(entries)?.pending.is_some() {
        return Err("file share has an incomplete import".into());
    }
    Ok(())
}

impl Catalog {
    fn read(entries: Option<&str>) -> Result<Self, String> {
        let Some(entries) = entries else {
            return Ok(Self {
                version: 1,
                files: Vec::new(),
                pending: None,
            });
        };
        if entries.len() > MAX_CATALOG_BYTES {
            return Err("file share catalog exceeds its size limit".into());
        }
        let catalog: Self =
            serde_json::from_str(entries).map_err(|_| "invalid file share catalog")?;
        catalog.validate()?;
        Ok(catalog)
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != 1 || self.files.len() > MAX_FILES {
            return Err("unsupported or oversized file share catalog".into());
        }
        let mut ids = HashSet::with_capacity(self.files.len());
        let mut size = self.pending.as_ref().map_or(0, |file| file.size_bytes);
        for file in &self.files {
            if !valid_name(&file.name)
                || file.chunks != chunk_count(file.size_bytes)
                || !ids.insert(&file.id)
            {
                return Err("invalid file share manifest".into());
            }
            size = size
                .checked_add(file.size_bytes)
                .ok_or("file share size overflow")?;
        }
        if size > MAX_TOTAL_FILE_BYTES
            || self
                .pending
                .as_ref()
                .is_some_and(|file| ids.contains(&file.id))
        {
            return Err("invalid file share catalog size or pending identity".into());
        }
        Ok(())
    }

    fn encode(&self) -> Result<String, String> {
        self.validate()?;
        let entries =
            serde_json::to_string(self).map_err(|_| "cannot encode file share catalog")?;
        if entries.len() > MAX_CATALOG_BYTES {
            return Err("file share catalog exceeds its size limit".into());
        }
        Ok(entries)
    }

    fn mutation(&self, schema: &str, revision: Option<&str>) -> Result<Value, String> {
        Ok(record_mutation(
            schema,
            catalog_key(),
            json!({CATALOG_ENTRIES_FIELD: self.encode()?}),
            revision.map_or(Edit::Create, Edit::Replace),
        ))
    }
}

fn catalog_key() -> KeyValue {
    KeyValue::new(Some(CATALOG_HASH.to_owned()), None)
}

fn chunk_key(id: &Id, index: u64) -> KeyValue {
    KeyValue::new(Some(id.as_str().to_owned()), Some(index.to_string()))
}

fn chunk_count(size_bytes: u64) -> u64 {
    size_bytes.div_ceil(CHUNK_BYTES as u64)
}

fn chunk_size(size_bytes: u64, index: u64) -> usize {
    size_bytes
        .saturating_sub(index.saturating_mul(CHUNK_BYTES as u64))
        .min(CHUNK_BYTES as u64) as usize
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !matches!(value, "." | "..")
        && !value.contains('/')
        && !value.chars().any(char::is_control)
}

enum Edit<'a> {
    Create,
    Replace(&'a str),
    Purge(&'a str),
}

fn record_mutation(schema: &str, key: KeyValue, record: Value, edit: Edit<'_>) -> Value {
    let (kind, expected) = match edit {
        Edit::Create => ("create", json!({"type": "absent"})),
        Edit::Replace(revision) => ("replace", json!({"type": "revision", "revision": revision})),
        Edit::Purge(revision) => ("purge", json!({"type": "revision", "revision": revision})),
    };
    json!({
        "type": "mutation",
        "schema": schema,
        "key_value": key,
        "record": record,
        "alternates": {},
        "mutation_type": kind,
        "expected": expected,
        "durability": "durable",
    })
}
