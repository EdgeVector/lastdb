//! The stored value at one record key of a format 7 home (the record model).
//!
//! One value holds the fields of the record and one stamp for each field
//! (decision D1). A write or a peer replay merges field by field: the newer
//! stamp wins for each field, so two devices that change two different fields
//! of one record keep both changes.
//!
//! ```text
//! {"v":1,
//!  "fields":{"title":"Plan v2","status":"done"},
//!  "stamps":{"title":[written_at,logical_counter,device_id,mutation_id], ...},
//!  "meta":{"source_file_name":"plan.pdf","metadata":{"file_blob_ref":"..."},
//!          "stamp":[written_at,logical_counter,device_id,mutation_id]},
//!  "deleted":[written_at,logical_counter,device_id,mutation_id]}
//! ```
//!
//! The metadata belongs to the record, not to a field (decision D7): one
//! source file name and one metadata map, with their own stamp (`meta.rs`).
//!
//! A stamp without a value is a removed field: it still refuses an older
//! write of that field. `deleted` is the stamp of the last delete. It is a
//! floor: a field write at or below it does not bring the record back, and a
//! newer write makes a new record. A tombstone is a value with no fields.

use crate::atom::file_pointer::{blob_refs_of_atom, is_private_pointer_hash};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

mod meta;

pub use meta::META_FILE_REF_KEYS;

/// The format version of a stored record value.
pub const RECORD_VALUE_VERSION: u8 = 1;

/// The cap for the encoded value of the full record (decision D3). The app
/// moves a large field to a file blob and keeps the pointer in the record.
pub const MAX_RECORD_BYTES: usize = 64 * 1024;

/// The stamp of one field write. The field order is the merge order: write
/// time, then the logical counter, then the device ID, then the mutation ID.
/// This is the order of a tip without the atom ID.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(from = "StampWire", into = "StampWire")]
pub struct FieldStamp {
    /// The write time of the original device, in nanoseconds since the epoch.
    pub written_at: u64,
    /// The logical counter of the author. Zero when the write supplies none.
    pub logical_counter: u64,
    /// The device identity, usually the public key of the writer.
    pub device_id: String,
    /// The mutation identity.
    pub mutation_id: String,
}

/// The stored form of a stamp: one JSON array in merge order.
#[derive(Serialize, Deserialize)]
struct StampWire(u64, u64, String, String);

impl From<StampWire> for FieldStamp {
    fn from(wire: StampWire) -> Self {
        Self {
            written_at: wire.0,
            logical_counter: wire.1,
            device_id: wire.2,
            mutation_id: wire.3,
        }
    }
}

impl From<FieldStamp> for StampWire {
    fn from(stamp: FieldStamp) -> Self {
        Self(
            stamp.written_at,
            stamp.logical_counter,
            stamp.device_id,
            stamp.mutation_id,
        )
    }
}

/// The result of one field merge.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldMerge {
    /// The write is newer. It set or removed the field. `old` and `old_stamp`
    /// are the replaced value and stamp, when the record had them. A replaced
    /// value from another device goes to a conflict record.
    Written {
        old: Option<Value>,
        old_stamp: Option<FieldStamp>,
    },
    /// The stored stamp is the same or newer. The record does not change.
    Kept,
    /// The delete floor is the same or newer. The record does not change.
    Deleted,
}

/// Why a stored record value is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordValueError {
    /// The bytes are not a record value of this format.
    Format(String),
    /// The encoded record is above [`MAX_RECORD_BYTES`].
    TooLarge { bytes: usize },
}

impl std::fmt::Display for RecordValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Format(reason) => write!(f, "invalid record value: {reason}"),
            Self::TooLarge { bytes } => write!(
                f,
                "the record is {bytes} bytes; the cap is {MAX_RECORD_BYTES} bytes. Move a large \
                 field to a file blob and keep its pointer in the record"
            ),
        }
    }
}

impl std::error::Error for RecordValueError {}

/// The metadata of one record (decision D7): the source file name and the
/// metadata map, with the stamp of the newest write that changed them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_file_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    /// The stamp of the write that set this metadata.
    pub stamp: FieldStamp,
}

/// The value at one record key: a live record or a tombstone.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredRecord {
    pub v: u8,
    /// The present field values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, Value>,
    /// One stamp for each field write above the delete floor, including the
    /// writes that removed a field.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub stamps: BTreeMap<String, FieldStamp>,
    /// The record metadata. A write without metadata keeps it; a local write
    /// with metadata replaces only the keys that it sends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<RecordMeta>,
    /// The stamp of the last delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted: Option<FieldStamp>,
}

impl Default for StoredRecord {
    /// An absent record: no fields, no stamps and no delete floor.
    fn default() -> Self {
        Self {
            v: RECORD_VALUE_VERSION,
            fields: BTreeMap::new(),
            stamps: BTreeMap::new(),
            meta: None,
            deleted: None,
        }
    }
}

impl StoredRecord {
    /// True when the record has a field value. A tombstone has none.
    #[must_use]
    pub fn is_live(&self) -> bool {
        !self.fields.is_empty()
    }

    /// The write time of the record: the newest write time of a field stamp
    /// or of the metadata stamp, in nanoseconds. A TTL ages a record by this
    /// time. `None` when the record holds no stamp above its delete floor.
    #[must_use]
    pub fn written_at(&self) -> Option<u64> {
        self.stamps
            .values()
            .chain(self.meta.as_ref().map(|meta| &meta.stamp))
            .map(|stamp| stamp.written_at)
            .max()
    }

    /// Merge one field write. `None` removes the field. The newer stamp wins;
    /// an equal stamp keeps the stored value, so a replay changes nothing.
    pub fn merge_field(
        &mut self,
        field: &str,
        value: Option<Value>,
        stamp: FieldStamp,
    ) -> FieldMerge {
        if self.deleted.as_ref().is_some_and(|floor| stamp <= *floor) {
            return FieldMerge::Deleted;
        }
        if self
            .stamps
            .get(field)
            .is_some_and(|current| stamp <= *current)
        {
            return FieldMerge::Kept;
        }
        let old_stamp = self.stamps.insert(field.to_owned(), stamp);
        let old = match value {
            Some(value) => self.fields.insert(field.to_owned(), value),
            None => self.fields.remove(field),
        };
        FieldMerge::Written { old, old_stamp }
    }

    /// Apply a delete. Each field at or below `stamp` goes, and `stamp`
    /// becomes the delete floor. A field with a newer stamp stays: it is a
    /// write after the delete. Returns true when the delete removed a value.
    pub fn delete(&mut self, stamp: FieldStamp) -> bool {
        if self.deleted.as_ref().is_some_and(|floor| stamp <= *floor) {
            return false;
        }
        let fields = &mut self.fields;
        let before = fields.len();
        self.stamps.retain(|field, current| {
            let keep = *current > stamp;
            if !keep {
                fields.remove(field);
            }
            keep
        });
        let mut removed = fields.len() != before;
        if self.meta.as_ref().is_some_and(|meta| meta.stamp <= stamp) {
            self.meta = None;
            removed = true;
        }
        self.deleted = Some(stamp);
        removed
    }

    /// Every file blob that a field value or the metadata of this record
    /// names, sorted and without duplicates. The write path compares the old
    /// and new sets to change the file blob counters in the same batch.
    #[must_use]
    pub fn blob_refs(&self) -> Vec<String> {
        let mut refs: Vec<String> = self
            .fields
            .values()
            .flat_map(|value| blob_refs_of_atom(value, None))
            .collect();
        if let Some(meta) = &self.meta {
            let mut metadata: HashMap<String, String> = meta.metadata.clone().into_iter().collect();
            // An atom drops the plaintext hash of its own account-v2 pointer
            // (`blob_refs_of_atom`). The record metadata can describe the
            // pointer of any field, so each field gets the same check.
            if metadata.get("file_hash").is_some_and(|hash| {
                self.fields
                    .values()
                    .any(|value| is_private_pointer_hash(value, hash))
            }) {
                metadata.remove("file_hash");
            }
            refs.extend(blob_refs_of_atom(&Value::Null, Some(&metadata)));
        }
        refs.sort_unstable();
        refs.dedup();
        refs
    }

    /// The stored bytes. A record above [`MAX_RECORD_BYTES`] is refused.
    pub fn encode(&self) -> Result<Vec<u8>, RecordValueError> {
        let bytes = self.encode_uncapped()?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(RecordValueError::TooLarge { bytes: bytes.len() });
        }
        Ok(bytes)
    }

    /// The stored bytes without the cap. A request that writes one key twice
    /// stages the earlier record before the later write; the cap applies to
    /// the final record of the request.
    pub fn encode_uncapped(&self) -> Result<Vec<u8>, RecordValueError> {
        serde_json::to_vec(self).map_err(|error| RecordValueError::Format(error.to_string()))
    }

    /// Read stored bytes. The value must have this format version, a stamp for
    /// each field, and no field or metadata stamp at or below the delete floor.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordValueError> {
        let record: Self = serde_json::from_slice(bytes)
            .map_err(|error| RecordValueError::Format(error.to_string()))?;
        if record.v != RECORD_VALUE_VERSION {
            return Err(RecordValueError::Format("unknown version".into()));
        }
        if record
            .fields
            .keys()
            .any(|field| !record.stamps.contains_key(field))
        {
            return Err(RecordValueError::Format("a field has no stamp".into()));
        }
        if let Some(floor) = &record.deleted {
            if record
                .stamps
                .values()
                .chain(record.meta.as_ref().map(|meta| &meta.stamp))
                .any(|stamp| stamp <= floor)
            {
                return Err(RecordValueError::Format(
                    "a stamp is at or below the delete floor".into(),
                ));
            }
        }
        Ok(record)
    }
}
