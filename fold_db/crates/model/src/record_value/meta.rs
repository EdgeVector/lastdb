//! The metadata merge of one record (decision D7).
//!
//! A local write replaces the source file name when it carries one, and each
//! metadata key that it sends; the stored keys that it does not send stay. A
//! peer replay replaces the whole metadata by stamp, so two devices end with
//! the same map, except the file blob keys of this device: a peer never sends
//! them, so the stored ones stay, and a record outcome leaves them out.
//!
//! A file blob reference that only the metadata holds counts on the blob
//! counter (`StoredRecord::blob_refs`), so each path keeps it and its counter.

use super::{FieldStamp, RecordMeta, StoredRecord};
use std::collections::BTreeMap;

/// The metadata keys that name a file blob of this device.
pub const META_FILE_REF_KEYS: [&str; 2] = ["file_blob_ref", "file_hash"];

impl RecordMeta {
    /// This metadata without the keys that name a file blob of this device:
    /// the part that a record outcome carries to a peer.
    #[must_use]
    pub fn without_file_refs(mut self) -> Self {
        for key in META_FILE_REF_KEYS {
            self.metadata.remove(key);
        }
        self
    }
}

impl StoredRecord {
    /// Merge the metadata of one local write that carries a source file name
    /// or a metadata map, key by key. The newer stamp wins, as for a field.
    /// Returns true when the metadata changed.
    pub fn write_meta(
        &mut self,
        source_file_name: Option<String>,
        metadata: BTreeMap<String, String>,
        stamp: FieldStamp,
    ) -> bool {
        if self.meta_refuses(&stamp) {
            return false;
        }
        let (stored_name, mut merged) = self
            .meta
            .take()
            .map(|meta| (meta.source_file_name, meta.metadata))
            .unwrap_or_default();
        merged.extend(metadata);
        self.meta = Some(RecordMeta {
            source_file_name: source_file_name.or(stored_name),
            metadata: merged,
            stamp,
        });
        true
    }

    /// Merge the metadata of a peer record outcome. The newer stamp replaces
    /// the whole metadata and keeps the stored file blob keys. Returns true
    /// when the metadata changed.
    pub fn merge_meta(
        &mut self,
        source_file_name: Option<String>,
        mut metadata: BTreeMap<String, String>,
        stamp: FieldStamp,
    ) -> bool {
        if self.meta_refuses(&stamp) {
            return false;
        }
        if let Some(stored) = &self.meta {
            for key in META_FILE_REF_KEYS {
                if let Some(value) = stored.metadata.get(key) {
                    metadata
                        .entry(key.to_owned())
                        .or_insert_with(|| value.clone());
                }
            }
        }
        self.meta = Some(RecordMeta {
            source_file_name,
            metadata,
            stamp,
        });
        true
    }

    /// True when the delete floor or the stored metadata stamp is the same as
    /// or newer than `stamp`.
    fn meta_refuses(&self, stamp: &FieldStamp) -> bool {
        self.deleted.as_ref().is_some_and(|floor| stamp <= floor)
            || self.meta.as_ref().is_some_and(|meta| *stamp <= meta.stamp)
    }
}
