//! The metadata merge of one record (decision D7).
//!
//! A local write replaces the source file name when it carries one, and each
//! metadata key that it sends. The stored keys that it does not send stay.

use super::{FieldStamp, RecordMeta, StoredRecord};
use std::collections::BTreeMap;

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

    /// True when the delete floor or the stored metadata stamp is the same as
    /// or newer than `stamp`.
    fn meta_refuses(&self, stamp: &FieldStamp) -> bool {
        self.deleted.as_ref().is_some_and(|floor| stamp <= floor)
            || self.meta.as_ref().is_some_and(|meta| *stamp <= meta.stamp)
    }
}
