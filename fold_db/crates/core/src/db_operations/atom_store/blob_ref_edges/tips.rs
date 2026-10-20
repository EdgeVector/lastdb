use super::*;
use crate::db_operations::atom_store::PerKeyRecord;
use crate::storage::KvMutation;
use std::collections::HashSet;

/// Exact metadata-root changes for accepted tip winners, with no source reads.
#[derive(Default)]
pub(crate) struct TipBlobChanges {
    pub(crate) puts: Vec<KvMutation>,
    pub(crate) deletes: Vec<KvMutation>,
    pub(crate) targets: Vec<String>,
}

impl TipBlobChanges {
    pub(crate) fn replace(
        &mut self,
        key: &str,
        scope: Option<&str>,
        old: Option<&PerKeyRecord>,
        new: Option<&PerKeyRecord>,
    ) -> Result<(), SchemaError> {
        let old_edges = old.map_or_else(Vec::new, |record| tip_edges(key, scope, record));
        let new_edges = new.map_or_else(Vec::new, |record| tip_edges(key, scope, record));
        let new_keys: HashSet<_> = new_edges.iter().map(BlobRefEdge::storage_key).collect();
        for edge in old_edges {
            self.targets.push(edge.blob_ref.clone());
            let key = edge.storage_key();
            if !new_keys.contains(&key) {
                self.deletes.extend(edge.delete_mutations());
            }
        }
        for edge in new_edges {
            self.targets.push(edge.blob_ref.clone());
            self.puts.extend(
                edge.put_rows()?
                    .into_iter()
                    .map(|(key, value)| KvMutation::put(key, value)),
            );
        }
        Ok(())
    }
}

pub(super) fn tip_edges(key: &str, scope: Option<&str>, record: &PerKeyRecord) -> Vec<BlobRefEdge> {
    let Some(meta) = record.meta.as_ref().filter(|meta| !meta.tombstoned) else {
        return Vec::new();
    };
    crate::atom::file_pointer::blob_refs_of_atom(&serde_json::Value::Null, meta.metadata.as_ref())
        .into_iter()
        .map(|blob_ref| BlobRefEdge {
            blob_ref,
            source: BlobRefSource::Tip {
                atom_uuid: record.entry.atom_uuid.clone(),
                key: key.to_string(),
                scope: scope.map(str::to_string),
            },
        })
        .collect()
}
