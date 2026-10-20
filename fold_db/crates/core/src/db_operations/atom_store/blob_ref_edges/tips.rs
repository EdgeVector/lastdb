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
        old: Option<&PerKeyRecord>,
        new: Option<&PerKeyRecord>,
    ) -> Result<(), SchemaError> {
        let old_edges = old.map_or_else(Vec::new, |record| tip_edges(key, record));
        let new_edges = new.map_or_else(Vec::new, |record| tip_edges(key, record));
        let new_keys: HashSet<_> = new_edges
            .iter()
            .map(|edge| edge.storage_key(None))
            .collect();
        for edge in old_edges {
            self.targets.push(edge.blob_ref.clone());
            let key = edge.storage_key(None);
            if !new_keys.contains(&key) {
                self.deletes.push(KvMutation::delete(key.into_bytes()));
            }
        }
        for edge in new_edges {
            self.targets.push(edge.blob_ref.clone());
            let value = serde_json::to_vec(&edge).map_err(|error| {
                SchemaError::InvalidData(format!("serialize tip blob reference: {error}"))
            })?;
            self.puts
                .push(KvMutation::put(edge.storage_key(None).into_bytes(), value));
        }
        Ok(())
    }
}

pub(super) fn tip_edges(key: &str, record: &PerKeyRecord) -> Vec<BlobRefEdge> {
    let Some(meta) = record.meta.as_ref().filter(|meta| !meta.tombstoned) else {
        return Vec::new();
    };
    crate::atom::file_pointer::blob_refs_of_atom(&serde_json::Value::Null, meta.metadata.as_ref())
        .into_iter()
        .map(|blob_ref| BlobRefEdge {
            blob_ref,
            atom_uuid: record.entry.atom_uuid.clone(),
            tip_key: Some(key.to_string()),
            atom_scope: None,
        })
        .collect()
}
