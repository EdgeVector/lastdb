//! Keep the first stored native atom, as the single-atom write path does.

use super::*;
use std::collections::HashSet;

pub(super) struct NativeAtomBatch {
    pub(super) existing: HashMap<String, Atom>,
    pub(super) blob_refs: Vec<String>,
}

impl AtomStore {
    /// The caller holds the atom GC and reference locks. Read the exact scoped
    /// storage keys through the request's accepted/private view; never use a
    /// uuid-only locator that could select another partition or scope.
    pub(super) async fn native_atom_batch(
        &self,
        atoms: &[SealedAtom],
        storage_prefix: Option<&str>,
    ) -> Result<NativeAtomBatch, SchemaError> {
        let mut existing = HashMap::new();
        let mut seen = HashSet::new();
        let keys: Vec<String> = atoms
            .iter()
            .map(|(atom, _)| build_storage_key(storage_prefix, &atom_key_codec::body_key(atom.uuid())))
            .filter(|key| seen.insert(key.clone()))
            .collect();
        if self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
            .is_some()
        {
            // The native get-many API accepts at most 10,000 keys per call.
            for keys in keys.chunks(10000) {
                let stored = self
                    .main_store
                    .inner()
                    .get_many(keys.iter().map(|key| key.as_bytes().to_vec()).collect())
                    .await
                    .map_err(SchemaError::from)?;
                for (key, body) in keys.iter().zip(stored) {
                    if let Some(body) = body {
                        existing.insert(key.clone(), self.decode_atom_bytes(&body).await?);
                    }
                }
            }
        }
        seen.clear();
        let blob_refs = atoms
            .iter()
            .filter_map(|(atom, _)| {
                let key = build_storage_key(storage_prefix, &atom_key_codec::body_key(atom.uuid()));
                seen.insert(key.clone())
                    .then(|| existing.get(&key).unwrap_or(atom))
            })
            .flat_map(|atom| {
                crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
            })
            .collect();
        Ok(NativeAtomBatch {
            existing,
            blob_refs,
        })
    }
}

impl NativeAtomBatch {
    pub(super) fn record_keep_small(
        &self,
        meters: &crate::db_operations::keep_small::KeepSmallMeters,
        puts: Vec<(String, String, u64, u64, bool)>,
    ) {
        for (schema, uuid, logical, blob_bytes, complete) in puts {
            meters.record_atom_put_with_blob(&schema, &uuid, logical, blob_bytes, complete);
        }
        for atom in self.existing.values() {
            let logical = serde_json::to_vec(atom).map_or(0, |v| v.len() as u64);
            let blob_bytes = crate::atom::file_pointer::blob_logical_bytes_of_atom(
                atom.content(),
                atom.metadata(),
            );
            meters.remember_atom_contribution(
                atom.source_schema_name(),
                atom.uuid(),
                logical,
                blob_bytes.unwrap_or_default(),
                blob_bytes.is_some(),
                false,
            );
        }
    }
}
