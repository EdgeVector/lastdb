//! Row construction for batched atom writes.

use super::*;

/// Durable rows built from one atom batch, before any write.
pub(super) struct AtomBatchRows {
    pub(super) blob_edge_items: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) atom_items: Vec<(Vec<u8>, Vec<u8>)>,
    pub(super) index_items: Vec<(String, Value)>,
    pub(super) keep_small_puts: Vec<(String, String, u64, u64, bool)>,
}

impl AtomStore {
    /// Encode every atom of a batch into its durable rows: the body (plus an
    /// optional gc marker), the schema-index marker, the blob
    /// reference edges, and the keep-small accounting entries.
    pub(super) async fn build_atom_batch_rows(
        &self,
        atoms: &[SealedAtom],
        storage_prefix: Option<&str>,
        automatic_gc_generation: u64,
        existing: &HashMap<String, Atom>,
    ) -> Result<AtomBatchRows, SchemaError> {
        // Deduplicate by key - atoms with same content have same UUID.
        // Canonical atoms stay in `main`; the schema-keyed marker is derived
        // and lives in the separate local-only `schema_index` namespace so
        // point reads over `atom:{uuid}` do not pay for index marker churn.
        let mut seen_keys = std::collections::HashSet::new();
        let mut seen_gc_markers = std::collections::HashSet::new();
        let mut seen_blob_edges = std::collections::HashSet::new();
        let mut blob_edge_items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut atom_items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut index_items: Vec<(String, Value)> = Vec::new();
        let mut keep_small_puts: Vec<(String, String, u64, u64, bool)> = Vec::new();
        for (atom, seal_molecule) in atoms {
            crate::atom::enforce_atom_content_limit(atom.source_schema_name(), atom.content())?;
            let key = build_storage_key(storage_prefix, &atom_key_codec::body_key(atom.uuid()));
            if !seen_keys.insert(key.clone()) {
                continue; // Skip duplicate keys
            }
            let stored = existing.get(&key);
            let atom = stored.unwrap_or(atom);
            let index_key = build_storage_key(
                storage_prefix,
                &schema_index_codec::record_key(atom.source_schema_name(), atom.uuid()),
            );
            let bundle_molecule = storage_prefix.and(seal_molecule.as_deref());
            if stored.is_none() {
                let value = self.encode_atom_bytes(atom, bundle_molecule).await?;
                atom_items.push((key.into_bytes(), value));
            }
            let logical = serde_json::to_vec(atom).map_or(0, |v| v.len() as u64);
            let blob_bytes = crate::atom::file_pointer::blob_logical_bytes_of_atom(
                atom.content(),
                atom.metadata(),
            );
            if stored.is_none() {
                keep_small_puts.push((
                    atom.source_schema_name().to_string(),
                    atom.uuid().to_string(),
                    logical,
                    blob_bytes.unwrap_or_default(),
                    blob_bytes.is_some(),
                ));
            }
            Self::push_blob_edge_rows(
                atom,
                storage_prefix,
                &mut seen_blob_edges,
                &mut blob_edge_items,
            )?;
            index_items.push((index_key, Value::Bool(true)));

            if automatic_gc_generation > 0 && seen_gc_markers.insert(atom.uuid().to_string()) {
                let marker_key =
                    Self::automatic_gc_atoms_reference_marker_key(storage_prefix, atom.uuid());
                let marker_value = serde_json::to_vec(&serde_json::json!({
                    "generation": automatic_gc_generation,
                }))
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "serialize automatic gc-atoms reference marker: {e}"
                    ))
                })?;
                atom_items.push((marker_key.into_bytes(), marker_value));
            }
        }
        Ok(AtomBatchRows {
            blob_edge_items,
            atom_items,
            index_items,
            keep_small_puts,
        })
    }

    /// Add one reference edge per blob the atom points at, skipping edges the
    /// batch already carries.
    fn push_blob_edge_rows(
        atom: &Atom,
        storage_prefix: Option<&str>,
        seen_blob_edges: &mut std::collections::HashSet<String>,
        blob_edge_items: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), SchemaError> {
        for blob_ref in
            crate::atom::file_pointer::blob_refs_of_atom(atom.content(), atom.metadata())
        {
            let edge = BlobRefEdge::atom(atom.uuid(), &blob_ref);
            let edge_key = edge.storage_key(storage_prefix);
            if seen_blob_edges.insert(edge_key.clone()) {
                let edge_value = serde_json::to_vec(&edge).map_err(|error| {
                    SchemaError::InvalidData(format!(
                        "serialize blob reference edge for {}: {error}",
                        atom.uuid()
                    ))
                })?;
                blob_edge_items.push((edge_key.into_bytes(), edge_value));
            }
        }
        Ok(())
    }
}
