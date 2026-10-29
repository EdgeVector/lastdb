//! Atom body encode/decode and storage-key reads for [`AtomStore`].

use super::*;

impl AtomStore {
    /// Load established contracts for every selected protein-move slot in one
    /// batch. Unchanged peer fields can be cold after a daemon restart.
    pub(crate) async fn load_molecule_key_bundles(
        &self,
        molecules: &[String],
    ) -> Result<(), crate::schema::SchemaError> {
        if let Some(store) = &self.molecule_keys {
            store.load_existing(molecules).await?;
        }
        Ok(())
    }

    /// Warm an existing molecule key bundle without creating storage state.
    /// Schema publication establishes keys; physical tools can inspect unbound molecules.
    pub(crate) async fn load_molecule_key_bundle(
        &self,
        molecule_uuid: &str,
    ) -> Result<(), crate::schema::SchemaError> {
        if let Some(store) = self.molecule_keys.as_ref() {
            store.load(molecule_uuid).await.map_err(|error| {
                crate::schema::SchemaError::InvalidData(format!(
                    "load molecule key bundle for {molecule_uuid}: {error}"
                ))
            })?;
        }
        Ok(())
    }

    /// Batch-load atoms by full storage keys (`atom:{uuid}` or
    /// `{prefix}:atom:{uuid}`).
    ///
    /// **Must** be used instead of `raw().get_items::<Atom>(…)`: it returns
    /// the atoms that the request scope captured in memory, decodes the other
    /// rows with the strict plain-row decoder, and notes the logical path of
    /// each atom that it returns.
    pub(super) async fn get_atoms_by_storage_keys_captured(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<crate::atom::Atom>>, crate::schema::SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let captured = runtime
            .as_ref()
            .map(|runtime| runtime.captured_atoms(keys))
            .transpose()?
            .unwrap_or_else(|| vec![None; keys.len()]);
        let raw = self.load_cold_atom_bytes(keys, &captured).await?;
        let mut out = Vec::with_capacity(keys.len());
        for ((requested, raw), captured) in keys.iter().zip(raw).zip(captured) {
            let atom = match captured {
                Some(atom) => Some(atom.as_ref().clone()),
                None => match raw {
                    Some(value) => Some(decode_plain_atom(&value)?),
                    None => None,
                },
            };
            if let Some(atom) = &atom {
                crate::storage::disk_pack::logical_paths::source::note_atom(
                    runtime.clone(),
                    requested,
                    atom,
                )?;
            }
            out.push(atom);
        }
        Ok(out)
    }

    /// Read the exact current keys that the caller selected.
    async fn load_cold_atom_bytes(
        &self,
        keys: &[String],
        captured: &[Option<Arc<crate::atom::Atom>>],
    ) -> Result<Vec<Option<Vec<u8>>>, crate::schema::SchemaError> {
        let cold: Vec<_> = captured
            .iter()
            .enumerate()
            .filter(|(_, atom)| atom.is_none())
            .map(|(index, _)| index)
            .collect();
        let lookup = cold
            .iter()
            .map(|&index| keys[index].as_bytes().to_vec())
            .collect();
        let values = self.main_store.inner().get_many(lookup).await?;
        let mut raw = vec![None; keys.len()];
        for (index, value) in cold.into_iter().zip(values) {
            raw[index] = value;
        }
        Ok(raw)
    }
}

/// The plain row of a disk format v2 home: the atom JSON with sorted keys, no
/// content seal and no molecule key bundle name. The block encryption of each
/// run file is the encryption at rest of this row.
pub(crate) fn encode_plain_atom(
    atom: &crate::atom::Atom,
) -> Result<Vec<u8>, crate::schema::SchemaError> {
    serde_json::to_value(atom)
        .and_then(|value| serde_json::to_vec(&value))
        .map_err(|e| crate::schema::SchemaError::InvalidData(format!("serialize plain atom: {e}")))
}

pub(crate) fn decode_plain_atom(
    stored: &[u8],
) -> Result<crate::atom::Atom, crate::schema::SchemaError> {
    let value = serde_json::from_slice(stored).map_err(|e| {
        crate::schema::SchemaError::InvalidData(format!("decode plain atom row JSON: {e}"))
    })?;
    serde_json::from_value(plain_atom_value(value)?)
        .map_err(|e| crate::schema::SchemaError::InvalidData(format!("decode plain atom: {e}")))
}

/// A plain row names no molecule key bundle: its content has no seal.
fn plain_atom_value(
    value: serde_json::Value,
) -> Result<serde_json::Value, crate::schema::SchemaError> {
    if value.get("molecule_key_bundle").is_some() {
        return Err(crate::schema::SchemaError::InvalidData(
            "a disk format v2 home holds plain atom rows, not sealed rows".into(),
        ));
    }
    Ok(value)
}
