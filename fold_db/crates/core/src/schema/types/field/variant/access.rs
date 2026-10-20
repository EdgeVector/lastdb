use crate::db_operations::MoleculeData;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
    /// AtomEntry at `key`, using the field kind's unified HashRange disk slot.
    #[must_use]
    pub(crate) fn atom_entry_at_key(&self, key: &KeyValue) -> Option<&crate::atom::AtomEntry> {
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates(key)?;
        molecule.get_atom_entry(&hash, &range)
    }

    /// Key metadata at `key`, using the same slot projection as atom entries.
    #[must_use]
    pub(crate) fn key_metadata_at_key(&self, key: &KeyValue) -> Option<&crate::atom::KeyMetadata> {
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates(key)?;
        molecule.get_key_metadata(&hash, &range)
    }

    /// Returns whether a molecule is present in this field.
    #[must_use]
    pub fn has_molecule(&self) -> bool {
        self.molecule.is_some()
    }

    /// Drop the in-memory materialized molecule. See prior docs.
    pub fn clear_molecule(&mut self) {
        self.molecule = None;
    }

    /// Clone the molecule data for persistence, if present.
    #[must_use]
    pub fn clone_molecule_data(&self) -> Option<MoleculeData> {
        self.molecule.clone()
    }

    /// Borrow the hydrated molecule data for persistence without deep-copying
    /// large fields.
    #[must_use]
    pub(crate) fn molecule_data(&self) -> Option<&MoleculeData> {
        self.molecule.as_ref()
    }

    /// Mutable access for draining pending tip versions after persist.
    pub(crate) fn molecule_data_mut(&mut self) -> Option<&mut MoleculeData> {
        self.molecule.as_mut()
    }

    /// Install a freshly-loaded molecule into this field's slot.
    pub(crate) fn set_molecule_data(&mut self, data: MoleculeData) -> Result<(), SchemaError> {
        // Keep unified HashRange loads as HashRange for keyed fields (Hash/
        // Range/HashRange all accept HashRange via matches_data).
        if !self.kind.matches_data(&data) {
            return Err(SchemaError::InvalidData(
                "set_molecule_data: MoleculeData variant does not match field kind".to_string(),
            ));
        }
        self.molecule = Some(data);
        Ok(())
    }

    /// Returns the writer public key from a Single molecule, if present and non-empty.
    #[must_use]
    pub fn molecule_writer_pubkey(&self) -> Option<String> {
        if self.kind != FieldKind::Single {
            return None;
        }
        match &self.molecule {
            Some(m) => m
                .get_atom_entry("", "")
                .map(|e| e.writer_pubkey.clone())
                .filter(|pk| !pk.is_empty()),
            _ => None,
        }
    }

    /// Collect every atom UUID currently referenced by this field's molecule.
    pub fn collect_atom_uuids(&self, out: &mut std::collections::HashSet<String>) {
        if let Some(m) = &self.molecule {
            for (_h, _r, uuid) in m.iter_all_atoms() {
                out.insert(uuid.clone());
            }
        }
    }
}
