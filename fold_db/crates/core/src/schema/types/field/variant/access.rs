use crate::db_operations::MoleculeData;
use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldVariant};

impl FieldVariant {
    /// AtomEntry at `key`, using the field kind's unified HashRange disk slot.
    #[must_use]
    pub(crate) fn atom_entry_at_key(&self, key: &KeyValue) -> Option<&crate::atom::AtomEntry> {
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates_ref(key)?;
        molecule.get_atom_entry(hash, range)
    }

    /// Key metadata at `key`, using the same slot projection as atom entries.
    #[must_use]
    pub(crate) fn key_metadata_at_key(&self, key: &KeyValue) -> Option<&crate::atom::KeyMetadata> {
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates_ref(key)?;
        molecule.get_key_metadata(hash, range)
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
    pub(crate) fn set_molecule_data(&mut self, data: MoleculeData) {
        self.molecule = Some(data);
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
                .map(|e| e.writer_pubkey().to_string())
                .filter(|pk| !pk.is_empty()),
            _ => None,
        }
    }
}
