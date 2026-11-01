use std::collections::HashMap;

use crate::schema::types::key_value::KeyValue;

use super::{FieldKind, FieldValue, FieldVariant};

impl FieldVariant {
    /// AtomEntry at `key`, using the field kind's unified HashRange disk slot.
    #[must_use]
    pub fn atom_entry_at_key(&self, key: &KeyValue) -> Option<&crate::atom::AtomEntry> {
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates_ref(key)?;
        molecule.get_atom_entry(hash, range)
    }

    /// Key metadata at `key`, using the same slot projection as atom entries.
    #[must_use]
    pub fn key_metadata_at_key(&self, key: &KeyValue) -> Option<&crate::atom::KeyMetadata> {
        let molecule = self.molecule.as_ref()?;
        let (hash, range) = self.slot_coordinates_ref(key)?;
        molecule.get_key_metadata(hash, range)
    }

    /// Drop the in-memory materialized molecule. See prior docs.
    pub fn clear_molecule(&mut self) {
        self.molecule = None;
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

    /// Stamp stable molecule identity; keyed fields retain the selected writer.
    /// Partial molecule counters describe the read's materialization, not the
    /// selected logical record, so query metadata does not expose them.
    pub fn stamp_molecule_meta(&self, resolved: &mut HashMap<KeyValue, FieldValue>) {
        let mol_uuid = self.common().molecule_uuid().cloned();
        let mol_writer_pubkey = self.molecule_writer_pubkey();
        for fv in resolved.values_mut() {
            fv.molecule_uuid = mol_uuid.clone();
            if let Some(pk) = mol_writer_pubkey.clone() {
                fv.writer_pubkey = Some(pk);
            }
        }
    }

    /// Key metadata and writer of the tip at `kv` in the hydrated molecule.
    pub fn entry_meta_at(
        &self,
        kv: &KeyValue,
    ) -> (Option<crate::atom::KeyMetadata>, Option<String>) {
        (
            self.key_metadata_at_key(kv).cloned(),
            self.atom_entry_at_key(kv)
                .map(|entry| entry.writer_pubkey().to_string()),
        )
    }
}
