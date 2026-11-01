//! Accessors, local writes, and metadata.

use crate::schema::types::key_value::KeyValue;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

use super::MoleculeHashRange;
use crate::atom::AtomEntry;

impl MoleculeHashRange {
    /// Returns the unique identifier of this molecule.
    #[must_use]
    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    /// Returns the timestamp of the last update.
    #[must_use]
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Returns the UUID of the Atom referenced by the specified hash and range values.
    #[must_use]
    pub fn get_atom_uuid(&self, hash_value: &str, range_value: &str) -> Option<&String> {
        self.atom_uuids
            .get(hash_value)
            .and_then(|range_map| range_map.get(range_value))
            .map(|e| &e.atom_uuid)
    }

    /// Returns the full AtomEntry at the specified hash and range values, if present.
    #[must_use]
    pub fn get_atom_entry(&self, hash_value: &str, range_value: &str) -> Option<&AtomEntry> {
        self.atom_uuids
            .get(hash_value)
            .and_then(|range_map| range_map.get(range_value))
    }

    /// Returns all atom UUIDs for a given hash value.
    #[must_use]
    pub fn get_atoms_for_hash(&self, hash_value: &str) -> Option<BTreeMap<String, String>> {
        self.atom_uuids.get(hash_value).map(|range_map| {
            range_map
                .iter()
                .map(|(k, e)| (k.clone(), e.atom_uuid.clone()))
                .collect()
        })
    }

    /// Returns the total number of atoms in this molecule.
    #[must_use]
    pub fn atom_count(&self) -> usize {
        self.atom_uuids
            .values()
            .map(std::collections::BTreeMap::len)
            .sum()
    }

    /// Checks if this molecule is empty (no atoms).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.atom_uuids.is_empty()
    }

    /// Returns an iterator over all hash values in this molecule.
    pub fn hash_values(&self) -> impl Iterator<Item = &String> {
        self.atom_uuids.keys()
    }

    /// Returns an iterator over all atoms across all hash groups
    /// Each item is (hash_value, range_value, atom_uuid)
    pub fn iter_all_atoms(&self) -> impl Iterator<Item = (&String, &String, &String)> {
        self.atom_uuids.iter().flat_map(|(hash_value, range_map)| {
            range_map
                .iter()
                .map(move |(range_value, entry)| (hash_value, range_value, &entry.atom_uuid))
        })
    }

    /// Returns the version counter for this molecule.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// First `n` live `(hash, range)` keys in page order (range, then hash).
    ///
    /// The atom map is the source. One entry per key, and a removed key is
    /// absent. This function does not read an order log.
    #[must_use]
    pub fn sample(&self, n: usize) -> Vec<KeyValue> {
        let mut keys: Vec<KeyValue> = self
            .iter_all_atoms()
            .map(|(hash, range, _)| KeyValue::new(Some(hash.clone()), Some(range.clone())))
            .collect();
        keys.sort_by(KeyValue::cmp_storage_order);
        keys.truncate(n);
        keys
    }

    /// Returns the per-key metadata for a given hash + range key, if any.
    #[must_use]
    pub fn get_key_metadata(&self, hash: &str, range: &str) -> Option<&crate::atom::KeyMetadata> {
        self.key_metadata
            .get(hash)
            .and_then(|range_map| range_map.get(range))
    }
}
