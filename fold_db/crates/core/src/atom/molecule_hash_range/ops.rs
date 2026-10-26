//! Accessors, local writes, and metadata.

use crate::schema::types::key_value::KeyValue;
use crate::security::Ed25519KeyPair;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

use super::MoleculeHashRange;
use crate::atom::{now_nanos, AtomEntry};

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

    /// Add a local tip using explicit hash and range values.
    /// A new write advances its author clock even when the atom is unchanged.
    pub fn set_atom_uuid_from_values(
        &mut self,
        hash_value: String,
        range_value: String,
        atom_uuid: String,
        keypair: &Ed25519KeyPair,
    ) {
        self.set_atom_uuid_from_values_with_author(
            hash_value,
            range_value,
            atom_uuid,
            keypair,
            0,
            String::new(),
        );
    }

    /// Add a local atom with the mutation's signed author-clock identity.
    pub fn set_atom_uuid_from_values_with_author(
        &mut self,
        hash_value: String,
        range_value: String,
        atom_uuid: String,
        keypair: &Ed25519KeyPair,
        logical_counter: u64,
        mutation_uuid: String,
    ) {
        let device_id = keypair.public_key_base64();
        self.version += 1;
        let written_at = now_nanos();
        self.atom_uuids.entry(hash_value).or_default().insert(
            range_value,
            AtomEntry::thin_with_author(
                atom_uuid,
                written_at,
                logical_counter,
                device_id,
                mutation_uuid,
                String::new(),
            ),
        );
        self.updated_at = Utc::now();
    }

    /// Clear conflict decisions after their successful store.
    pub(crate) fn clear_pending_tip_conflicts(&mut self) {
        self.pending_tip_conflicts.clear();
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

    /// Removes the reference at the specified hash and range values.
    /// Bumps the version counter if an entry was actually removed.
    pub fn remove_atom_uuid(&mut self, hash_value: &str, range_value: &str) -> Option<String> {
        let (entry, remove_empty_hash) = match self.atom_uuids.get_mut(hash_value) {
            Some(range_map) => {
                let entry = range_map.remove(range_value);
                let remove_empty_hash = entry.is_some() && range_map.is_empty();
                (entry, remove_empty_hash)
            }
            None => return None,
        };

        if remove_empty_hash {
            self.atom_uuids.remove(hash_value);
        }

        if let Some(entry) = entry {
            self.remove_key_metadata(hash_value, range_value);
            self.version += 1;
            self.updated_at = Utc::now();
            return Some(entry.atom_uuid);
        }

        None
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

    /// Sets per-key metadata for a given hash + range key combination.
    pub fn set_key_metadata(
        &mut self,
        hash: String,
        range: String,
        meta: crate::atom::KeyMetadata,
    ) {
        self.key_metadata
            .entry(hash)
            .or_default()
            .insert(range, meta);
    }

    /// Returns the per-key metadata for a given hash + range key, if any.
    #[must_use]
    pub fn get_key_metadata(&self, hash: &str, range: &str) -> Option<&crate::atom::KeyMetadata> {
        self.key_metadata
            .get(hash)
            .and_then(|range_map| range_map.get(range))
    }

    pub(super) fn remove_key_metadata(&mut self, hash: &str, range: &str) {
        if let Some(range_map) = self.key_metadata.get_mut(hash) {
            range_map.remove(range);
            if range_map.is_empty() {
                self.key_metadata.remove(hash);
            }
        }
    }
}
