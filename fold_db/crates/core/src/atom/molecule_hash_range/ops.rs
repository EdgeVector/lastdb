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

    /// Adds an atom UUID using explicit hash and range values.
    /// Bumps the version counter only when the atom actually changes.
    ///
    /// When the existing entry at `(hash_value, range_value)` already
    /// references the same `atom_uuid` **and** was signed by the supplied
    /// `keypair`'s pubkey, this is a complete no-op: `updated_at`, the
    /// existing entry's `written_at`, and the existing signature are all
    /// preserved. The `written_at` invariant is
    /// load-bearing — `merge` consumes `AtomEntry.written_at` for
    /// last-writer-wins per-key resolution, so silently bumping it on a
    /// no-op write changes cross-node merge outcomes (mirror of PR #114's
    /// merge-side fix and PR #123's `MoleculeHash` write-path fix). See
    /// `MoleculeRange::set_atom_uuid` for the rationale on the
    /// `writer_pubkey` check.
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
        if self
            .get_atom_entry(&hash_value, &range_value)
            .is_some_and(|e| {
                e.atom_uuid == atom_uuid
                    && (e.device_id == device_id
                        || e.writer_pubkey == device_id
                        || (e.device_id.is_empty() && e.writer_pubkey.is_empty()))
            })
        {
            return;
        }
        let prev_tip_id = self.archive_current_tip(&hash_value, &range_value, &atom_uuid);
        let value_changed = self.get_atom_uuid(&hash_value, &range_value) != Some(&atom_uuid);
        if value_changed {
            self.version += 1;
        }
        let written_at = now_nanos();
        self.atom_uuids.entry(hash_value).or_default().insert(
            range_value,
            AtomEntry::thin_with_author(
                atom_uuid,
                written_at,
                logical_counter,
                device_id,
                mutation_uuid,
                prev_tip_id,
            ),
        );
        self.updated_at = Utc::now();
    }

    /// If this slot already has a different atom, archive the current tip as a
    /// tip-version node and return its id for the new head's `prev_tip_id` when
    /// point-in-time history was explicitly enabled. Thin tips are the default,
    /// so ordinary Mini writes replace the head without growing `tv:` forever.
    pub(super) fn archive_current_tip(
        &mut self,
        hash: &str,
        range: &str,
        new_atom_uuid: &str,
    ) -> String {
        let Some(old) = self.get_atom_entry(hash, range).cloned() else {
            return String::new();
        };
        if old.atom_uuid == new_atom_uuid {
            // Same content (e.g. device-only no-op already filtered); keep chain.
            return old.prev_tip_id;
        }
        let version_id = self
            .tip_history_enabled
            .then(|| uuid::Uuid::new_v4().to_string());
        self.pending_replaced_tips.push((
            hash.to_string(),
            range.to_string(),
            old.clone(),
            version_id.clone(),
        ));
        if let Some(version_id) = version_id {
            self.pending_tip_versions.push((version_id.clone(), old));
            version_id
        } else {
            String::new()
        }
    }

    /// Explicitly opt this in-memory molecule into full per-slot history.
    ///
    /// This flag is not serialized: Mini remains thin after every load, while
    /// specialized callers may request history before mutating a molecule.
    /// Existing `prev_tip_id` / `tv:` chains remain readable and reclaimable.
    pub fn set_tip_history_enabled(&mut self, enabled: bool) {
        self.tip_history_enabled = enabled;
    }

    /// Drain tip versions archived since the last persist (to write as `tv:`).
    pub(crate) fn take_pending_tip_versions(&mut self) -> Vec<(String, AtomEntry)> {
        self.pending_replaced_tips.clear();
        self.pending_tip_conflicts.clear();
        std::mem::take(&mut self.pending_tip_versions)
    }

    /// Borrow pending tip versions without clearing (for read-only inspection).
    #[must_use]
    pub(crate) fn pending_tip_versions(&self) -> &[(String, AtomEntry)] {
        &self.pending_tip_versions
    }

    /// Borrow prior tips replaced or removed since the last durable persist.
    #[must_use]
    pub(crate) fn pending_replaced_tips(&self) -> &[(String, String, AtomEntry, Option<String>)] {
        &self.pending_replaced_tips
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
            self.pending_replaced_tips.push((
                hash_value.to_string(),
                range_value.to_string(),
                entry.clone(),
                None,
            ));
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
        keys.sort_by(KeyValue::cmp_page_order);
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

    fn remove_key_metadata(&mut self, hash: &str, range: &str) {
        if let Some(range_map) = self.key_metadata.get_mut(hash) {
            range_map.remove(range);
            if range_map.is_empty() {
                self.key_metadata.remove(hash);
            }
        }
    }

    /// Updates a key WITHOUT signing. Only for ephemeral in-memory operations (rewind).
    /// Legacy `history:`-based rewind no longer runs on the default write path;
    /// this still clears key metadata when the atom changes.
    ///
    /// When the existing entry at `(hash_value, range_value)` already
    /// references the same `atom_uuid`, this is a complete no-op: the
    /// existing `written_at` and `updated_at` are preserved. Even on the
    /// ephemeral rewind path, keeping the
    /// `written_at` invariant on a no-op call avoids surprising downstream
    /// consumers that diff `AtomEntry.written_at` to detect re-emission.
    pub(crate) fn set_atom_uuid_from_values_unsigned(
        &mut self,
        hash_value: String,
        range_value: String,
        atom_uuid: String,
    ) {
        if self.get_atom_uuid(&hash_value, &range_value) == Some(&atom_uuid) {
            return;
        }
        let prev_tip_id = self.archive_current_tip(&hash_value, &range_value, &atom_uuid);
        self.version += 1;
        self.remove_key_metadata(&hash_value, &range_value);
        self.atom_uuids.entry(hash_value).or_default().insert(
            range_value,
            AtomEntry::thin_with_prev(atom_uuid, now_nanos(), String::new(), prev_tip_id),
        );
        self.updated_at = Utc::now();
    }

    /// Force-install a resident tip's LWW identity without last-writer-wins.
    ///
    /// Restore uses this so a later imported apply compares against the
    /// acknowledged T0 clocks, not a zero-clock shell or `now_nanos()`.
    pub(crate) fn overlay_resident_atom_entry(
        &mut self,
        hash_value: String,
        range_value: String,
        entry: AtomEntry,
    ) {
        if let Some(existing) = self.get_atom_entry(&hash_value, &range_value) {
            if existing.atom_uuid == entry.atom_uuid
                && existing.written_at == entry.written_at
                && existing.logical_counter == entry.logical_counter
                && existing.lww_device() == entry.lww_device()
                && existing.mutation_uuid == entry.mutation_uuid
            {
                return;
            }
        }
        self.version += 1;
        self.remove_key_metadata(&hash_value, &range_value);
        self.atom_uuids
            .entry(hash_value)
            .or_default()
            .insert(range_value, entry);
        self.updated_at = Utc::now();
    }
}
