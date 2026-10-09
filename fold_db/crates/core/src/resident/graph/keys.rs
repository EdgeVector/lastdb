//! Key-set completeness, key index and key tombstones.

use super::*;

impl ResidentGraph {
    pub(crate) fn key_set_completeness_for(
        &self,
        molecule_uuid: &str,
    ) -> ResidentKeySetCompleteness {
        if let Some(state) = self
            .key_completeness
            .read()
            .expect("key completeness lock")
            .get(molecule_uuid)
            .cloned()
        {
            return state;
        }
        let inserted = self
            .key_index
            .read()
            .expect("key index lock")
            .get(molecule_uuid)
            .map_or(0, BTreeSet::len);
        let deleted = self
            .key_tombstones
            .read()
            .expect("key tombstone lock")
            .get(molecule_uuid)
            .map_or(0, BTreeMap::len);
        if inserted == 0 && deleted == 0 {
            ResidentKeySetCompleteness::Unknown
        } else {
            ResidentKeySetCompleteness::PartialOverlay { inserted, deleted }
        }
    }

    pub(super) fn demote_complete_key_set(&self, molecule_uuid: &str) {
        let inserted = self
            .key_index
            .read()
            .expect("key index lock")
            .get(molecule_uuid)
            .map_or(0, BTreeSet::len);
        let deleted = self
            .key_tombstones
            .read()
            .expect("key tombstone lock")
            .get(molecule_uuid)
            .map_or(0, BTreeMap::len);
        let mut completeness = self
            .key_completeness
            .write()
            .expect("key completeness lock");
        if matches!(
            completeness.get(molecule_uuid),
            Some(ResidentKeySetCompleteness::Complete { .. })
        ) {
            completeness.insert(
                molecule_uuid.to_string(),
                ResidentKeySetCompleteness::PartialOverlay { inserted, deleted },
            );
            self.metrics.record_key_set_demote();
        }
    }

    pub(super) fn remove_key_index_member(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        demote_if_complete: bool,
    ) -> bool {
        let key = ResidentMoleculeKey::new(hash, range);
        let removed = {
            let mut index = self.key_index.write().expect("key index lock");
            let Some(keys) = index.get_mut(molecule_uuid) else {
                return false;
            };
            let removed = keys.remove(&key);
            if keys.is_empty() {
                index.remove(molecule_uuid);
            }
            removed
        };
        if removed {
            self.invalidate_partition(molecule_uuid, hash);
            self.discharge(&key.dirty_key(molecule_uuid));
            if demote_if_complete {
                self.demote_complete_key_set(molecule_uuid);
            }
        }
        removed
    }

    pub(super) fn remove_key_tombstone(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        demote_if_complete: bool,
    ) -> bool {
        let key = ResidentMoleculeKey::new(hash, range);
        let removed = {
            let mut tombstones = self.key_tombstones.write().expect("key tombstone lock");
            let Some(keys) = tombstones.get_mut(molecule_uuid) else {
                return false;
            };
            let removed = keys.remove(&key).is_some();
            if keys.is_empty() {
                tombstones.remove(molecule_uuid);
            }
            removed
        };
        if removed {
            self.forget_dirty(&key.tombstone_dirty_key(molecule_uuid));
            self.discharge(&key.tombstone_dirty_key(molecule_uuid));
            if demote_if_complete {
                self.demote_complete_key_set(molecule_uuid);
            }
        }
        removed
    }

    pub(super) fn install_key_index_member(&self, molecule_uuid: &str, hash: &str, range: &str) {
        let key = ResidentMoleculeKey::new(hash, range);
        self.key_index
            .write()
            .expect("key index lock")
            .entry(molecule_uuid.to_string())
            .or_default()
            .insert(key.clone());
        self.remove_key_tombstone(molecule_uuid, hash, range, false);
        self.charge_and_enforce(
            key.dirty_key(molecule_uuid),
            key.approx_bytes(molecule_uuid),
        );
    }

    pub fn install_tip(&self, tip: ResidentTip) {
        self.invalidate_partition(&tip.molecule_uuid, &tip.hash);
        let partition = (tip.molecule_uuid.clone(), tip.hash.clone());
        let k = tip_map_key(&tip.molecule_uuid, &tip.hash, &tip.range);
        let charge_key = tip.dirty_key();
        let bytes = tip.approx_bytes();
        self.install_key_index_member(&tip.molecule_uuid, &tip.hash, &tip.range);
        self.tips.write().expect("tips lock").insert(k, tip);
        self.charge_and_enforce(charge_key, bytes);
        self.invalidate_partition(&partition.0, &partition.1);
    }

    pub fn apply_tip(&self, tip: ResidentTip) {
        // Publish the revision before dirty state. An exact persist completion
        // holds the slot read lock while it checks and clears dirty state.
        self.bump_resident_revision(&tip.molecule_uuid, &tip.hash, &tip.range);
        // Dirty BEFORE install — see `apply_schema` for why.
        self.mark_dirty(
            ResidentMoleculeKey::new(tip.hash.clone(), tip.range.clone())
                .dirty_key(&tip.molecule_uuid),
        );
        self.mark_dirty(tip.dirty_key());
        self.install_tip(tip);
    }

    /// True when [`apply_key_tombstone`] marked this slot deleted and the
    /// durable purge has not yet cleared the overlay.
    #[must_use]
    pub fn is_key_tombstoned(&self, molecule_uuid: &str, hash: &str, range: &str) -> bool {
        self.key_tombstones
            .read()
            .expect("key tombstone lock")
            .get(molecule_uuid)
            .is_some_and(|set| set.contains_key(&ResidentMoleculeKey::new(hash, range)))
    }

    /// Schema-level Skip-Delete overlay (API hash/range). Independent of
    /// whether the field cache already has a molecule uuid.
    pub fn next_tombstone_id(&self) -> u64 {
        self.next_tombstone_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn apply_schema_key_tombstone_with_id(
        &self,
        schema: &str,
        hash: &str,
        range: &str,
        tombstone_id: u64,
    ) {
        self.schema_key_tombstones
            .write()
            .expect("schema key tombstone lock")
            .insert(
                (schema.to_string(), hash.to_string(), range.to_string()),
                tombstone_id,
            );
    }

    pub fn apply_schema_key_tombstone(&self, schema: &str, hash: &str, range: &str) {
        let tombstone_id = self.next_tombstone_id();
        self.apply_schema_key_tombstone_with_id(schema, hash, range, tombstone_id);
    }

    #[must_use]
    pub fn is_schema_key_tombstoned(&self, schema: &str, hash: &str, range: &str) -> bool {
        self.schema_key_tombstones
            .read()
            .expect("schema key tombstone lock")
            .contains_key(&(schema.to_string(), hash.to_string(), range.to_string()))
    }

    pub fn forget_schema_key_tombstone(&self, schema: &str, hash: &str, range: &str) {
        self.schema_key_tombstones
            .write()
            .expect("schema key tombstone lock")
            .remove(&(schema.to_string(), hash.to_string(), range.to_string()));
    }

    /// Drop the molecule-slot overlay for a resurrecting Create/Update.
    ///
    /// Schema-level forget is not enough: HashKey reads still hide a key
    /// whose molecule overlay is set, even when the catalog still names the
    /// pre-delete atom.
    pub fn forget_key_tombstone(&self, molecule_uuid: &str, hash: &str, range: &str) {
        self.remove_key_tombstone(molecule_uuid, hash, range, true);
    }

    pub fn forget_schema_key_tombstone_if_current(
        &self,
        schema: &str,
        hash: &str,
        range: &str,
        tombstone_id: u64,
    ) {
        let key = (schema.to_string(), hash.to_string(), range.to_string());
        let mut tombstones = self
            .schema_key_tombstones
            .write()
            .expect("schema key tombstone lock");
        if tombstones.get(&key) == Some(&tombstone_id) {
            tombstones.remove(&key);
        }
    }

    /// Mark a molecule key as explicitly deleted in the resident overlay.
    pub fn apply_key_tombstone(&self, molecule_uuid: &str, hash: &str, range: &str) {
        let tombstone_id = self.next_tombstone_id();
        self.apply_key_tombstone_with_id(molecule_uuid, hash, range, tombstone_id);
    }

    pub fn apply_key_tombstone_with_id(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        tombstone_id: u64,
    ) {
        // Use the same revision-first publication rule as `apply_tip`.
        self.bump_resident_revision(molecule_uuid, hash, range);
        let key = ResidentMoleculeKey::new(hash, range);
        // This delete is visible before its durable erase. Eviction must not
        // discard the overlay and expose the old disk tip in that interval.
        self.mark_dirty(key.tombstone_dirty_key(molecule_uuid));
        let tip_key = DirtyKey::MoleculeTip {
            molecule_uuid: molecule_uuid.to_string(),
            hash: hash.to_string(),
            range: range.to_string(),
        };
        self.remove_key_index_member(molecule_uuid, hash, range, false);
        let removed_tip = self
            .tips
            .write()
            .expect("tips lock")
            .remove(&tip_map_key(molecule_uuid, hash, range))
            .is_some();
        if removed_tip {
            self.forget_dirty(&tip_key);
            self.discharge(&tip_key);
        }
        self.key_tombstones
            .write()
            .expect("key tombstone lock")
            .entry(molecule_uuid.to_string())
            .or_default()
            .insert(key.clone(), tombstone_id);
        self.charge_and_enforce(
            key.tombstone_dirty_key(molecule_uuid),
            key.approx_bytes(molecule_uuid),
        );
        self.invalidate_partition(molecule_uuid, hash);
    }

    pub fn forget_key_tombstone_if_current(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        tombstone_id: u64,
    ) {
        let key = ResidentMoleculeKey::new(hash, range);
        let mut tombstones = self.key_tombstones.write().expect("key tombstone lock");
        let remove = tombstones
            .get(molecule_uuid)
            .and_then(|keys| keys.get(&key))
            == Some(&tombstone_id);
        if !remove {
            return;
        }
        if let Some(keys) = tombstones.get_mut(molecule_uuid) {
            keys.remove(&key);
            if keys.is_empty() {
                tombstones.remove(molecule_uuid);
            }
        }
        self.invalidate_partition(molecule_uuid, hash);
        self.clear_dirty(&key.tombstone_dirty_key(molecule_uuid));
        self.discharge(&key.tombstone_dirty_key(molecule_uuid));
        drop(tombstones);
        self.enforce_budget();
    }

    /// Declare that resident holds the complete key set for this molecule.
    pub fn mark_key_set_complete(&self, molecule_uuid: &str, as_of: u64) {
        self.key_completeness
            .write()
            .expect("key completeness lock")
            .insert(
                molecule_uuid.to_string(),
                ResidentKeySetCompleteness::Complete { as_of },
            );
    }

    /// Ordered resident key-set walk for one molecule.
    ///
    /// `Complete` snapshots may answer enumeration from resident alone.
    /// `PartialOverlay` snapshots are additive/subtractive overlays for a
    /// durable range walk; `Unknown` means resident has no key-set claim.
    pub fn resident_key_set_range(
        &self,
        molecule_uuid: &str,
        start: Option<&ResidentMoleculeKey>,
        end: Option<&ResidentMoleculeKey>,
    ) -> ResidentKeySetSnapshot {
        let start_bound = start.map_or(Bound::Unbounded, Bound::Included);
        let end_bound = end.map_or(Bound::Unbounded, Bound::Excluded);
        let keys = self
            .key_index
            .read()
            .expect("key index lock")
            .get(molecule_uuid)
            .map(|keys| keys.range((start_bound, end_bound)).cloned().collect())
            .unwrap_or_default();
        let tombstones = self
            .key_tombstones
            .read()
            .expect("key tombstone lock")
            .get(molecule_uuid)
            .map(|keys| {
                keys.range((start_bound, end_bound))
                    .map(|(key, _)| key.clone())
                    .collect()
            })
            .unwrap_or_default();
        let completeness = self.key_set_completeness_for(molecule_uuid);
        match completeness {
            ResidentKeySetCompleteness::Complete { .. } => self.metrics.record_key_set_hit(),
            // Split, because the two non-hit outcomes mean opposite things to
            // anyone reading the gauge. `PartialOverlay` did useful work — the
            // read path applies it. `Unknown` means resident had nothing to
            // say, and a hit rate pinned at zero against a large `unknown` is
            // a missing producer rather than a cache that needs tuning.
            ResidentKeySetCompleteness::PartialOverlay { .. } => {
                self.metrics.record_key_set_overlay();
            }
            ResidentKeySetCompleteness::Unknown => self.metrics.record_key_set_unknown(),
        }
        ResidentKeySetSnapshot {
            molecule_uuid: molecule_uuid.to_string(),
            completeness,
            keys,
            tombstones,
        }
    }
}
