//! Tip admission, range reads and hold/release.

use super::*;

impl LogicalResidentSet {
    /// Insert one owned tip, or replace the value of an existing tip.
    /// A new tip starts with hold 1. A second admit on the same key takes
    /// another hold. The tip admits clean.
    pub fn admit_tip(&mut self, molecule: MoleculeId, hash: String, range: String, tip: Tip) {
        let key = TipKey {
            molecule,
            hash,
            range,
        };
        if self.tips.contains_key(&key) {
            self.replace_tip(&key, tip);
            return;
        }
        self.insert_new_tip(key, tip);
    }

    pub fn tip(&self, molecule: MoleculeId, hash: &str, range: &str) -> Option<&Tip> {
        self.tips
            .get(&tip_key(molecule, hash, range))
            .map(|entry| &entry.value)
    }

    pub fn hold_count(&self, molecule: MoleculeId, hash: &str, range: &str) -> u32 {
        self.tips
            .get(&tip_key(molecule, hash, range))
            .map_or(0, |entry| entry.hold)
    }

    pub fn tip_token(
        &self,
        molecule: MoleculeId,
        hash: &str,
        range: &str,
    ) -> Option<DurabilityToken> {
        self.tips
            .get(&tip_key(molecule, hash, range))
            .and_then(|entry| entry.token)
    }

    pub fn is_dirty(&self, molecule: MoleculeId, hash: &str, range: &str) -> bool {
        self.tips
            .get(&tip_key(molecule, hash, range))
            .is_some_and(|entry| entry.token.is_some())
    }

    /// Completeness of one hash's ordered view. `Absent` when no view exists.
    pub fn hash_completeness(&self, molecule: MoleculeId, hash: &str) -> HashCompleteness {
        self.hash_views
            .get(&(molecule, hash.to_string()))
            .map_or(HashCompleteness::Absent, |view| view.completeness)
    }

    /// Ordered live ranges for one hash. Empty when the view is absent.
    pub fn hash_order(&self, molecule: MoleculeId, hash: &str) -> Vec<String> {
        self.hash_views
            .get(&(molecule, hash.to_string()))
            .map_or_else(Vec::new, |view| view.order.keys().cloned().collect())
    }

    /// Read the ordered page. Does not fill. Does not answer from `Partial`.
    pub fn range_page(
        &self,
        molecule: MoleculeId,
        hash: &str,
        start: Bound<&str>,
        end: Bound<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Tip)>, RangeNotResident> {
        let view = self
            .hash_views
            .get(&(molecule, hash.to_string()))
            .ok_or(RangeNotResident)?;
        if !matches!(view.completeness, HashCompleteness::Complete { .. }) {
            return Err(RangeNotResident);
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for range in view.order.range::<str, _>((start, end)).map(|(r, _)| r) {
            if self.has_tombstone(molecule, hash, range) {
                continue;
            }
            let Some(tip) = self.tip(molecule, hash, range).cloned() else {
                continue;
            };
            out.push((range.clone(), tip));
            if out.len() == limit {
                break;
            }
        }
        Ok(out)
    }

    /// Resident tips for one hash. Used as merge overlay before admission.
    pub fn tips_for_hash(&self, molecule: MoleculeId, hash: &str) -> Vec<(String, Tip)> {
        self.tips
            .iter()
            .filter(|(key, _)| key.molecule == molecule && key.hash == hash)
            .map(|(key, entry)| (key.range.clone(), entry.value.clone()))
            .collect()
    }

    /// Dirty tombstone ranges for one hash. Used as merge overlay.
    pub fn tombstone_ranges_for_hash(&self, molecule: MoleculeId, hash: &str) -> Vec<String> {
        self.tombstones
            .keys()
            .filter(|key| key.molecule == molecule && key.hash == hash)
            .map(|key| key.range.clone())
            .collect()
    }

    /// Mark this hash complete after a fill that applied dirty records.
    pub fn mark_hash_complete(
        &mut self,
        molecule: MoleculeId,
        hash: &str,
        live_ranges: impl IntoIterator<Item = String>,
    ) {
        let order: BTreeMap<String, ()> =
            live_ranges.into_iter().map(|range| (range, ())).collect();
        self.hash_views.insert(
            (molecule, hash.to_string()),
            HashView {
                completeness: HashCompleteness::Complete { as_of: self.clock },
                order,
            },
        );
    }

    /// Release one call hold and keep a clean record warm.
    ///
    /// Range fill uses this so a fetched tip stays after the call returns.
    /// [`Self::release_tip`] also keeps a clean hold-0 tip. Over
    /// [`RESIDENT_KEY_CAP`], LRU purge may then drop an unheld clean record.
    pub fn release_tip_hold(&mut self, molecule: MoleculeId, hash: &str, range: &str) -> bool {
        let hold_transition = {
            let Some(entry) = self.tips.get_mut(&tip_key(molecule, hash, range)) else {
                return false;
            };
            let before = entry.hold;
            entry.release_hold();
            (before, entry.hold)
        };
        self.note_hold_transition(hold_transition.0, hold_transition.1);
        self.purge_over_cap();
        true
    }

    /// A request hold. This is an integer on the entry. It is not
    /// `slot_readers`.
    pub fn hold_tip(&mut self, molecule: MoleculeId, hash: &str, range: &str) -> bool {
        let hold_transition = {
            let Some(entry) = self.tips.get_mut(&tip_key(molecule, hash, range)) else {
                return false;
            };
            let before = entry.hold;
            entry.take_hold();
            (before, entry.hold)
        };
        self.note_hold_transition(hold_transition.0, hold_transition.1);
        self.touch(ResidentKey::MoleculeTip {
            molecule,
            hash: hash.to_string(),
            range: range.to_string(),
        });
        self.publish_occupancy();
        true
    }

    /// Release one call hold. A clean hold-0 tip stays warm.
    /// A just-covered write stays warm. Over [`RESIDENT_KEY_CAP`], LRU purge
    /// may then drop an unheld clean record.
    pub fn release_tip(&mut self, molecule: MoleculeId, hash: &str, range: &str) -> bool {
        let key = tip_key(molecule, hash, range);
        let (before, hold, token) = {
            let Some(entry) = self.tips.get_mut(&key) else {
                return false;
            };
            let before = entry.hold;
            entry.release_hold();
            (before, entry.hold, entry.token)
        };
        self.note_hold_transition(before, hold);
        if hold == 0 && self.token_is_covered(token) {
            let was_dirty = if let Some(entry) = self.tips.get_mut(&key) {
                let was_dirty = entry.token.is_some();
                entry.token = None;
                was_dirty
            } else {
                false
            };
            self.note_dirty_transition(was_dirty, false);
        }
        self.purge_over_cap();
        true
    }

    pub fn release_tombstone(&mut self, molecule: MoleculeId, hash: &str, range: &str) -> bool {
        let key = tip_key(molecule, hash, range);
        let (before, hold, token) = {
            let Some(entry) = self.tombstones.get_mut(&key) else {
                return false;
            };
            let before = entry.hold;
            entry.release_hold();
            (before, entry.hold, entry.token)
        };
        self.note_hold_transition(before, hold);
        if hold == 0 && self.token_is_covered(token) {
            let was_dirty = if let Some(entry) = self.tombstones.get_mut(&key) {
                let was_dirty = entry.token.is_some();
                entry.token = None;
                was_dirty
            } else {
                false
            };
            self.note_dirty_transition(was_dirty, false);
        }
        self.purge_over_cap();
        true
    }
}
