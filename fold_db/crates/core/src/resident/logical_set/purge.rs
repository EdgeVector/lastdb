//! Least-recently-used purge and the private drop helpers.

use super::*;

impl LogicalResidentSet {
    /// Drop least-recently-used clean unheld records until the used-record
    /// count is at most [`resident_key_cap`]. A skip of a held or dirty
    /// record does not change that record's recency tick.
    pub(super) fn purge_over_cap(&mut self) {
        let mut purged = 0u64;
        let mut stalled_over = false;
        let cap = resident_key_cap();
        while self.logical_record_count() > cap {
            let Some(key) = self.oldest_purgeable_key() else {
                stalled_over = true;
                break;
            };
            self.drop_used_record(key);
            purged = purged.saturating_add(1);
        }
        if let Some(metrics) = &self.metrics {
            if purged > 0 {
                metrics.record_purged_keys(purged);
            }
            let over = self.logical_record_count().saturating_sub(cap) as u64;
            // A stall is a pass that wanted to purge and could not: every
            // remaining record is held or dirty.
            metrics.record_purge_pass(purged, if stalled_over { over } else { 0 });
        }
        self.publish_occupancy();
    }

    /// Cold-end used record that can leave. Skips a held or dirty record
    /// without walking the rest of the set when the cold end can leave.
    pub(super) fn oldest_purgeable_key(&self) -> Option<ResidentKey> {
        self.order
            .values()
            .find(|key| self.is_purgeable(key))
            .cloned()
    }

    pub(super) fn is_purgeable(&self, key: &ResidentKey) -> bool {
        match key {
            ResidentKey::MoleculeTip {
                molecule,
                hash,
                range,
            } => self
                .tips
                .get(&tip_key(*molecule, hash, range))
                .is_some_and(Held::can_leave),
            ResidentKey::Schema(name) => self.schemas.get(name).is_some_and(Held::can_leave),
            ResidentKey::Field { schema, field } => self
                .fields
                .get(&field_key(schema, field))
                .is_some_and(Held::can_leave),
            ResidentKey::Atom(id) => {
                // An atom named by a resident tip leaves with that tip
                // (`unlink_atom`). Independent purge would drop the body
                // while the tip stayed warm.
                self.atom_bodies.contains_key(id)
                    && self.atom_tips.get(id).is_none_or(HashSet::is_empty)
            }
            ResidentKey::Record { collection, id } => self.records.contains_key(&RecordKey {
                collection: collection.clone(),
                id: id.clone(),
            }),
            ResidentKey::Tombstone {
                molecule,
                hash,
                range,
            } => self
                .tombstones
                .get(&tip_key(*molecule, hash, range))
                .is_some_and(Held::can_leave),
            ResidentKey::Protein(_) => false,
        }
    }

    pub(super) fn drop_used_record(&mut self, key: ResidentKey) {
        match key {
            ResidentKey::MoleculeTip {
                molecule,
                hash,
                range,
            } => self.drop_tip(&tip_key(molecule, &hash, &range)),
            ResidentKey::Atom(id) => {
                self.atom_bodies.remove(&id);
                self.forget_key(&ResidentKey::Atom(id));
            }
            ResidentKey::Schema(name) => {
                if let Some(entry) = self.schemas.remove(&name) {
                    self.note_used_remove(entry.hold, entry.token.is_some());
                }
                self.forget_key(&ResidentKey::Schema(name));
            }
            ResidentKey::Field { schema, field } => {
                if let Some(entry) = self.fields.remove(&field_key(&schema, &field)) {
                    self.note_used_remove(entry.hold, entry.token.is_some());
                }
                self.forget_key(&ResidentKey::Field { schema, field });
            }
            ResidentKey::Record { collection, id } => {
                let key = RecordKey { collection, id };
                self.records.remove(&key);
                self.forget_key(&ResidentKey::Record {
                    collection: key.collection,
                    id: key.id,
                });
            }
            ResidentKey::Tombstone {
                molecule,
                hash,
                range,
            } => self.clear_tombstone(&tip_key(molecule, &hash, &range)),
            ResidentKey::Protein(_) => {}
        }
    }

    pub(super) fn take_tip_for_delete(&mut self, key: &TipKey) -> Option<Tip> {
        let entry = self.tips.remove(key)?;
        self.note_used_remove(entry.hold, entry.token.is_some());
        self.forget_key(&resident_tip_key(key));
        let atom_body = self.atom_bodies.get(&entry.value.atom).cloned();
        let tip_body = self.tip_bodies.remove(key);
        self.undo_snapshots.insert(
            key.clone(),
            UndoDeleteSnapshot {
                hold: entry.hold,
                token: entry.token,
                atom_body,
                tip_body,
            },
        );
        self.unlink_atom(&entry.value.atom, key);
        self.note_tip_removed(key.molecule);
        Some(entry.value)
    }

    pub(super) fn restore_deleted_tip(
        &mut self,
        key: TipKey,
        tip: Tip,
        snapshot: UndoDeleteSnapshot,
    ) {
        // Link before the body lands so a cap purge cannot drop an
        // unlinked restored atom. Purge runs only after the tip is in.
        self.link_atom(&tip.atom, &key);
        if let Some(body) = snapshot.atom_body {
            self.store_atom_body(tip.atom.clone(), body);
        }
        if let Some(body) = snapshot.tip_body {
            self.tip_bodies.insert(key.clone(), body);
        }
        *self.molecule_tips.entry(key.molecule).or_default() += 1;
        self.touch(resident_tip_key(&key));
        self.note_live_tip(&key);
        self.note_used_insert(snapshot.hold, snapshot.token.is_some());
        self.tips.insert(
            key,
            Held {
                value: tip,
                hold: snapshot.hold,
                token: snapshot.token,
            },
        );
        self.purge_over_cap();
    }

    pub(super) fn insert_new_tip(&mut self, key: TipKey, tip: Tip) {
        self.clear_tombstone(&key);
        self.link_atom(&tip.atom, &key);
        *self.molecule_tips.entry(key.molecule).or_default() += 1;
        self.touch(resident_tip_key(&key));
        self.note_live_tip(&key);
        self.tips.insert(key, Held::admit(tip));
        self.note_used_insert(1, false);
        self.purge_over_cap();
    }

    pub(super) fn replace_tip(&mut self, key: &TipKey, tip: Tip) {
        self.clear_tombstone(key);
        let (old_atom, hold, token) = {
            let existing = self.tips.get(key).expect("held tip");
            (existing.value.atom.clone(), existing.hold, existing.token)
        };
        let same_atom = old_atom == tip.atom;
        if !same_atom {
            self.unlink_atom(&old_atom, key);
        }
        let atom = tip.atom.clone();
        let new_hold = hold.saturating_add(1);
        self.note_hold_transition(hold, new_hold);
        self.tips.insert(
            key.clone(),
            Held {
                value: tip,
                hold: new_hold,
                token,
            },
        );
        if !same_atom {
            self.link_atom(&atom, key);
        }
        self.touch(resident_tip_key(key));
        self.publish_occupancy();
    }

    pub(super) fn tombstone_resident_key(key: &TipKey) -> ResidentKey {
        ResidentKey::Tombstone {
            molecule: key.molecule,
            hash: key.hash.clone(),
            range: key.range.clone(),
        }
    }

    /// Remove the counted tombstone. Keeps the undo snapshot for
    /// [`Self::undo_delete`].
    pub(super) fn drop_tombstone_record(&mut self, key: &TipKey) {
        let removed = if let Some(entry) = self.tombstones.remove(key) {
            self.note_used_remove(entry.hold, entry.token.is_some());
            true
        } else {
            false
        };
        self.forget_key(&Self::tombstone_resident_key(key));
        if removed {
            self.publish_occupancy();
        }
    }

    /// A later put of the same key supersedes a delete overlay.
    pub(super) fn clear_tombstone(&mut self, key: &TipKey) {
        self.drop_tombstone_record(key);
        self.undo_snapshots.remove(key);
    }

    pub(super) fn drop_tip(&mut self, key: &TipKey) {
        let Some(entry) = self.tips.remove(key) else {
            return;
        };
        self.note_used_remove(entry.hold, entry.token.is_some());
        self.tip_bodies.remove(key);
        self.forget_key(&resident_tip_key(key));
        self.unlink_atom(&entry.value.atom, key);
        if self.remove_live_tip_from_order(key) {
            self.demote_hash(key.molecule, &key.hash);
        }
        self.note_tip_removed(key.molecule);
    }

    pub(super) fn note_live_tip(&mut self, key: &TipKey) {
        let view = self
            .hash_views
            .entry((key.molecule, key.hash.clone()))
            .or_default();
        view.order.insert(key.range.clone(), ());
        if !matches!(view.completeness, HashCompleteness::Complete { .. }) {
            view.completeness = HashCompleteness::Partial;
        }
    }

    /// Hide a deleted range. A delete does not demote `Complete`.
    pub(super) fn hide_range_from_order(&mut self, molecule: MoleculeId, hash: &str, range: &str) {
        let view_key = (molecule, hash.to_string());
        let should_remove = {
            let Some(view) = self.hash_views.get_mut(&view_key) else {
                return;
            };
            view.order.remove(range);
            view.order.is_empty() && !matches!(view.completeness, HashCompleteness::Complete { .. })
        };
        if should_remove {
            self.hash_views.remove(&view_key);
        }
    }

    pub(super) fn remove_live_tip_from_order(&mut self, key: &TipKey) -> bool {
        let view_key = (key.molecule, key.hash.clone());
        let Some(view) = self.hash_views.get_mut(&view_key) else {
            return false;
        };
        view.order.remove(&key.range).is_some()
    }

    pub(super) fn demote_hash(&mut self, molecule: MoleculeId, hash: &str) {
        let view_key = (molecule, hash.to_string());
        let should_remove = {
            let Some(view) = self.hash_views.get_mut(&view_key) else {
                return;
            };
            if matches!(view.completeness, HashCompleteness::Complete { .. }) {
                view.completeness = HashCompleteness::Partial;
            }
            view.order.is_empty()
        };
        if should_remove {
            self.hash_views.remove(&view_key);
        }
    }

    pub(super) fn link_atom(&mut self, atom: &AtomId, key: &TipKey) {
        self.atom_tips
            .entry(atom.clone())
            .or_default()
            .insert(key.clone());
    }

    pub(super) fn unlink_atom(&mut self, atom: &AtomId, key: &TipKey) {
        let empty = self.atom_tips.get_mut(atom).is_some_and(|tips| {
            tips.remove(key);
            tips.is_empty()
        });
        if empty {
            self.atom_tips.remove(atom);
            self.atom_bodies.remove(atom);
            self.forget_key(&ResidentKey::Atom(atom.clone()));
        }
    }

    pub(super) fn forget_key(&mut self, key: &ResidentKey) {
        if let Some(tick) = self.recency.remove(key) {
            self.order.remove(&tick);
        }
    }

    pub(super) fn note_tip_removed(&mut self, molecule: MoleculeId) {
        let Some(count) = self.molecule_tips.get_mut(&molecule) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count > 0 {
            return;
        }
        self.molecule_tips.remove(&molecule);
        self.drop_proteins_without_a_member(molecule);
    }

    pub(super) fn drop_proteins_without_a_member(&mut self, molecule: MoleculeId) {
        let Some(protein_ids) = self.molecule_proteins.get(&molecule).cloned() else {
            return;
        };
        for protein_id in protein_ids {
            let still = self.proteins.get(&protein_id).is_some_and(|entry| {
                entry
                    .value
                    .iter()
                    .any(|member| self.molecule_tips.get(member).copied().unwrap_or(0) > 0)
            });
            if !still {
                self.remove_protein(&protein_id);
            }
        }
    }

    pub(super) fn remove_protein(&mut self, protein_id: &str) {
        let Some(entry) = self.proteins.remove(protein_id) else {
            return;
        };
        for member in entry.value {
            let empty = self.molecule_proteins.get_mut(&member).is_some_and(|ids| {
                ids.remove(protein_id);
                ids.is_empty()
            });
            if empty {
                self.molecule_proteins.remove(&member);
            }
        }
    }
}
