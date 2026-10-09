//! Tip and atom install, resolve and rehydrate.

use super::*;

impl ResidentGraph {
    pub fn resolve_tip(
        &self,
        molecule_uuid: &str,
        hash: &str,
        range: &str,
    ) -> Option<ResolveOutcome<ResidentTip>> {
        let k = tip_map_key(molecule_uuid, hash, range);
        let hit = {
            let map = self.tips.read().expect("tips lock");
            map.get(&k).map(|t| {
                self.metrics.record_hit(ResidentKind::Molecule);
                ResolveOutcome::hit(t.clone())
            })
        };
        if let Some(ref outcome) = hit {
            self.note_touch(&outcome.value.dirty_key());
            self.note_touch(
                &ResidentMoleculeKey::new(outcome.value.hash.clone(), outcome.value.range.clone())
                    .dirty_key(&outcome.value.molecule_uuid),
            );
        }
        hit
    }

    /// Install tip from disk (or after a store write).
    ///
    /// **Overwrite rule:** if the tip key is already resident with a *different*
    /// `atom_uuid`, replace it (rehydrate). Same `atom_uuid` → hit. Never keep a
    /// stale tip forever after a write/rehydrate.
    pub fn rehydrate_tip(&self, tip: ResidentTip) -> ResolveOutcome<ResidentTip> {
        self.rehydrate_tip_at(tip, None)
            .expect("an unconditional rehydrate always returns a tip")
    }

    /// Install a disk tip only when `observed_revisions` are still current.
    ///
    /// `None` keeps the legacy overwrite rule (tests / persist-plan round-trip).
    /// Cold rehydrate passes the revision observed before the disk read.
    pub(crate) fn rehydrate_tip_at(
        &self,
        tip: ResidentTip,
        observed_revisions: Option<(u64, u64)>,
    ) -> Option<ResolveOutcome<ResidentTip>> {
        let k = tip_map_key(&tip.molecule_uuid, &tip.hash, &tip.range);
        let charge_key = tip.dirty_key();

        // Lock order: slots then dirty then tips (same as apply_tip).
        {
            let slots = self.slots.write().expect("resident slot lock");
            let current = slots.get(&k).copied().unwrap_or_default();
            if let Some(observed) = observed_revisions {
                if (current.resident_revision, current.durable_revision) != observed {
                    self.metrics.record_stale_rehydrate_rejected();
                    drop(slots);
                    if let Some(existing) =
                        self.resolve_tip(&tip.molecule_uuid, &tip.hash, &tip.range)
                    {
                        return Some(existing);
                    }
                    return None;
                }
            }
        }

        // H1: never install a durable tip over a dirty one. An acked T0
        // write must not be clobbered by a later cold rehydrate of the old
        // LastStore tip.
        if self.is_dirty(&charge_key) {
            if let Some(existing) = self.resolve_tip(&tip.molecule_uuid, &tip.hash, &tip.range) {
                return Some(existing);
            }
        }

        let bytes = tip.approx_bytes();
        {
            let mut slots = self.slots.write().expect("resident slot lock");
            let current = slots.get(&k).copied().unwrap_or_default();
            if let Some(observed) = observed_revisions {
                if (current.resident_revision, current.durable_revision) != observed {
                    self.metrics.record_stale_rehydrate_rejected();
                    drop(slots);
                    if let Some(existing) =
                        self.resolve_tip(&tip.molecule_uuid, &tip.hash, &tip.range)
                    {
                        return Some(existing);
                    }
                    return None;
                }
            }
            let mut map = self.tips.write().expect("tips lock");
            if let Some(existing) = map.get(&k) {
                if existing == &tip {
                    self.metrics.record_hit(ResidentKind::Molecule);
                    let outcome = ResolveOutcome::hit(existing.clone());
                    let control = slots.entry(k).or_default();
                    control.state = ResidentSlotState::Ready;
                    drop(map);
                    drop(slots);
                    self.note_touch(&charge_key);
                    return Some(outcome);
                }
            }
            map.insert(k.clone(), tip.clone());
            let control = slots.entry(k).or_default();
            control.state = ResidentSlotState::Ready;
        }
        self.install_key_index_member(&tip.molecule_uuid, &tip.hash, &tip.range);
        self.metrics.record_rehydrate(ResidentKind::Molecule);
        self.charge_and_enforce(charge_key, bytes);
        Some(ResolveOutcome::rehydrated(tip))
    }

    /// After a successful durable tip write: install tip without dirty (store already has it).
    pub fn publish_tip_after_store(&self, tip: ResidentTip) {
        self.install_tip(tip);
    }

    pub fn install_atom(&self, atom: ResidentAtom) {
        let charge_key = atom.dirty_key();
        let bytes = atom.approx_bytes();
        self.atoms
            .write()
            .expect("atoms lock")
            .insert(atom.uuid.clone(), atom);
        self.charge_and_enforce(charge_key, bytes);
    }

    pub fn apply_atom(&self, atom: ResidentAtom) {
        // Dirty BEFORE install — see `apply_schema` for why.
        self.mark_dirty(atom.dirty_key());
        self.install_atom(atom);
    }

    pub fn resolve_atom(&self, uuid: &str) -> Option<ResolveOutcome<ResidentAtom>> {
        let hit = self.atoms.read().expect("atoms lock").get(uuid).map(|a| {
            self.metrics.record_hit(ResidentKind::Atom);
            ResolveOutcome::hit(a.clone())
        });
        if hit.is_some() {
            self.note_touch(&DirtyKey::Atom(uuid.to_string()));
        }
        hit
    }

    /// Install atom from disk. Content-addressed: same uuid is same content → hit;
    /// missing uuid → rehydrate. (Tip refresh after write points at the *new* uuid.)
    pub fn rehydrate_atom(&self, atom: ResidentAtom) -> ResolveOutcome<ResidentAtom> {
        let charge_key = atom.dirty_key();
        let bytes = atom.approx_bytes();
        {
            let mut map = self.atoms.write().expect("atoms lock");
            if let Some(existing) = map.get(&atom.uuid) {
                // Same uuid is content-addressed identity; still refresh content if
                // tests/stores ever rewrite (should not happen for real atoms).
                if existing.content == atom.content {
                    self.metrics.record_hit(ResidentKind::Atom);
                    let outcome = ResolveOutcome::hit(existing.clone());
                    drop(map);
                    self.note_touch(&charge_key);
                    return outcome;
                }
            }
            map.insert(atom.uuid.clone(), atom.clone());
        }
        self.metrics.record_rehydrate(ResidentKind::Atom);
        self.charge_and_enforce(charge_key, bytes);
        ResolveOutcome::rehydrated(atom)
    }
}
