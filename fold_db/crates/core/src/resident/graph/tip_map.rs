//! Tip ownership and atom references share one lock.

use super::*;

#[derive(Debug, Default)]
pub(super) struct TipMap {
    by_key: HashMap<String, ResidentTip>,
    atom_references: HashMap<String, usize>,
}

impl TipMap {
    pub(super) fn get(&self, key: &str) -> Option<&ResidentTip> {
        self.by_key.get(key)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &ResidentTip> {
        self.by_key.values()
    }

    pub(super) fn references_atom(&self, uuid: &str) -> bool {
        self.atom_references.contains_key(uuid)
    }

    pub(super) fn insert(&mut self, key: String, tip: ResidentTip) -> Option<ResidentTip> {
        let atom_uuid = tip.atom_uuid.clone();
        let previous = self.by_key.insert(key, tip);
        if let Some(old) = &previous {
            self.unlink_atom(&old.atom_uuid);
        }
        *self.atom_references.entry(atom_uuid).or_default() += 1;
        previous
    }

    pub(super) fn remove(&mut self, key: &str) -> Option<ResidentTip> {
        let tip = self.by_key.remove(key)?;
        self.unlink_atom(&tip.atom_uuid);
        Some(tip)
    }

    fn unlink_atom(&mut self, uuid: &str) {
        let references = self
            .atom_references
            .get_mut(uuid)
            .expect("tip atom reference");
        *references -= 1;
        if *references == 0 {
            self.atom_references.remove(uuid);
        }
    }
}
