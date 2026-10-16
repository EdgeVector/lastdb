//! Tip ownership and atom references share one lock.

use super::*;

#[derive(Debug, Default)]
pub(super) struct TipMap {
    by_key: HashMap<String, ResidentTip>,
    atom_references: HashMap<String, usize>,
    storage_aliases: HashMap<String, HashSet<String>>,
    aliases_by_key: HashMap<String, Vec<String>>,
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
        self.insert_with_storage_aliases(key, tip, Vec::new())
    }

    pub(super) fn has_storage_alias(&self, storage_key: &str) -> bool {
        self.storage_aliases.contains_key(storage_key)
    }

    pub(super) fn storage_alias_bytes(key: &str, aliases: &[String]) -> u64 {
        aliases
            .iter()
            .map(|alias| (alias.len() * 2 + key.len() + 256) as u64)
            .sum()
    }

    pub(super) fn insert_with_storage_aliases(
        &mut self,
        key: String,
        tip: ResidentTip,
        aliases: Vec<String>,
    ) -> Option<ResidentTip> {
        let previous = self.remove(&key);
        let atom_uuid = tip.atom_uuid.clone();
        for alias in &aliases {
            self.storage_aliases
                .entry(alias.clone())
                .or_default()
                .insert(key.clone());
        }
        if !aliases.is_empty() {
            self.aliases_by_key.insert(key.clone(), aliases);
        }
        self.by_key.insert(key, tip);
        *self.atom_references.entry(atom_uuid).or_default() += 1;
        previous
    }

    pub(super) fn remove(&mut self, key: &str) -> Option<ResidentTip> {
        let tip = self.by_key.remove(key)?;
        self.unlink_atom(&tip.atom_uuid);
        for alias in self.aliases_by_key.remove(key).unwrap_or_default() {
            let owners = self
                .storage_aliases
                .get_mut(&alias)
                .expect("tip storage alias");
            owners.remove(key);
            if owners.is_empty() {
                self.storage_aliases.remove(&alias);
            }
        }
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
