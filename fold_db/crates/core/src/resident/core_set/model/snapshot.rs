//! Read only the retained typed version, never mutable current-model state.

use super::*;
use std::any::Any;

impl CoreModelSnapshot {
    /// Match exact body coordinates in one pass; UUID identity alone is unsafe.
    pub(crate) fn atoms_for_sources(
        &self,
        requested: &[(String, String, String)],
    ) -> Vec<Option<CoreWarmSelection>> {
        let mut positions: HashMap<_, Vec<_>> = HashMap::new();
        for (position, (collection, key, uuid)) in requested.iter().enumerate() {
            positions
                .entry((collection.as_str(), key.as_str()))
                .or_default()
                .push((position, uuid.as_str()));
        }
        let mut selected: Vec<Option<CoreWarmSelection>> = vec![None; requested.len()];
        let mut conflicting = HashSet::new();
        for molecule in self.root.molecules.values() {
            for slot in molecule.slots.values() {
                let Some(value) = &slot.value else { continue };
                for (collection, key) in &slot.sources {
                    for &(position, uuid) in positions
                        .get(&(collection.as_str(), key.as_str()))
                        .into_iter()
                        .flatten()
                    {
                        if value.atom.uuid() != uuid || conflicting.contains(&position) {
                            continue;
                        }
                        if selected[position]
                            .as_ref()
                            .is_some_and(|previous| previous.atom.as_ref() != value.atom.as_ref())
                        {
                            selected[position] = None;
                            conflicting.insert(position);
                        } else {
                            selected[position] = slot.lookup().into_value();
                        }
                    }
                }
            }
        }
        selected
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.root.molecules.is_empty() && self.root.definitions.is_empty()
    }

    pub(crate) fn has_changes(&self, scope: Option<&str>, molecule: &str) -> bool {
        self.molecule(scope, molecule)
            .is_some_and(|model| !model.slots.is_empty())
    }

    pub(crate) fn lookup(&self, path: &CorePath) -> CoreWarmLookup {
        self.lookup_key(
            path.scope.as_deref(),
            &path.molecule,
            &path.hash,
            &path.range,
        )
    }

    pub(crate) fn lookup_key(
        &self,
        scope: Option<&str>,
        molecule: &str,
        hash: &str,
        range: &str,
    ) -> CoreWarmLookup {
        self.molecule(scope, molecule)
            .and_then(|model| model.slots.get(&(hash.to_owned(), range.to_owned())))
            .map_or_else(CoreWarmLookup::unknown, |slot| slot.lookup())
    }

    /// This is the existing ordered logical molecule, with dirty keys only.
    pub(crate) fn with_molecule<T>(
        &self,
        scope: Option<&str>,
        molecule: &str,
        inspect: impl FnOnce(&MoleculeHashRange) -> T,
    ) -> Option<T> {
        self.molecule(scope, molecule)
            .map(|model| inspect(&model.model))
    }

    /// Ordered membership deltas include absences omitted by molecule methods.
    pub(crate) fn changes(
        &self,
        scope: Option<&str>,
        molecule: &str,
    ) -> Vec<(String, String, bool)> {
        self.molecule(scope, molecule)
            .map_or_else(Vec::new, |model| {
                model
                    .slots
                    .iter()
                    .map(|((hash, range), slot)| {
                        (hash.clone(), range.clone(), slot.value.is_none())
                    })
                    .collect()
            })
    }

    pub(crate) fn source_coordinates(&self, path: &CorePath) -> Vec<(String, String)> {
        self.molecule(path.scope.as_deref(), &path.molecule)
            .and_then(|model| model.slots.get(&(path.hash.clone(), path.range.clone())))
            .map_or_else(Vec::new, |slot| slot.sources.clone())
    }

    pub(crate) fn memory_get<T: Any + Send + Sync>(
        &self,
        owner: usize,
        name: &str,
    ) -> Option<Arc<CoreMemoryValue<T>>> {
        self.root
            .definitions
            .get(&(owner, name.to_owned()))?
            .value
            .clone()
            .downcast()
            .ok()
    }

    fn molecule(&self, scope: Option<&str>, molecule: &str) -> Option<&ModelMolecule> {
        self.root
            .molecules
            .get(&MoleculeKey::new(scope, molecule))
            .map(Arc::as_ref)
    }
}
