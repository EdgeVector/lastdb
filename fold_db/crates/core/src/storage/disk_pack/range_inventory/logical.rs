use super::catalog::Molecule;
use super::{invalid, StorageResult};
use crate::atom::delete_barrier::DeleteBarrier;
use crate::db_operations::atom_store::{
    AtomStore, MoleculeGenerationDelete, MoleculeGenerationPointer, MoleculeGenerationSlot,
    PerKeyRecord,
};
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct LogicalSlots {
    scopes: BTreeMap<(String, String), Slots>,
}

#[derive(Default)]
struct Slots {
    pointer: Option<MoleculeGenerationPointer>,
    live: BTreeMap<String, PerKeyRecord>,
    bases: BTreeMap<String, BTreeMap<String, MoleculeGenerationSlot>>,
    deletes: BTreeMap<String, MoleculeGenerationDelete>,
    barriers: BTreeMap<String, DeleteBarrier>,
}

impl LogicalSlots {
    pub(super) fn observe(
        &mut self,
        scope: &str,
        molecule: &str,
        family: &str,
        key: &str,
        generation: Option<&str>,
        value: &[u8],
    ) -> StorageResult<()> {
        let state = self
            .scopes
            .entry((scope.into(), molecule.into()))
            .or_default();
        match family {
            "mk" => {
                state.live.insert(key.into(), decode(value)?);
            }
            "mgp" => {
                let pointer: MoleculeGenerationPointer = decode(value)?;
                if pointer.generation.is_empty()
                    || pointer.generation.contains([':', '\0'])
                    || pointer
                        .previous_generation
                        .as_ref()
                        .is_some_and(|g| g.is_empty() || g.contains([':', '\0']))
                {
                    return Err(invalid("a generation pointer is invalid"));
                }
                state.pointer = Some(pointer);
            }
            "mgr" => {
                state
                    .bases
                    .entry(
                        generation
                            .ok_or_else(|| invalid("generation identity is absent"))?
                            .into(),
                    )
                    .or_default()
                    .insert(key.into(), decode(value)?);
            }
            "mgd" => {
                state.deletes.insert(key.into(), decode(value)?);
            }
            "rdel" => {
                state.barriers.insert(key.into(), decode(value)?);
            }
            _ => return Err(invalid("unknown logical slot family")),
        }
        Ok(())
    }

    pub(super) fn count_winners(
        &mut self,
        molecules: &mut BTreeMap<String, Molecule>,
    ) -> StorageResult<()> {
        for ((_, molecule), mut state) in std::mem::take(&mut self.scopes) {
            let mut base = state
                .pointer
                .as_ref()
                .and_then(|p| state.bases.remove(&p.generation))
                .unwrap_or_default();
            let keys: BTreeSet<_> = state.live.keys().chain(base.keys()).cloned().collect();
            let report = &mut molecules
                .get_mut(&molecule)
                .ok_or_else(|| invalid("a logical slot has no molecule inventory"))?
                .report;
            for key in keys {
                let record =
                    AtomStore::merge_generation_slot(state.live.remove(&key), base.remove(&key));
                let record = if state.pointer.is_some() {
                    AtomStore::apply_generation_delete(record, state.deletes.remove(&key))
                } else {
                    record
                };
                if record.as_ref().is_some_and(|record| {
                    state
                        .barriers
                        .get(&key)
                        .is_none_or(|barrier| !barrier.blocks_tip(&record.entry))
                }) {
                    report.logical_tips += 1;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(|_| invalid("a stored lifecycle record is unreadable"))
}
