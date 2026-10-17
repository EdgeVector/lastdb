//! Logical Delete suppression across every exact readable physical candidate.

use super::*;

type ResidentSlot = (String, String, String);
type CandidateResidentSlots = HashMap<String, Vec<ResidentSlot>>;

pub(super) struct DeletePublication {
    pub(super) winning: HashSet<(String, String, String)>,
    _tip_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
    _publication_guards: Vec<tokio::sync::OwnedMutexGuard<()>>,
}

impl MutationManager {
    pub(super) async fn prepare_delete_publication(
        &self,
        schema: &crate::schema::types::Schema,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<DeletePublication, SchemaError> {
        let barriers = super::super::super::purge::plan_normal_delete_barriers(
            &self.db_ops,
            schema,
            mutations,
        )?;
        let keys: Vec<_> = barriers
            .iter()
            .map(|barrier| barrier.mk_key.clone())
            .collect();
        let tip_guards = self.db_ops.atoms().lock_tip_commits(&keys).await;
        let publication_guards = self.db_ops.atoms().lock_tip_publications(&keys).await;
        let slots_by_key = self.delete_candidate_resident_slots(schema, mutations)?;
        let disk_tips = self
            .db_ops
            .atoms()
            .delete_target_tips_with_generations(&keys, storage_prefix)
            .await?;
        let mut winning = HashSet::new();
        let mut protected = HashSet::new();
        for (barrier, disk_tip) in barriers.iter().zip(disk_tips) {
            let slots = slots_by_key.get(&barrier.mk_key).ok_or_else(|| {
                SchemaError::InvalidData(
                    "Delete barrier has no matching memory molecule key".into(),
                )
            })?;
            winning.extend(slots.iter().cloned());
            for (uuid, hash, range) in slots {
                let newer_in_memory =
                    crate::db_operations::resident_read::graph_reads_enabled(&self.db_ops)
                        && self
                            .db_ops
                            .resident()
                            .resolve_tip(uuid, hash, range)
                            .is_some_and(|tip| !barrier.blocks_resident_tip(&tip.value));
                let newer_on_disk = disk_tip
                    .as_ref()
                    .is_some_and(|tip| !barrier.blocks_tip(&tip.entry));
                if newer_in_memory || newer_on_disk {
                    protected.insert((uuid.clone(), hash.clone(), range.clone()));
                }
            }
        }
        winning.retain(|slot| !protected.contains(slot));
        self.db_ops
            .atoms()
            .register_pending_delete_barriers(barriers);
        Ok(DeletePublication {
            winning,
            _tip_guards: tip_guards,
            _publication_guards: publication_guards,
        })
    }

    fn delete_candidate_resident_slots(
        &self,
        schema: &crate::schema::types::Schema,
        mutations: &[Mutation],
    ) -> Result<CandidateResidentSlots, SchemaError> {
        let mut slots_by_key: CandidateResidentSlots = HashMap::new();
        for mutation in mutations {
            for field in schema.runtime_fields.values() {
                let Some(uuid) = field.common().molecule_uuid() else {
                    continue;
                };
                let Some((hash, range)) = field.disk_slot_for_key(&mutation.key_value) else {
                    continue;
                };
                for candidate in super::super::super::purge::delete_candidates_for_key(
                    &self.db_ops,
                    field,
                    &mutation.key_value,
                )? {
                    slots_by_key.entry(candidate.mk_key).or_default().push((
                        uuid.clone(),
                        hash.clone(),
                        range.clone(),
                    ));
                }
            }
        }
        Ok(slots_by_key)
    }
}
