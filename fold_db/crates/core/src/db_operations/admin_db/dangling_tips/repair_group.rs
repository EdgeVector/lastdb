use super::*;
use crate::db_operations::MoleculeData;

impl AtomStore {
    pub(super) async fn repair_dangling_tip_group(
        &self,
        molecule_uuid: &str,
        candidates: Vec<RepairCandidate>,
        storage_prefix: Option<&str>,
    ) -> (Vec<Result<RepairAttempt, SchemaError>>, u64) {
        let mut attempts = Vec::with_capacity(candidates.len());
        // Durable writers acquire the molecule barrier, atom body locks, then
        // exact tip locks. Keep these through the final live check and removal.
        // A caller that needs durable and publication locks takes them in that
        // order. Otherwise a Put can replace a validated tip, or a body can
        // reappear, before removal.
        let _molecule_guard = self
            .lock_molecule_commit(molecule_uuid, storage_prefix)
            .await;
        let atom_uuids: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.slot.uuid.clone())
            .collect();
        let _atom_guards = self.lock_automatic_gc_atoms(&atom_uuids).await;
        let tip_keys: Vec<String> = candidates
            .iter()
            .map(|candidate| candidate.slot.tip_key.clone())
            .collect();
        let _tip_guards = self.lock_tip_commits(&tip_keys).await;
        let _publication_guards = self.lock_tip_publications(&tip_keys).await;
        let ready = self
            .revalidate_candidates(candidates, storage_prefix, &mut attempts)
            .await;
        if ready.is_empty() {
            return (attempts, 0);
        }

        let slots: Vec<(String, String)> = ready
            .iter()
            .map(|candidate| (candidate.key.hash.clone(), candidate.key.range.clone()))
            .collect();
        let mut molecule = match self
            .load_molecule_for_storage_slot_purge(molecule_uuid, storage_prefix, &slots)
            .await
        {
            Ok(Some(molecule)) => molecule,
            Ok(None) => {
                attempts.extend(
                    ready
                        .into_iter()
                        .map(|_| Ok(RepairAttempt::MoleculeMissing)),
                );
                return (attempts, 1);
            }
            Err(e) => {
                let message = format!("load repair molecule {molecule_uuid}: {e}");
                attempts.extend(
                    ready
                        .into_iter()
                        .map(|_| Err(SchemaError::InvalidData(message.clone()))),
                );
                return (attempts, 1);
            }
        };

        let removable = retain_slots_still_in_molecule(&mut molecule, ready, &mut attempts);
        if removable.is_empty() {
            return (attempts, 1);
        }

        let removed_slots: Vec<(String, String)> = removable
            .iter()
            .map(|candidate| (candidate.key.hash.clone(), candidate.key.range.clone()))
            .collect();
        // This molecule holds only the requested slots. An empty partial load
        // cannot prove that no sibling remains in the live or base generation.
        // Leave the header for later cleanup if these were the final slots.
        let remove_result = self
            .remove_molecule_keys(molecule_uuid, &molecule, &removed_slots, storage_prefix)
            .await;
        if let Err(e) = remove_result {
            let message = format!(
                "remove {} repair slot(s) from molecule {molecule_uuid}: {e}",
                removable.len()
            );
            attempts.extend(
                removable
                    .into_iter()
                    .map(|_| Err(SchemaError::InvalidData(message.clone()))),
            );
            return (attempts, 1);
        }

        self.delete_repaired_locators(removable, &mut attempts)
            .await;
        (attempts, 1)
    }

    /// Re-check each candidate under the commit locks. Candidates that are
    /// settled (restored, changed, mis-derived) land in `attempts`; the rest
    /// are returned as ready to repair.
    async fn revalidate_candidates(
        &self,
        candidates: Vec<RepairCandidate>,
        storage_prefix: Option<&str>,
        attempts: &mut Vec<Result<RepairAttempt, SchemaError>>,
    ) -> Vec<RepairCandidate> {
        let mut ready = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            match self
                .revalidate_dangling_tip(&candidate.slot, storage_prefix)
                .await
            {
                Ok(None) => ready.push(candidate),
                Ok(Some(attempt)) => attempts.push(Ok(attempt)),
                Err(e) => attempts.push(Err(e)),
            }
        }
        ready
    }

    /// Delete each repaired slot's locator record, pushing one attempt per
    /// candidate.
    async fn delete_repaired_locators(
        &self,
        removable: Vec<RepairCandidate>,
        attempts: &mut Vec<Result<RepairAttempt, SchemaError>>,
    ) {
        for candidate in removable {
            let mut deleted_keys = 1u64;
            let locator_exists = self.raw().exists_item(&candidate.slot.locator_key).await;
            match locator_exists {
                Ok(true) => {
                    if let Err(e) = self.raw().delete_item(&candidate.slot.locator_key).await {
                        attempts.push(Err(SchemaError::InvalidData(format!(
                            "repair delete locator: {e}"
                        ))));
                        continue;
                    }
                    deleted_keys += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    attempts.push(Err(SchemaError::InvalidData(format!(
                        "repair locator exists: {e}"
                    ))));
                    continue;
                }
            }
            attempts.push(Ok(RepairAttempt::Repaired { deleted_keys }));
        }
    }

    pub(super) async fn revalidate_dangling_tip(
        &self,
        slot: &DanglingTipSlot,
        storage_prefix: Option<&str>,
    ) -> Result<Option<RepairAttempt>, SchemaError> {
        // The last word before anything is deleted, so it must come from the
        // live group rather than whatever tier happens to be warm.
        match self
            .classify_repairable_dangling_tip(slot, storage_prefix, ExistsAuthority::Live)
            .await?
        {
            None => return Ok(Some(RepairAttempt::BodyRestored)),
            Some(UnresolvedTipClass::MisDerivedPartition) => {
                return Ok(Some(RepairAttempt::MisDerived));
            }
            Some(_) => {}
        }

        let current: Option<Value> = self
            .raw()
            .get_item(&slot.tip_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("repair re-read tip: {e}")))?;
        let Some(current) = current else {
            return Ok(Some(RepairAttempt::Changed));
        };
        if current.pointer("/entry/atom_uuid").and_then(|x| x.as_str()) != Some(slot.uuid.as_str())
        {
            return Ok(Some(RepairAttempt::Changed));
        }
        Ok(None)
    }

    pub(super) async fn classify_repairable_dangling_tip(
        &self,
        slot: &DanglingTipSlot,
        storage_prefix: Option<&str>,
        authority: ExistsAuthority,
    ) -> Result<Option<UnresolvedTipClass>, SchemaError> {
        use crate::atom::atom_locator_codec;

        let present = self
            .exists_with_authority(
                &[slot.prefixed_key.clone(), slot.flat_key.clone()],
                "repair exists body",
                authority,
            )
            .await?;
        if present.into_iter().any(|ok| ok) {
            return Ok(None);
        }
        let locator: Option<Value> = self
            .raw()
            .get_item(&slot.locator_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("repair get locator: {e}")))?;
        let Some(locator_value) = locator else {
            return Ok(Some(UnresolvedTipClass::NoBodyAnywhere));
        };
        let Some(located) = atom_locator_codec::decode_value(&locator_value) else {
            return Ok(Some(UnresolvedTipClass::UndecodableLocator));
        };
        if located.as_str() == slot.partition.as_str() {
            return Ok(Some(UnresolvedTipClass::OrphanLocator));
        }
        let located_key = build_storage_key(
            storage_prefix,
            &crate::atom::atom_key_codec::storage_key(
                crate::atom::AtomKeyEncoding::PartitionPrefix,
                Some(&located),
                &slot.uuid,
            ),
        );
        let elsewhere = self
            .exists_with_authority(&[located_key], "repair exists located body", authority)
            .await?;
        if elsewhere.first().copied().unwrap_or(false) {
            Ok(Some(UnresolvedTipClass::MisDerivedPartition))
        } else {
            Ok(Some(UnresolvedTipClass::OrphanLocator))
        }
    }

    pub(super) async fn locator_partition_for_report(
        &self,
        locator_key: &str,
    ) -> Result<Option<String>, SchemaError> {
        let raw: Option<Value> = self
            .raw()
            .get_item(locator_key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("repair report locator: {e}")))?;
        Ok(raw
            .as_ref()
            .and_then(crate::atom::atom_locator_codec::decode_value)
            .map(|p| p.as_str().to_string()))
    }
}

/// Drop each candidate's slot from the loaded molecule when it still names
/// the expected atom. Candidates whose slot moved are recorded as skipped.
fn retain_slots_still_in_molecule(
    molecule: &mut MoleculeData,
    ready: Vec<RepairCandidate>,
    attempts: &mut Vec<Result<RepairAttempt, SchemaError>>,
) -> Vec<RepairCandidate> {
    let mut removable = Vec::with_capacity(ready.len());
    for candidate in ready {
        let current_uuid = molecule
            .get_atom_uuid(&candidate.key.hash, &candidate.key.range)
            .map(String::as_str);
        if current_uuid != Some(candidate.slot.uuid.as_str()) {
            attempts.push(Ok(RepairAttempt::AtomNotInMolecule));
            continue;
        }
        let removed = molecule.remove_atom_uuid(&candidate.key.hash, &candidate.key.range);
        debug_assert_eq!(removed.as_deref(), Some(candidate.slot.uuid.as_str()));
        removable.push(candidate);
    }
    removable
}
