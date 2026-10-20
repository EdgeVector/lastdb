use super::*;

impl AtomStore {
    /// For each slot, whether its body already exists at either derived key.
    pub(super) async fn probe_page_bodies_present(
        &self,
        decoded: &[DanglingTipSlot],
    ) -> Result<Vec<bool>, SchemaError> {
        // Batch current flat and located body addresses for the page.
        // Final candidates receive a live check before removal.
        let mut probe_keys = Vec::with_capacity(decoded.len() * 2);
        for slot in decoded {
            probe_keys.push(slot.prefixed_key.clone());
            probe_keys.push(slot.flat_key.clone());
        }
        let found = self
            .repair_exists(&probe_keys, "repair exists body (page)")
            .await?;
        Ok(decoded
            .iter()
            .enumerate()
            .map(|(i, _)| {
                found.get(i * 2).copied().unwrap_or(false)
                    || found.get(i * 2 + 1).copied().unwrap_or(false)
            })
            .collect::<Vec<bool>>())
    }

    /// Classify one slot whose body the page probe did not find: skip it,
    /// or count it as repairable (and queue it for repair on an execute run).
    pub(super) async fn classify_dangling_tip_slot(
        &self,
        slot: DanglingTipSlot,
        report: &mut DanglingTipRepairReport,
        walk: &mut DanglingTipWalk<'_>,
    ) -> Result<(), SchemaError> {
        let storage_prefix = walk.storage_prefix;
        let Some(class) = self
            .classify_repairable_dangling_tip(&slot, storage_prefix, ExistsAuthority::Cached)
            .await?
        else {
            report.skipped_body_restored += 1;
            return Ok(());
        };
        if class == UnresolvedTipClass::MisDerivedPartition {
            report.skipped_mis_derived += 1;
            return Ok(());
        }
        // The paged walk answered from whatever tier was warm. Before a
        // tip is called repairable — reported as such in a dry run, and
        // deleted in an execute — confirm it against the live group.
        // The candidate set is small by construction, so the live loads
        // this costs are bounded by the damage, not by the store.
        let Some(class) = self
            .classify_repairable_dangling_tip(&slot, storage_prefix, ExistsAuthority::Live)
            .await?
        else {
            report.skipped_body_restored += 1;
            report.rescued_by_live_probe += 1;
            return Ok(());
        };
        if class == UnresolvedTipClass::MisDerivedPartition {
            report.skipped_mis_derived += 1;
            report.rescued_by_live_probe += 1;
            return Ok(());
        }
        report.repairable_tips += 1;
        if let Some(cap) = walk.options.audit_unresolved {
            if report.unresolved.len() < cap {
                report.unresolved.push(UnresolvedTip {
                    tip_key: slot.tip_key.clone(),
                    atom_uuid: slot.uuid.clone(),
                    derived_partition: slot.partition.as_str().to_string(),
                    molecule_uuid: slot
                        .partition
                        .molecule_uuid()
                        .unwrap_or_default()
                        .to_string(),
                    schema: None,
                    locator_partition: self.locator_partition_for_report(&slot.locator_key).await?,
                    class,
                });
            } else {
                report.unresolved_truncated = true;
            }
        }
        if walk.options.dry_run {
            return Ok(());
        }
        let Some(key) = decode_repair_key(storage_prefix, &slot.tip_key) else {
            report.skipped_unparseable_key += 1;
            return Ok(());
        };
        walk.repair_groups
            .entry(key.molecule_uuid.clone())
            .or_default()
            .push(RepairCandidate { slot, key });
        Ok(())
    }
}

/// Decode one `mk:` page into slots. Advances `page_start` to the last key
/// read, and skips `head` (the previous page's last key, re-read by the
/// inclusive range start). Rows that do not parse are dropped.
pub(super) fn decode_dangling_tip_page(
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    head: Option<&str>,
    page_start: &mut String,
    storage_prefix: Option<&str>,
) -> Vec<DanglingTipSlot> {
    use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding, AtomPartition};

    let mut decoded: Vec<DanglingTipSlot> = Vec::with_capacity(rows.len());
    for (k, v) in rows {
        let tip_key = String::from_utf8_lossy(&k).into_owned();
        *page_start = tip_key.clone();
        if head == Some(tip_key.as_str()) {
            continue;
        }
        let Some(base_key) = strip_storage_prefix(storage_prefix, &tip_key) else {
            continue;
        };
        let Some(partition) = AtomPartition::from_record_key(base_key) else {
            continue;
        };
        let Ok(val) = serde_json::from_slice::<Value>(&v) else {
            continue;
        };
        let Some(uuid) = val
            .pointer("/entry/atom_uuid")
            .and_then(|x| x.as_str())
            .filter(|u| !u.is_empty())
        else {
            continue;
        };
        let uuid = uuid.to_string();
        decoded.push(DanglingTipSlot {
            flat_key: build_storage_key(storage_prefix, &atom_key_codec::flat_key(&uuid)),
            prefixed_key: build_storage_key(
                storage_prefix,
                &atom_key_codec::storage_key(
                    AtomKeyEncoding::PartitionPrefix,
                    Some(&partition),
                    &uuid,
                ),
            ),
            locator_key: build_storage_key(storage_prefix, &atom_locator_codec::locator_key(&uuid)),
            tip_key,
            uuid,
            partition,
        });
    }
    decoded
}
