use super::*;

impl AtomStore {
    /// For each slot, whether its body already exists at either derived key.
    pub(super) async fn probe_page_bodies_present(
        &self,
        decoded: &[RekeySlot],
    ) -> Result<Vec<bool>, SchemaError> {
        // Decide the healthy majority with ONE probe for the page.
        //
        // Almost every tip a full walk sees has its body exactly where the
        // tip derives it, and `classify_repairable_dangling_tip` settles
        // that case on its first `exists` alone. Asking that question one
        // tip at a time made the walk's cheap-tier round trips scale with
        // the page — the cost `rekey_page_round_trips_do_not_scale_with_page_size`
        // exists to hold down on the migration walk over the same `mk:`
        // plane, which this walk did not inherit.
        //
        // Measured on the live primary (0.23.3-1663-gd5a4bba77, 16.5 GiB
        // store, 8.68 GiB of tips): a bounded dry run cost 49.7 s for 100
        // tips and 62.2 s for 1000 within one page, i.e. ~13.9 ms of
        // marginal classify per tip against hash-scattered partitions. At
        // the ~750K-1.9M tips that plane has held, sizing a repair is a
        // multi-hour job, and `probe_locator_only_population` — a cheap
        // bounded gauge, 4096 tips over 512 windows — inherits the same
        // per-tip cost. A full `mk:` dry run exceeded the CLI's own 600 s
        // admin deadline on that store on 2026-09-07, against 2m35s for the
        // same walk on 2026-08-18, so the bounded probe is now the only
        // affordable instrument and its spread has to carry the answer.
        //
        // The probe below asks the same question on the same tier for the
        // same keys, so a slot it calls present takes exactly the branch the
        // per-slot classifier took. Only the slots it calls absent — bounded
        // by the damage, not by the store — walk on to the locator reads and
        // the live re-confirmation, which are deliberately left per-slot.
        let mut probe_keys = Vec::with_capacity(decoded.len() * 2);
        for slot in decoded {
            probe_keys.push(slot.prefixed_key.clone());
            probe_keys.push(slot.flat_key.clone());
        }
        let found = self
            .rekey_exists(&probe_keys, "repair exists body (page)")
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
        slot: RekeySlot,
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
) -> Vec<RekeySlot> {
    use crate::atom::{atom_key_codec, atom_locator_codec, AtomKeyEncoding, AtomPartition};

    let mut decoded: Vec<RekeySlot> = Vec::with_capacity(rows.len());
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
        decoded.push(RekeySlot {
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
