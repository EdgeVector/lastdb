use super::catalog::{Catalog, Molecule};
use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::atom::delete_barrier::{delete_barrier_key, DeleteBarrier};
use crate::atom::molecule_key_codec::{
    decode_hash_range, hash_range_record_key,
    storage_profile::{ProfiledRangeCodec, RangeStorageProfile},
};
use crate::atom::MutationEvent;
use crate::db_operations::atom_store::QueueValue;
use crate::storage::current_keys;
use std::collections::BTreeMap;

pub(super) fn observe(
    catalog: &mut Catalog,
    record: &MetadataRecord,
    families: &mut BTreeMap<String, u64>,
    limits: InventoryLimits,
) -> StorageResult<()> {
    if !matches!(
        record.collection.as_str(),
        "tips" | "proteins" | "indexes" | "molecule_ref_edges"
    ) {
        *families.entry(record.collection.clone()).or_default() += 1;
        return Ok(());
    }
    current_keys::validate("main", record.key.as_bytes())?;
    let bare = current_keys::source_key(&record.key)?;
    let scope = &record.key[..record.key.len() - bare.len()];
    let family = bare.split([':', '\0']).next().unwrap_or_default();
    let label = if current_keys::main_family(family) {
        family
    } else {
        "other_control"
    };
    *families
        .entry(format!("{}/{label}", record.collection))
        .or_default() += 1;
    match family {
        "mk" | "mgr" | "mgd" => {
            let (molecule, generation, suffix) = slot_parts(bare, family)?;
            let key = format!("mk:{molecule}:{suffix}");
            register(catalog, &format!("{scope}{key}"), limits)?
                .report
                .physical_slots += 1;
            catalog
                .logical
                .observe(scope, molecule, family, &key, generation, &record.value)?;
        }
        "mgp" => {
            let molecule = bare
                .strip_prefix("mgp:v1:")
                .ok_or_else(|| invalid("a generation pointer key is invalid"))?;
            catalog.molecule(molecule, limits)?;
            catalog
                .logical
                .observe(scope, molecule, family, "", None, &record.value)?;
        }
        "mh" => {
            let _: crate::db_operations::atom_store::MoleculeHeader =
                super::logical::decode(&record.value)?;
            catalog.molecule(
                bare.strip_prefix("mh:")
                    .ok_or_else(|| invalid("a molecule header key is invalid"))?,
                limits,
            )?;
        }
        "rdel" => {
            let barrier: DeleteBarrier = super::logical::decode(&record.value)?;
            if delete_barrier_key(barrier.mk_key.as_bytes()) != record.key {
                return Err(invalid("a Delete barrier has a different target identity"));
            }
            let molecule = register(catalog, &barrier.mk_key, limits)?
                .report
                .molecule
                .clone();
            catalog.molecule(&molecule, limits)?.report.delete_barriers += 1;
            let key = current_keys::source_key(&barrier.mk_key)?;
            catalog
                .logical
                .observe(scope, &molecule, family, key, None, &record.value)?;
        }
        "history" => observe_history(catalog, record, bare, limits)?,
        "crq" | "crs" => observe_retention(catalog, record, family, limits)?,
        _ => {}
    }
    Ok(())
}

fn observe_history(
    catalog: &mut Catalog,
    record: &MetadataRecord,
    bare: &str,
    limits: InventoryLimits,
) -> StorageResult<()> {
    let event: MutationEvent = super::logical::decode(&record.value)?;
    let molecule = history_molecule(bare)?;
    if molecule != event.molecule_uuid {
        return Err(invalid("a history event has a different molecule identity"));
    }
    catalog
        .molecule(molecule, limits)?
        .report
        .history_key_references += 1;
    if let Some(delete) = event.suppressed_by_delete {
        if register(catalog, &delete.mk_key, limits)?.report.molecule != molecule {
            return Err(invalid(
                "a history Delete reference has a different molecule identity",
            ));
        }
    }
    Ok(())
}

fn observe_retention(
    catalog: &mut Catalog,
    record: &MetadataRecord,
    family: &str,
    limits: InventoryLimits,
) -> StorageResult<()> {
    let slot = match QueueValue::decode(&record.value)
        .map_err(|_| invalid("a retained key reference is unreadable"))?
    {
        QueueValue::Record(entry) => {
            let expected = if family == "crq" {
                &entry.queue
            } else {
                &entry.dir
            };
            if entry.version != 1 || expected != &record.key {
                return Err(invalid("a retained record key differs from its pointer"));
            }
            // Retention repair can recover a queue entry without its original
            // slot directory. Preserve that metadata; never invent a coordinate.
            if family == "crq" && entry.slot.is_empty() && entry.dir.is_empty() {
                let molecule = history_molecule(current_keys::source_key(&entry.history)?)?;
                let report = &mut catalog.molecule(molecule, limits)?.report;
                report.retained_key_references += 1;
                report.unresolved_retained_references += 1;
                report
                    .blockers
                    .push("a repaired retained record has no recoverable slot coordinate".into());
                return Ok(());
            }
            entry.slot
        }
        QueueValue::Drain(marker) => {
            if family != "crq" || marker.version != 1 || !marker.drain || marker.queue != record.key
            {
                return Err(invalid("a drain marker key differs from its pointer"));
            }
            marker.slot
        }
    };
    register(catalog, &slot, limits)?
        .report
        .retained_key_references += 1;
    Ok(())
}

fn history_molecule(key: &str) -> StorageResult<&str> {
    crate::kind_partition::rest_of(key, "history")
        .and_then(|rest| rest.split_once(':'))
        .map(|(molecule, _)| molecule)
        .ok_or_else(|| invalid("a history key is invalid"))
}

fn slot_parts<'a>(
    key: &'a str,
    family: &str,
) -> StorageResult<(&'a str, Option<&'a str>, &'a str)> {
    let prefix = match family {
        "mk" => "mk:",
        "mgr" => "mgr:v1:",
        "mgd" => "mgd:v1:",
        _ => return Err(invalid("unknown slot family")),
    };
    let (molecule, tail) = key
        .strip_prefix(prefix)
        .and_then(|k| k.split_once(':'))
        .ok_or_else(|| invalid("a stored slot key is malformed"))?;
    if family == "mgr" {
        let (generation, suffix) = tail
            .split_once(':')
            .filter(|(g, _)| !g.is_empty())
            .ok_or_else(|| invalid("a generation slot is malformed"))?;
        Ok((molecule, Some(generation), suffix))
    } else {
        Ok((molecule, None, tail))
    }
}

fn register<'a>(
    catalog: &'a mut Catalog,
    key: &str,
    limits: InventoryLimits,
) -> StorageResult<&'a mut Molecule> {
    if key.len() > limits.max_key_bytes {
        return Err(invalid("a referenced key exceeds its byte limit"));
    }
    let bare = current_keys::source_key(key)?;
    let (molecule, _, _) = slot_parts(bare, "mk")?;
    if decode_hash_range(bare, molecule).is_none() {
        return Err(invalid("a referenced molecule key is malformed"));
    }
    let entry = catalog.molecule(molecule, limits)?;
    entry.coordinates.insert(key.into(), bare.into());
    Ok(entry)
}

pub(super) fn validate_conversion(
    molecule: &mut Molecule,
    limits: InventoryLimits,
) -> StorageResult<()> {
    let report = &mut molecule.report;
    report.distinct_storage_keys = molecule.coordinates.len();
    report.source_key_bytes = molecule
        .coordinates
        .keys()
        .map(|key| key.len() as u64)
        .sum();
    let source = ProfiledRangeCodec::new(RangeStorageProfile::LegacyUtf8V1, &molecule.codec);
    let target = report
        .proposed_profile
        .filter(|_| report.blockers.is_empty())
        .map(|profile| ProfiledRangeCodec::new(profile, &molecule.codec));
    let mut targets = BTreeMap::new();
    let mut bytes = 0_u64;
    for (scoped, bare) in &molecule.coordinates {
        let (hash, range) = decode_hash_range(bare, &report.molecule)
            .ok_or_else(|| invalid("a stored coordinate is malformed"))?;
        let scope = &scoped[..scoped.len() - bare.len()];
        let api = source.decode_range(&report.molecule, &range).map_err(|_| {
            invalid("a range key does not match the explicit source encoding or molecule key")
        })?;
        report.source_ranges_checked += 1;
        let Some(target) = target.as_ref() else {
            continue;
        };
        let next = if range.is_empty() && molecule.permits_empty_slot {
            scoped.clone()
        } else {
            let prefix = scoped.len() - range.len();
            let bound = limits
                .max_key_bytes
                .checked_sub(prefix)
                .ok_or_else(|| invalid("a key exceeds its byte limit"))?;
            let Ok(range) = target.encode_range(&report.molecule, &api, bound) else {
                report.blockers.push(
                    "a range cannot use the proposed profile within the key byte limit".into(),
                );
                continue;
            };
            format!(
                "{scope}{}",
                hash_range_record_key(&report.molecule, &hash, &range)
            )
        };
        bytes = bytes
            .checked_add(next.len() as u64)
            .filter(|bytes| *bytes <= limits.max_metadata_bytes as u64)
            .ok_or_else(|| invalid("converted key metadata exceeds its byte limit"))?;
        if targets.insert(next, scoped).is_some() {
            report.canonical_collisions += 1;
        }
        report.target_keys_checked += 1;
    }
    if report.canonical_collisions != 0 {
        report
            .blockers
            .push("distinct stored keys have the same canonical target".into());
    }
    if target.is_some() && report.target_keys_checked == report.distinct_storage_keys {
        report.proposed_key_bytes = Some(bytes);
    }
    Ok(())
}
