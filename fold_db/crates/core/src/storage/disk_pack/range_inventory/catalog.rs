use super::{invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::atom::molecule_key_codec::storage_profile::RangeStorageProfile;
use crate::atom::MoleculeKeyCodec;
use crate::db_operations::molecule_key_store::{node_bundle_storage_keys, open_frozen_node_bundle};
use crate::schema::Schema;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Serialize)]
pub struct MoleculeInventory {
    pub molecule: String,
    pub declarations: Vec<String>,
    pub proposed_profile: Option<RangeStorageProfile>,
    pub blockers: Vec<String>,
    pub physical_slots: u64,
    pub distinct_storage_keys: usize,
    pub logical_tips: u64,
    pub delete_barriers: u64,
    pub history_key_references: u64,
    pub retained_key_references: u64,
    pub canonical_collisions: u64,
    pub source_ranges_checked: usize,
    pub target_keys_checked: usize,
    /// Distinct normalized mk-coordinate bytes, not physical database size.
    pub source_key_bytes: u64,
    /// Excludes generation prefixes, barriers, pointers, counters and payloads.
    pub proposed_key_bytes: Option<u64>,
}

pub(super) struct Molecule {
    pub(super) report: MoleculeInventory,
    pub(super) codec: MoleculeKeyCodec,
    pub(super) range_profiles: BTreeSet<String>,
    pub(super) permits_empty_slot: bool,
    pub(super) coordinates: BTreeMap<String, String>,
}

pub(super) struct Catalog {
    pub(super) schemas: usize,
    pub(super) molecules: BTreeMap<String, Molecule>,
    pub(super) logical: super::logical::LogicalSlots,
    records: BTreeMap<(String, String), Vec<u8>>,
    account_key: zeroize::Zeroizing<[u8; 32]>,
    configured: MoleculeKeyCodec,
}

impl Catalog {
    pub(super) fn load(
        records: &[MetadataRecord],
        key: &[u8; 32],
        configured: &MoleculeKeyCodec,
        limits: InventoryLimits,
    ) -> StorageResult<Self> {
        let key_rows = records
            .iter()
            .filter(|row| row.collection == "molecule_keys")
            .map(|row| ((row.collection.clone(), row.key.clone()), row.value.clone()))
            .collect();
        let mut catalog = Self {
            schemas: 0,
            molecules: BTreeMap::new(),
            logical: super::logical::LogicalSlots::default(),
            records: key_rows,
            account_key: zeroize::Zeroizing::new(*key),
            configured: configured.clone(),
        };
        for record in records.iter().filter(|row| row.collection == "schemas") {
            let opened = crate::crypto::is_sealed_at_rest(&record.value)
                .then(|| {
                    crate::crypto::at_rest::open_at_rest_bounded(
                        key,
                        &record.value,
                        4 * 1024 * 1024,
                    )
                    .map(zeroize::Zeroizing::new)
                })
                .transpose()
                .map_err(|_| invalid("a schema envelope cannot be opened"))?;
            let bytes = opened
                .as_deref()
                .map_or(record.value.as_slice(), Vec::as_slice);
            let mut schema: Schema = serde_json::from_slice(bytes)
                .map_err(|_| invalid("a schema declaration is unreadable"))?;
            schema
                .populate_runtime_fields()
                .map_err(|_| invalid("a schema declaration is invalid"))?;
            catalog.schemas += 1;
            let range = schema
                .key
                .as_ref()
                .and_then(|key| key.range_field.as_deref());
            let profile =
                range.map(|path| RangeStorageProfile::typed(&schema.key_field_type(path)));
            for (name, field) in &schema.runtime_fields {
                let molecule = field
                    .common()
                    .molecule_uuid
                    .as_deref()
                    .ok_or_else(|| invalid("a schema field has no molecule"))?;
                let entry = catalog.molecule(molecule, limits)?;
                entry
                    .report
                    .declarations
                    .push(format!("{}:{name}", schema.name));
                match &profile {
                    None => entry.permits_empty_slot = true,
                    Some(Ok(profile)) => {
                        entry.range_profiles.insert(
                            serde_json::to_string(profile)
                                .map_err(|_| invalid("cannot encode a profile"))?,
                        );
                        match entry.report.proposed_profile {
                            Some(prior) if prior != *profile => {
                                entry.report.blockers.push(
                                    "shared molecule has different declared range domains".into(),
                                );
                                entry.report.proposed_profile = None;
                            }
                            None if entry.range_profiles.len() == 1 => {
                                entry.report.proposed_profile = Some(*profile)
                            }
                            _ => {}
                        }
                    }
                    Some(Err(_)) => entry
                        .report
                        .blockers
                        .push("a declared range type has no typed profile".into()),
                }
            }
        }
        Ok(catalog)
    }

    pub(super) fn molecule(
        &mut self,
        molecule: &str,
        limits: InventoryLimits,
    ) -> StorageResult<&mut Molecule> {
        if !crate::atom::molecule_uuid::is_canonical_molecule_uuid(molecule) {
            return Err(invalid("a molecule identity is not canonical"));
        }
        if !self.molecules.contains_key(molecule) {
            if self.molecules.len() >= limits.max_records {
                return Err(invalid("molecule count exceeds its limit"));
            }
            let [metadata, wrap] = node_bundle_storage_keys(molecule)?;
            let metadata = self.records.get(&("molecule_keys".into(), metadata));
            let wrap = self.records.get(&("molecule_keys".into(), wrap));
            let codec = match (metadata, wrap) {
                (None, None) => self.configured.clone(),
                (Some(metadata), Some(wrap)) => {
                    open_frozen_node_bundle(molecule, &self.account_key, metadata, wrap)?
                        .key_codec(&self.configured)
                }
                _ => return Err(invalid("molecule bundle metadata and node wrap disagree")),
            };
            self.molecules.insert(
                molecule.into(),
                Molecule {
                    report: MoleculeInventory {
                        molecule: molecule.into(),
                        declarations: Vec::new(),
                        proposed_profile: None,
                        blockers: Vec::new(),
                        physical_slots: 0,
                        distinct_storage_keys: 0,
                        logical_tips: 0,
                        delete_barriers: 0,
                        history_key_references: 0,
                        retained_key_references: 0,
                        canonical_collisions: 0,
                        source_ranges_checked: 0,
                        target_keys_checked: 0,
                        source_key_bytes: 0,
                        proposed_key_bytes: None,
                    },
                    codec,
                    range_profiles: BTreeSet::new(),
                    permits_empty_slot: false,
                    coordinates: BTreeMap::new(),
                },
            );
        }
        Ok(self.molecules.get_mut(molecule).expect("inserted above"))
    }

    pub(super) fn finish(
        &mut self,
        limits: InventoryLimits,
    ) -> StorageResult<Vec<MoleculeInventory>> {
        self.logical.count_winners(&mut self.molecules)?;
        let mut reports = Vec::new();
        for molecule in self.molecules.values_mut() {
            if molecule.report.declarations.is_empty() {
                molecule
                    .report
                    .blockers
                    .push("molecule has no schema declaration".into());
            }
            if molecule.report.proposed_profile.is_none()
                && molecule.permits_empty_slot
                && molecule.range_profiles.is_empty()
            {
                molecule.report.proposed_profile = Some(RangeStorageProfile::LegacyUtf8V1);
            }
            molecule.report.declarations.sort();
            molecule.report.declarations.dedup();
            super::keys::validate_conversion(molecule, limits)?;
            molecule.report.blockers.sort();
            molecule.report.blockers.dedup();
        }
        for (_, molecule) in std::mem::take(&mut self.molecules) {
            reports.push(molecule.report);
        }
        Ok(reports)
    }
}
