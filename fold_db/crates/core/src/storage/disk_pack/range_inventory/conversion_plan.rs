//! A bounded, explicit mapping from one source publication to typed coordinates.

use super::{catalog::Catalog, invalid, InventoryLimits, MetadataRecord, StorageResult};
use crate::atom::molecule_key_codec::{
    decode_hash_range, hash_range_record_key,
    storage_profile::{ProfiledRangeCodec, RangeStorageProfile},
    ConflictKeyIdentity,
};
use crate::atom::{HashKeyEncoding, RangeKeyEncoding};
use crate::db_operations::molecule_key_store::{node_bundle_storage_keys, typed_frozen_metadata};
use laststore::disk_run::home::HomeMutation;
use schema_types::molecule_range_profile::MoleculeRangeProfile;
use std::collections::BTreeMap;

pub(super) struct ConversionPlan {
    pub(super) coordinates: BTreeMap<String, String>,
    pub(super) metadata: BTreeMap<(String, String), HomeMutation>,
    pub(super) extras: Vec<HomeMutation>,
    pub(super) profiles: BTreeMap<String, MoleculeRangeProfile>,
}

impl ConversionPlan {
    pub(super) fn build(
        catalog: &Catalog,
        records: &[MetadataRecord],
        account_key: &[u8; 32],
        source_range: RangeKeyEncoding,
        limits: InventoryLimits,
    ) -> StorageResult<Self> {
        let mut plan = Self {
            coordinates: BTreeMap::new(),
            metadata: BTreeMap::new(),
            extras: Vec::new(),
            profiles: BTreeMap::new(),
        };
        let mut bytes = 0usize;
        let key_rows: BTreeMap<_, _> = records
            .iter()
            .filter(|row| row.collection == "molecule_keys")
            .map(|row| (row.key.as_str(), row))
            .collect();
        for (molecule, entry) in &catalog.molecules {
            if !entry.report.blockers.is_empty() {
                return Err(invalid("a molecule has unresolved conversion blockers"));
            }
            let scalar = entry
                .report
                .proposed_profile
                .ok_or_else(|| invalid("a molecule profile is absent"))?;
            let profile = if entry.permits_empty_slot {
                MoleculeRangeProfile::EmptyV1
            } else {
                MoleculeRangeProfile::TypedV1 { scalar }
            };
            profile
                .validate()
                .map_err(|_| invalid("invalid target profile"))?;
            plan.profiles.insert(molecule.clone(), profile);
            let decoder = ProfiledRangeCodec::new(RangeStorageProfile::LegacyUtf8V1, &entry.codec);
            for (scoped, bare) in &entry.coordinates {
                let (hash, stored) = decode_hash_range(bare, molecule)
                    .ok_or_else(|| invalid("invalid source coordinate"))?;
                let api = decoder
                    .decode_range(molecule, &stored)
                    .map_err(|_| invalid("source coordinate cannot be decoded"))?;
                let range = profile
                    .encode_value(&api)
                    .map_err(|_| invalid("source coordinate has no typed value"))?;
                let prefix = &scoped[..scoped.len() - bare.len()];
                let target = format!("{prefix}{}", hash_range_record_key(molecule, &hash, &range));
                bytes = bytes
                    .checked_add(scoped.len() + target.len())
                    .ok_or_else(|| invalid("mapping byte count overflows"))?;
                if target.len() > limits.max_key_bytes || bytes > limits.max_metadata_bytes {
                    return Err(invalid("coordinate mappings exceed their byte allowance"));
                }
                plan.coordinates.insert(scoped.clone(), target);
            }
            let [metadata, _] = node_bundle_storage_keys(molecule)?;
            let source = key_rows.get(metadata.as_str());
            plan.extras.push(HomeMutation {
                collection: "molecule_keys".into(),
                key: metadata,
                body: Some(typed_frozen_metadata(
                    molecule,
                    profile,
                    ConflictKeyIdentity::LegacyV1 {
                        hash: HashKeyEncoding::BlindV1,
                        range: source_range,
                    },
                    account_key,
                    source.map(|row| row.value.as_slice()),
                )?),
            });
        }
        let mut unique = std::collections::BTreeSet::new();
        for target in plan.coordinates.values() {
            if !unique.insert(target) {
                return Err(invalid("canonical coordinate collision"));
            }
        }
        Ok(plan)
    }

    pub(super) fn coordinate(&self, old: &str) -> StorageResult<&str> {
        self.coordinates
            .get(old)
            .map(String::as_str)
            .ok_or_else(|| invalid("a reference has no inventoried coordinate"))
    }
}
