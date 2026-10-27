//! Per-key molecule deletion.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

use super::super::counter::{TipChange, TipChangeSet, TipCommit};
use super::super::retention::SlotReleases;
use super::super::types::{MoleculeData, MoleculeHeader};
use super::super::{AtomStore, PerKeyRecord};

type MoleculeKeyRemoval<'a> = (
    &'a str,
    &'a MoleculeData,
    &'a [(String, String)],
    Option<&'a str>,
);

/// The keys and rows of the slot removals of one storage scope.
#[derive(Default)]
struct SlotRemovalPlan {
    /// Storage keys of the `mk:` records that the removal deletes.
    keys: Vec<String>,
    /// Molecule headers and generation-delete markers.
    rows: Vec<(String, Value)>,
}

impl AtomStore {
    /// Remove named slots from many per-key molecules in **one** batch with the
    /// counter changes that the removed tips cause.
    ///
    /// `removed_slots` are **storage-form** `(hash, range)` pairs: the segments
    /// the molecule is keyed by. `data` is the molecule **after** the removal;
    /// only its header (`version`/`updated_at`) is read.
    ///
    /// Purge of a multi-field schema (BoardCards has 24 fields) used to pay one
    /// delete and one header put per field, and measured ~8.6 s of exclusive
    /// hold for one record. Now one planner call per storage scope writes every
    /// tip delete, every counter change, every body delete at zero, and every
    /// header. The `mord:`/`moc:` order log stays as it is: removing `mk:` rows
    /// can only widen the `moc:{M} >= count(mk:{M}:…)` margin that
    /// `db order-log-audit` checks, never invert it.
    pub(crate) async fn remove_molecules_keys_batch(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
    ) -> Result<TipCommit, SchemaError> {
        self.remove_slots(removals, Vec::new(), false).await
    }

    /// [`Self::remove_molecules_keys_batch`] for an erase (delete, purge, TTL
    /// sweep), with caller rows (delete barriers, ledger rows) that join the
    /// same batch. The erase ends the retention of the kept conflict records of
    /// the slots it removes: the same batch releases them (`retention`).
    pub(crate) async fn remove_molecules_keys_with_rows(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
        extra_rows: Vec<(String, Value)>,
    ) -> Result<TipCommit, SchemaError> {
        self.remove_slots(removals, extra_rows, true).await
    }

    async fn remove_slots(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
        extra_rows: Vec<(String, Value)>,
        release_records: bool,
    ) -> Result<TipCommit, SchemaError> {
        let current = self.read_removed_slots(removals).await?;
        let (generation_markers, _generation_guards) =
            self.removal_generation_markers(removals).await?;
        let mut plans: BTreeMap<Option<&str>, SlotRemovalPlan> = BTreeMap::new();
        for (removal, needs_marker) in removals.iter().zip(generation_markers) {
            let plan = plans.entry(removal.3).or_default();
            Self::plan_molecule_removal(removal, &current, needs_marker, plan)?;
        }
        let mut extra_rows = Some(extra_rows);
        let mut total = TipCommit::default();
        for (scope, plan) in plans {
            let mut rows = plan.rows;
            rows.extend(extra_rows.take().unwrap_or_default());
            let slot_releases = if release_records {
                self.erase_release_requests(scope, removals).await?
            } else {
                SlotReleases::default()
            };
            rows.extend(slot_releases.rows);
            let commit = self
                .commit_tip_changes(TipChangeSet {
                    scope,
                    tips: plan
                        .keys
                        .into_iter()
                        .map(|key| TipChange { key, new: None })
                        .collect(),
                    bodies: &[],
                    body_source: None,
                    records: Vec::new(),
                    releases: slot_releases.releases,
                    rows,
                    deletes: Vec::new(),
                    guarded_deletes: Vec::new(),
                })
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!("Failed to remove slots from molecules: {e}"))
                })?;
            self.retention_state().note_purge(commit.records_released);
            total.absorb(commit);
        }
        if let Some(rows) = extra_rows.filter(|rows| !rows.is_empty()) {
            // No slot was named. Keep the caller rows in the request batch.
            let commit = self
                .commit_tip_changes(TipChangeSet {
                    scope: None,
                    tips: Vec::new(),
                    bodies: &[],
                    body_source: None,
                    records: Vec::new(),
                    releases: Vec::new(),
                    rows,
                    deletes: Vec::new(),
                    guarded_deletes: Vec::new(),
                })
                .await?;
            total.absorb(commit);
        }
        Ok(total)
    }

    /// An immutable base needs a shadow marker. An in-progress generation cut
    /// does too, even before its first pointer exists. Try the cleanup guard
    /// without waiting: a held read guard means preparation may still publish
    /// an older cut, so keep the marker. Otherwise hold the exclusive guard
    /// through the counter commit, which prevents a cut across this removal.
    /// Delete replay barriers remain independent rows in that same batch.
    async fn removal_generation_markers(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
    ) -> Result<
        (
            Vec<bool>,
            Vec<std::sync::Arc<tokio::sync::OwnedRwLockWriteGuard<()>>>,
        ),
        SchemaError,
    > {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        let mut guards = Vec::new();
        let protected: Vec<_> = removals
            .iter()
            .map(|removal| {
                let key = build_storage_key(
                    removal.3,
                    &molecule_key_codec::molecule_generation_pointer_key(removal.0),
                );
                match crate::storage::disk_pack::transaction::lease(
                    runtime.as_deref(),
                    format!("delete-generation:{key}"),
                    || {
                        self.generation_cleanup_lock(removal.0, removal.3)
                            .try_write_owned()
                            .ok()
                    },
                )? {
                    Some(guard) => {
                        guards.push(guard);
                        Ok(true)
                    }
                    None => Ok(false),
                }
            })
            .collect::<Result<_, SchemaError>>()?;
        let keys: Vec<_> = removals
            .iter()
            .map(|removal| {
                build_storage_key(
                    removal.3,
                    &molecule_key_codec::molecule_generation_pointer_key(removal.0),
                )
            })
            .collect();
        let pointers: Vec<Option<super::super::MoleculeGenerationPointer>> =
            self.main_store.get_items(&keys).await.map_err(|error| {
                SchemaError::InvalidData(format!("read removal generation pointers: {error}"))
            })?;
        Ok((
            pointers
                .into_iter()
                .zip(protected)
                .map(|(pointer, protected)| pointer.is_some() || !protected)
                .collect(),
            guards,
        ))
    }

    /// The releases for the kept conflict records of the slots that the
    /// removals of one storage scope name.
    async fn erase_release_requests(
        &self,
        scope: Option<&str>,
        removals: &[MoleculeKeyRemoval<'_>],
    ) -> Result<SlotReleases, SchemaError> {
        let slots: Vec<_> = removals
            .iter()
            .filter(|removal| removal.3 == scope)
            .map(|removal| (removal.0, removal.2))
            .collect();
        self.slot_release_requests(scope, &slots).await
    }

    /// The physical record of every slot the removals name, in one grouped read
    /// per storage prefix, keyed by full storage key. A slot without a record
    /// is absent from the map.
    ///
    /// This replaces one read per slot: each of those reads is a point get plus
    /// a generation pointer read, and under a small warm set each one loads a
    /// hash group. `load_exact_per_key_records` reads the same rows in one
    /// `get_items` and reads each molecule's generation pointer once, with the
    /// same merge rules as [`Self::load_exact_per_key_records`].
    async fn read_removed_slots(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
    ) -> Result<HashMap<String, PerKeyRecord>, SchemaError> {
        let mut by_prefix: HashMap<Option<&str>, Vec<String>> = HashMap::new();
        for (molecule_uuid, _, removed_slots, storage_prefix) in removals {
            for (hash, range) in *removed_slots {
                by_prefix.entry(*storage_prefix).or_default().push(
                    molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range),
                );
            }
        }
        let mut current = HashMap::new();
        for (storage_prefix, base_keys) in by_prefix {
            let records = self
                .load_exact_per_key_records(storage_prefix, &base_keys)
                .await?;
            for (base_key, record) in base_keys.into_iter().zip(records) {
                if let Some(record) = record {
                    current.insert(build_storage_key(storage_prefix, &base_key), record);
                }
            }
        }
        Ok(current)
    }

    /// Plan the key deletes and header bump for one molecule's removed slots.
    fn plan_molecule_removal(
        removal: &MoleculeKeyRemoval<'_>,
        current: &HashMap<String, PerKeyRecord>,
        has_generation: bool,
        plan: &mut SlotRemovalPlan,
    ) -> Result<(), SchemaError> {
        let (molecule_uuid, data, removed_slots, storage_prefix) = removal;
        if removed_slots.is_empty() {
            return Ok(());
        }
        plan.keys.reserve(removed_slots.len());
        for (hash, range) in *removed_slots {
            Self::plan_slot_removal(
                molecule_uuid,
                hash,
                range,
                *storage_prefix,
                current,
                has_generation,
                plan,
            )?;
        }

        let header = MoleculeHeader {
            version: data.version(),
            updated_at: data.updated_at(),
        };
        let header_value = serde_json::to_value(&header)
            .map_err(|e| SchemaError::InvalidData(format!("serialize molecule header: {e}")))?;
        plan.rows.push((
            build_storage_key(
                *storage_prefix,
                &molecule_key_codec::header_key(molecule_uuid),
            ),
            header_value,
        ));
        Ok(())
    }

    /// Delete one live slot, and hide its immutable base only when a generation
    /// exists. The same stored winner still drives the atom counter release.
    fn plan_slot_removal(
        molecule_uuid: &str,
        hash: &str,
        range: &str,
        storage_prefix: Option<&str>,
        current: &HashMap<String, PerKeyRecord>,
        has_generation: bool,
        plan: &mut SlotRemovalPlan,
    ) -> Result<(), SchemaError> {
        let base_key = molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range);
        if let Some(current) = current
            .get(&build_storage_key(storage_prefix, &base_key))
            .filter(|_| has_generation)
        {
            let marker = super::super::MoleculeGenerationDelete {
                shadow: current.entry.clone(),
            };
            plan.rows.push((
                build_storage_key(
                    storage_prefix,
                    &molecule_key_codec::molecule_generation_delete_key(molecule_uuid, hash, range),
                ),
                serde_json::to_value(marker).map_err(|error| {
                    SchemaError::InvalidData(format!("serialize generation delete marker: {error}"))
                })?,
            ));
        }
        plan.keys.push(build_storage_key(storage_prefix, &base_key));
        Ok(())
    }
}
