//! Per-key molecule deletion.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use std::collections::HashMap;

use super::super::types::{MoleculeData, MoleculeHeader};
use super::super::AtomStore;

type MoleculeKeyRemoval<'a> = (
    &'a str,
    &'a MoleculeData,
    &'a [(String, String)],
    Option<&'a str>,
);

impl AtomStore {
    /// Remove EXACTLY the named slots from a per-key molecule, leaving every
    /// other slot's row untouched.
    ///
    /// `removed_slots` are **storage-form** `(hash, range)` pairs — the segments
    /// the molecule itself is keyed by, which is what
    /// `per_key_storage_records` emits under
    /// [`MoleculeKeyDomain::Storage`](super::super::MoleculeKeyDomain). `data`
    /// is the molecule **after** the removal; only its header
    /// (`version`/`updated_at`) is read.
    ///
    /// # Why this exists
    ///
    /// The only removal primitive used to be
    /// `delete_per_key_molecule` followed by a full rewrite, and both
    /// halves are O(molecule cardinality). That is the right cost for retiring
    /// a molecule and a catastrophic one for retiring a *slot*: measured on the
    /// primary 2026-08-09, purging ONE record from a 24-field schema whose
    /// field molecules hold ~19.2k keys apiece rewrote ~347.7k `mk:` rows and
    /// grew the `tips` plane by ~442 MiB — for one record. The store grew only
    /// while a purge ran and was flat otherwise, so the reclaim path was the
    /// growth path. Card
    /// `lastdb-purge-rewrites-the-whole-molecule-to-remove-one-key`.
    ///
    /// This is the removal counterpart to
    /// [`Self::store_molecule_changed_keys`], which had already made the
    /// *upsert* path O(touched keys) and explicitly left removal to the caller.
    ///
    /// # What is deliberately NOT touched
    ///
    /// The `mord:`/`moc:` order log. `MoleculeHashRange::remove_atom_uuid` does
    /// not prune `update_order` — it only bumps `version`/`updated_at` — so the
    /// full rewrite this replaces wrote the order log back **unchanged**.
    /// Leaving it alone is therefore the identical end state, and it keeps the
    /// `moc:{M} >= count(mk:{M}:…)` invariant `db order-log-audit` is defined
    /// against: removing `mk:` rows can only widen that margin, never invert it.
    ///
    /// # Ordering
    ///
    /// Rows are deleted BEFORE the header is bumped. A crash between the two
    /// leaves the erasure durable and only the header version behind, which
    /// self-heals on the next write to the molecule and cannot resurrect a
    /// removed row: `load_molecule_per_key` scans the `mk:` range, so a slot
    /// with no row is absent whatever the header says. Erasure durability
    /// outranks header freshness for a compliance verb. This is strictly more
    /// crash-safe than the delete-everything-then-rewrite path it replaces,
    /// where the same window loses the whole molecule.
    pub(crate) async fn remove_molecule_keys(
        &self,
        molecule_uuid: &str,
        data: &MoleculeData,
        removed_slots: &[(String, String)],
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        self.remove_molecules_keys_batch(&[(molecule_uuid, data, removed_slots, storage_prefix)])
            .await
    }

    /// Remove named slots from many per-key molecules in **one** durable
    /// delete and **one** durable header put.
    ///
    /// Purge of a multi-field schema (BoardCards has 24 fields) used to call
    /// [`Self::remove_molecule_keys`] once per field: each call paid its own
    /// `batch_delete` + header `batch_put_items`. Measured on the primary
    /// 2026-08-17, that exclusive `purge_commit` hold averaged ~8.6 s for a
    /// single-record purge while `records_purged=1` — the per-field durable
    /// round trips were pure overhead once the slot lists were already in
    /// hand. The end state is identical: every named `mk:` / retired-marker
    /// key is gone and each molecule header is bumped to the post-removal
    /// `version`/`updated_at`.
    ///
    /// Empty `removed_slots` entries are skipped. An all-empty batch is a
    /// no-op (no store call).
    pub(crate) async fn remove_molecules_keys_batch(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
    ) -> Result<(), SchemaError> {
        self.remove_molecules_keys_with_extra_batch(removals, Vec::new())
            .await
    }

    /// Remove exact molecule slots and caller-supplied derived rows in one
    /// durable delete batch, then bump the affected molecule headers.
    ///
    /// Barrierless purge uses `extra_keys` for the reverse-reference rows that
    /// describe the removed tips and tip versions. The authoritative row and
    /// its shadow edge therefore cannot survive in opposite states.
    pub(crate) async fn remove_molecules_keys_with_extra_batch(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
        extra_keys: Vec<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        self.remove_molecules_keys_with_extra_and_trailing_batch(removals, extra_keys, Vec::new())
            .await
    }

    /// Remove source rows before the caller-supplied trailing derived rows.
    ///
    /// Compact reverse edges use `trailing_keys`. LastStore applies these
    /// deletes after every source delete and before one durability barrier.
    /// A crash can retain an extra edge, but it cannot leave a source row with
    /// its compact edge missing.
    pub(crate) async fn remove_molecules_keys_with_extra_and_trailing_batch(
        &self,
        removals: &[MoleculeKeyRemoval<'_>],
        mut extra_keys: Vec<Vec<u8>>,
        mut trailing_keys: Vec<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        let _coverage = self.invalidate_coverage_during(
            removals
                .iter()
                .filter(|(_, _, _, prefix)| prefix.is_none())
                .map(|(uuid, _, _, _)| *uuid),
            None,
        );
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut headers: Vec<(String, serde_json::Value)> = Vec::new();
        let mut removed_tip_edges: Vec<(Option<String>, super::super::AtomRefEdge)> = Vec::new();
        let mut molecule_count = 0usize;
        let mut slot_count = 0usize;

        for (molecule_uuid, data, removed_slots, storage_prefix) in removals {
            if removed_slots.is_empty() {
                continue;
            }
            molecule_count += 1;
            slot_count += removed_slots.len();
            keys.reserve(removed_slots.len() * 3);
            for (hash, range) in *removed_slots {
                let base_key =
                    molecule_key_codec::hash_range_record_key(molecule_uuid, hash, range);
                if let Some((_, current)) = self
                    .get_per_key_unfiltered(&base_key, *storage_prefix)
                    .await?
                {
                    let edge = Self::live_tip_ref_edge(molecule_uuid, hash, range, &current.entry);
                    if let Ok(edge_key) = edge.storage_key_v2(*storage_prefix) {
                        trailing_keys.push(edge_key.into_bytes());
                        removed_tip_edges.push((storage_prefix.map(str::to_string), edge));
                    }
                    let marker = super::super::MoleculeGenerationDelete {
                        shadow: current.entry,
                    };
                    headers.push((
                        build_storage_key(
                            *storage_prefix,
                            &molecule_key_codec::molecule_generation_delete_key(
                                molecule_uuid,
                                hash,
                                range,
                            ),
                        ),
                        serde_json::to_value(marker).map_err(|error| {
                            SchemaError::InvalidData(format!(
                                "serialize generation delete marker: {error}"
                            ))
                        })?,
                    ));
                }
                keys.push(build_storage_key(*storage_prefix, &base_key).into_bytes());
                // Derived markers for this slot. Both features are retired
                // (`HASH_RANGE_PAGE_INDEX_ENABLED` / `HASH_RANGE_HASH_KEY_LOOKUP_ENABLED`
                // are `false`), so nothing co-writes them any more — but a store
                // written before they were retired still carries rows, and the
                // whole-molecule delete this replaces reaped them. Deleting a
                // marker can only cost a reader the derived shortcut and send it to
                // the authoritative `mk:` scan, which is already the missing-marker
                // fallback; KEEPING one would leave a row that names a purged
                // record. For a compliance verb that trade is one-sided.
                keys.push(
                    build_storage_key(
                        *storage_prefix,
                        &molecule_key_codec::hash_range_page_index_key(molecule_uuid, hash, range),
                    )
                    .into_bytes(),
                );
                keys.push(
                    build_storage_key(
                        *storage_prefix,
                        &molecule_key_codec::hash_key_lookup_key(molecule_uuid, hash),
                    )
                    .into_bytes(),
                );
            }

            let header = MoleculeHeader {
                version: data.version(),
                updated_at: data.updated_at(),
            };
            let header_value = serde_json::to_value(&header)
                .map_err(|e| SchemaError::InvalidData(format!("serialize molecule header: {e}")))?;
            headers.push((
                build_storage_key(
                    *storage_prefix,
                    &molecule_key_codec::header_key(molecule_uuid),
                ),
                header_value,
            ));
        }

        keys.append(&mut extra_keys);
        keys.sort_unstable();
        keys.dedup();
        trailing_keys.sort_unstable();
        trailing_keys.dedup();
        keys.retain(|key| trailing_keys.binary_search(key).is_err());

        if keys.is_empty() && trailing_keys.is_empty() {
            return Ok(());
        }

        let mut ref_atoms: Vec<String> = removed_tip_edges
            .iter()
            .map(|(_, edge)| edge.atom_uuid.clone())
            .collect();
        ref_atoms.sort_unstable();
        ref_atoms.dedup();
        let _count_guards = self.lock_atom_ref_counts(&ref_atoms).await;
        let mut count_mutations = Vec::new();
        let mut by_prefix: HashMap<Option<String>, HashMap<Vec<u8>, super::super::AtomRefEdge>> =
            HashMap::new();
        for (prefix, edge) in removed_tip_edges {
            let key = edge.storage_key_v2(prefix.as_deref())?.into_bytes();
            by_prefix.entry(prefix).or_default().insert(key, edge);
        }
        for (prefix, inactive) in by_prefix {
            count_mutations.extend(
                self.atom_live_ref_count_mutations(&HashMap::new(), &inactive, prefix.as_deref())
                    .await?,
            );
        }

        let result = if trailing_keys.is_empty() && count_mutations.is_empty() {
            self.main_store.inner().batch_delete(keys).await
        } else {
            let mut mutations =
                Vec::with_capacity(keys.len() + count_mutations.len() + trailing_keys.len());
            mutations.extend(keys.into_iter().map(KvMutation::delete));
            mutations.extend(count_mutations);
            mutations.extend(trailing_keys.into_iter().map(KvMutation::delete));
            self.main_store.inner().batch_mutate(mutations).await
        };
        result.map_err(|e| {
            SchemaError::InvalidData(format!(
                "Failed to remove {slot_count} slot(s) from {molecule_count} molecule(s): {e}"
            ))
        })?;

        self.main_store.batch_put_items(headers).await.map_err(|e| {
            SchemaError::InvalidData(format!(
                "Failed to bump headers after removing slots from {molecule_count} molecule(s): {e}"
            ))
        })
    }
}
