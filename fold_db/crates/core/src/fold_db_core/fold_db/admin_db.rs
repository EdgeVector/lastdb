//! Owner-facing DB admin: inventory + history clear + atom GC + photo blob migrate.

use crate::db_operations::{
    AtomDeleteLedgerEntry, AtomGcReport, AtomPartitionRekeyOptions, AtomPartitionRekeyReport,
    DanglingTipRepairOptions, DanglingTipRepairReport, DbInventory, DroppedSchemaReapCursor,
    HistoryClearReport, LegacyKeyForkAudit, LegacyRefBlobPurgeReport, OrderLogAudit,
    ProteinGcReport, SchemaCurrentStorageReport, SchemaIdxPurgeReport, SchemaLogicalStorageReport,
    SchemaLogicalStorageRow, SchemaRecordKey, SchemaRecordKeysReport, SchemaStorageReport,
    SupersededVersionRetentionCheckpoint, SupersededVersionRetentionOptions,
    SupersededVersionRetentionReport, ThinTipMigrateReport, TipHistoryDrainCheckpoint,
    TipHistoryDrainOptions, TipHistoryDrainReport, TombstoneFlagBackfillReport,
    LIVE_ATTRIBUTION_EPOCH_ID,
};
use crate::error::FoldDbError;
#[cfg(feature = "sharing")]
use crate::hex::hex_lower;
use crate::schema::types::Schema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

#[cfg(feature = "sharing")]
use crate::access::AccessContext;
#[cfg(feature = "sharing")]
use crate::schema::types::field::HashRangeFilter;
#[cfg(feature = "sharing")]
use crate::schema::types::key_value::KeyValue;
#[cfg(feature = "sharing")]
use crate::schema::types::operations::{Mutation, MutationType, Query};
#[cfg(feature = "sharing")]
use base64::Engine;
#[cfg(feature = "sharing")]
use serde_json::Value;
#[cfg(feature = "sharing")]
use sha2::{Digest, Sha256};

use super::FoldDB;

mod atom_gc;
mod order_log;
mod photo_blobs;
mod storage_reports;

/// Wrap an inner admin-op [`SchemaError`] for the owner socket **without
/// throwing away a caller fault**.
///
/// Every wrapper in this file used to end with
/// `.map_err(|e| FoldDbError::Database(format!("op: {e}")))`. The label reads
/// well in a log line and is destructive on the wire: `Database` is the one
/// [`FoldDbError`] variant the socket's `HostError` mapping has no typed arm
/// for, so it collapses to `500 Internal Server Error`. A malformed
/// `after_key` and an unknown schema name are both pure caller input, and both
/// arrive here already typed one layer down — the label erased that and
/// reported the caller's own typo as a server bug. The socket logs every 5xx at
/// ERROR and observability promotes each into its own Sentry issue, so a single
/// bad cursor became a storm with zero users affected (the same shape as Sentry
/// `7620011366` and `7641868650`).
///
/// So: classify, then label. A variant that can only mean "the request was
/// wrong" keeps its type and gets its own 4xx. Everything else — including
/// [`SchemaError::InvalidData`], which this codebase uses for store and IO
/// failures as well as bad input — keeps the operation label and stays a `500`.
/// Under-classifying costs a noisy 500; over-classifying tells a caller its
/// request was permanently malformed when the store merely hiccuped, and the
/// caller then drops the work. Prefer the noisy 500.
fn admin_op_error(operation: &str, error: crate::schema::SchemaError) -> FoldDbError {
    use crate::schema::SchemaError as SE;
    match error {
        // Unambiguously the caller's request: a name that does not resolve, a
        // cursor outside the keyspace, a denial, a lost CAS race, an oversized
        // payload, a full disk, transient capture backpressure. Each has
        // exactly one typed status.
        caller_fault @ (SE::NotFound(_)
        | SE::InvalidCursor(_)
        | SE::InvalidField(_)
        | SE::Blocked(_)
        | SE::PermissionDenied(_)
        | SE::InvalidPermission(_)
        | SE::CatalogMembershipDenied { .. }
        | SE::TransportNotAttested { .. }
        | SE::CasConflict { .. }
        | SE::AtomContentTooLarge { .. }
        | SE::StorageFull { .. }
        | SE::CaptureQueueFull { .. }) => FoldDbError::Schema(caller_fault),
        // Overloaded or internal: keep the operation label and stay a 500.
        other => FoldDbError::Database(format!("{operation}: {other}")),
    }
}

/// Report from migrating Photo `file_bytes` into the local CAS blob tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhotoBlobMigrateReport {
    pub dry_run: bool,
    pub photos_seen: u64,
    pub photos_migrated: u64,
    pub photos_skipped: u64,
    pub photos_failed: u64,
    pub bytes_to_cas: u64,
    pub errors: Vec<String>,
}

/// One bounded, resumable legacy-tombstone drain pass.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LegacyTombstoneDrainReport {
    pub dry_run: bool,
    pub keys_scanned: u64,
    pub keys_unreadable: u64,
    pub atoms_fetched: u64,
    pub atoms_missing: u64,
    pub tombstones_found: u64,
    pub tombstones_drained: u64,
    pub unowned_tombstones: u64,
    pub search_tombstones_queued: u64,
    pub per_schema_drained: BTreeMap<String, u64>,
    pub more_remaining: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

impl FoldDB {
    /// Drain one bounded page of plane residue toward its canonical
    /// collection, on the live store, over the owner socket.
    ///
    /// This is what lets the residue that dual-read attribution names (for
    /// example `sync_conflicts` serving 87% of legacy hits, 2026-07-30) be
    /// drained without an offline exclusive open of the primary home. The
    /// underlying pass is bounded, resumable, dry-run by default, and
    /// copy-then-delete per key — see
    /// [`crate::storage::laststore::LastStoreNamespacedStore::drain_plane_residue_collection`].
    pub async fn drain_plane_residue(
        &self,
        options: crate::storage::laststore::PlaneResidueDrainOptions,
    ) -> Result<crate::storage::laststore::PlaneResidueDrainReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .drain_plane_residue(options)
            .await
            .map_err(|e| FoldDbError::Database(format!("plane-residue drain: {e}")))
    }

    /// Rewrite sealed values in one plane to the requested ENB target policy.
    ///
    /// Dry-run is the default. Never point this at the live primary from a
    /// routine; owner-gated there. Same-key replace; no dual-key copies.
    pub async fn reseal_at_rest(
        &self,
        options: crate::storage::ResealAtRestOptions,
    ) -> Result<crate::storage::ResealAtRestReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .reseal_at_rest(options)
            .await
            .map_err(|e| FoldDbError::Database(format!("reseal-at-rest: {e}")))
    }

    /// Remove un-enveloped rows from one encrypted plane. They already read as
    /// absent (`decision-2026-09-14-drop-dual-read-unsealed-is-gone`); this
    /// returns their bytes.
    ///
    /// Dry-run is the default. Owner-gated. Refuses every plaintext-by-policy
    /// namespace by name. On request only: a non-zero count on a healthy home
    /// is a signal for a human, not a job for a schedule.
    pub async fn reap_unsealed(
        &self,
        options: crate::storage::ReapUnsealedOptions,
    ) -> Result<crate::storage::ReapUnsealedReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .reap_unsealed(options)
            .await
            .map_err(|e| FoldDbError::Database(format!("reap-unsealed: {e}")))
    }

    /// Compact one allowlisted LastStore collection (default dry-run).
    ///
    /// Reclaims superseded segment bodies on allowlisted planes. Atom execute
    /// records durable retirement provenance for the next backup-manifest cut.
    pub async fn compact_collection(
        &self,
        collection: &str,
        dry_run: bool,
    ) -> Result<crate::storage::laststore::CollectionCompactReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .compact_collection(crate::storage::laststore::CollectionCompactOptions {
                collection: collection.to_string(),
                dry_run,
                seed_committed_history: collection == "atoms" && !dry_run,
            })
            .await
            .map_err(|e| FoldDbError::Database(format!("compact collection: {e}")))
    }

    #[cfg(feature = "cloud-sync")]
    /// Read-only, bounded pin-log plane audit (`lastdb db pin-log-audit`).
    ///
    /// Opens `sync_pin_log`, loads durable published-F maps, and classifies
    /// each entry row as confirmed-orphan (frontier ≤ writer HWM) or
    /// genuinely pending. Never deletes.
    pub async fn audit_pin_log(
        &self,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<crate::sync::engine::PinLogPlaneReport, FoldDbError> {
        let store = self
            .db_ops()
            .namespaced_store()
            .open_namespace(crate::sync::engine::PIN_LOG_NAMESPACE)
            .await
            .map_err(|e| FoldDbError::Database(format!("open sync_pin_log: {e}")))?;
        let max_keys = max_keys.unwrap_or(crate::sync::engine::PIN_LOG_OPERATOR_KEYS_PER_CALL);
        crate::sync::engine::audit_pin_log_plane(store.as_ref(), max_keys, after_key)
            .await
            .map_err(|e| admin_op_error("pin-log-audit", e))
    }

    /// Clear mutation history rows. `schema` = None means all schemas.
    /// `keep_last_per_key`: `0` purges all history (latest-only / tip is current);
    /// `>= 1` keeps the newest N events per field key.
    pub async fn clear_mutation_history(
        &self,
        schema: Option<&str>,
        keep_last_per_key: usize,
        dry_run: bool,
    ) -> Result<HistoryClearReport, FoldDbError> {
        let names: Vec<String> = if let Some(s) = schema {
            vec![s.to_string()]
        } else {
            self.db_ops()
                .get_all_schemas()
                .await
                .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?
                .into_keys()
                .collect()
        };
        let field_mols = self.schema_field_molecules(&names);
        self.db_ops()
            .atoms()
            .clear_mutation_history(&field_mols, keep_last_per_key, dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("clear_mutation_history: {e}")))
    }

    /// Delete all `schemaidx:` marker keys from the live store, forcing
    /// a later schema listing to rebuild the secondary index.
    pub async fn purge_schemaidx(&self) -> Result<SchemaIdxPurgeReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .purge_schemaidx(None)
            .await
            .map_err(|e| FoldDbError::Database(format!("purge_schemaidx: {e}")))
    }

    /// One bounded, resumable pass that drains existing live tip-version
    /// chains without collection compaction. See
    /// [`crate::db_operations::AtomStore::drain_tip_history_chains`].
    pub async fn drain_tip_history_chains(
        &self,
        options: TipHistoryDrainOptions,
    ) -> Result<TipHistoryDrainReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .drain_tip_history_chains(options)
            .await
            .map_err(|e| admin_op_error("drain_tip_history_chains", e))
    }

    /// Drain from the durable automatic-reclaim checkpoint and advance it.
    pub async fn drain_tip_history_chains_from_checkpoint(
        &self,
        options: TipHistoryDrainOptions,
    ) -> Result<(TipHistoryDrainReport, TipHistoryDrainCheckpoint), FoldDbError> {
        self.db_ops()
            .atoms()
            .drain_tip_history_chains_from_checkpoint(options)
            .await
            .map_err(|e| {
                FoldDbError::Database(format!("drain_tip_history_chains_from_checkpoint: {e}"))
            })
    }

    /// Read the durable tip-history drain checkpoint.
    pub async fn tip_history_drain_checkpoint(
        &self,
    ) -> Result<TipHistoryDrainCheckpoint, FoldDbError> {
        self.db_ops()
            .atoms()
            .tip_history_drain_checkpoint(None)
            .await
            .map_err(|e| FoldDbError::Database(format!("tip_history_drain_checkpoint: {e}")))
    }

    /// One bounded pass that drops expired `tv:` nodes on live heads and keeps
    /// versions inside the 7-day window. Tombstoned heads are skipped.
    pub async fn retain_superseded_versions(
        &self,
        options: SupersededVersionRetentionOptions,
    ) -> Result<SupersededVersionRetentionReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .retain_superseded_versions(options)
            .await
            .map_err(|e| admin_op_error("retain_superseded_versions", e))
    }

    /// Retention pass from the durable checkpoint (resume / settle).
    pub async fn retain_superseded_versions_from_checkpoint(
        &self,
        options: SupersededVersionRetentionOptions,
    ) -> Result<
        (
            SupersededVersionRetentionReport,
            SupersededVersionRetentionCheckpoint,
        ),
        FoldDbError,
    > {
        self.db_ops()
            .atoms()
            .retain_superseded_versions_from_checkpoint(options)
            .await
            .map_err(|e| {
                FoldDbError::Database(format!("retain_superseded_versions_from_checkpoint: {e}"))
            })
    }

    /// Drop the legacy `metadata` hash group that held the keep-small
    /// snapshot, without loading it (`lastdb db reclaim-keep-small-legacy`).
    ///
    /// The snapshot moved to its own `keep_small` plane on 2026-09-21 after a
    /// per-write flush (fold #2127) filled its `metadata` group with 39 GB of
    /// superseded copies — 6,782 segments on the primary — which the first
    /// write after boot then loaded whole, crossing the 16 GiB memory guard
    /// and restarting the daemon every 7-17 minutes. Nothing reads the old key
    /// now, `compact` cannot touch a group it cannot load, and the cold-load
    /// cap refuses the load. Dropping the directory is the only byte-return
    /// path. The store proves from the group's id sidecar that it holds
    /// nothing but `keep_small:meters` and refuses otherwise. Dry-run by
    /// default.
    pub fn reclaim_keep_small_legacy(
        &self,
        dry_run: bool,
    ) -> Result<crate::storage::laststore::DeadHashGroupDropReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .drop_dead_hash_group(crate::storage::laststore::DeadHashGroupDropOptions {
                collection: "metadata".to_string(),
                expected_only_id: crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY.to_string(),
                dry_run,
            })
            .map_err(|e| FoldDbError::Database(format!("reclaim_keep_small_legacy: {e}")))
    }

    /// Drop the current `keep_small` hash group that holds only the local
    /// storage-meter snapshot, without loading it.
    ///
    /// The snapshot is node-local, capture-skipped bookkeeping. A missing row
    /// starts the normal bootstrap path, so this is safe only after the store
    /// proves that the group sidecar names exactly `keep_small:meters`.
    /// This provides a byte-return path when the group's superseded snapshots
    /// exceed the cold-group load cap and generic compact must refuse it.
    pub fn reclaim_keep_small_snapshot(
        &self,
        dry_run: bool,
    ) -> Result<crate::storage::laststore::DeadHashGroupDropReport, FoldDbError> {
        self.db_ops()
            .namespaced_store()
            .drop_dead_hash_group(crate::storage::laststore::DeadHashGroupDropOptions {
                collection: crate::db_operations::KEEP_SMALL_SNAPSHOT_COLLECTION.to_string(),
                expected_only_id: crate::db_operations::KEEP_SMALL_SNAPSHOT_KEY.to_string(),
                dry_run,
            })
            .map_err(|e| FoldDbError::Database(format!("reclaim_keep_small_snapshot: {e}")))
    }

    /// Measure (and optionally delete) legacy `ref:` whole-molecule blobs —
    /// pre-per-key layout residue the live read path never dual-reads.
    /// Dry-run by default.
    pub async fn purge_ref_blobs(
        &self,
        dry_run: bool,
    ) -> Result<LegacyRefBlobPurgeReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .purge_ref_blobs(dry_run, None)
            .await
            .map_err(|e| FoldDbError::Database(format!("purge_ref_blobs: {e}")))
    }

    /// Sample the locator-only tip population (bounded; always read-only).
    ///
    /// See [`crate::db_operations::AtomStore::probe_locator_only_population`].
    pub async fn probe_locator_only_population(
        &self,
        options: crate::db_operations::LocatorOnlyProbeOptions,
    ) -> Result<crate::db_operations::LocatorOnlyPopulationReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .probe_locator_only_population(options)
            .await
            .map_err(|e| admin_op_error("probe_locator_only_population", e))
    }

    /// Remove `mk:` tips whose atom body is unreachable by every read route.
    ///
    /// Dry-run by default. Execute revalidates each tip/body immediately before
    /// rewriting the molecule, and ledgers the repair batch before mutation.
    pub async fn repair_dangling_tips(
        &self,
        options: DanglingTipRepairOptions,
    ) -> Result<DanglingTipRepairReport, FoldDbError> {
        let mut report = self
            .db_ops()
            .atoms()
            .repair_dangling_tips(options)
            .await
            .map_err(|e| admin_op_error("repair_dangling_tips", e))?;
        // Join each unresolved tip's molecule against the live schema catalog
        // so an operator can answer "which schema is producing these" from
        // this report alone, instead of a separate molecule-keys probe per
        // molecule uuid.
        if !report.unresolved.is_empty() {
            let by_molecule = self.molecule_schema_index().await;
            for row in &mut report.unresolved {
                row.schema = by_molecule.get(row.molecule_uuid.as_str()).cloned();
            }
        }
        Ok(report)
    }

    /// [`Self::repair_dangling_tips`] limited to one schema's field molecules,
    /// and optionally to one API HashKey inside them.
    ///
    /// `requested_schema` matches the stored catalog name, `schema.name`, the
    /// `descriptive_name` (e.g. `BoardCards`), or the identity hash. Every
    /// matching schema is included: a descriptive name can span several
    /// identities, and dangling rows of an older identity are still damage.
    /// No match is an error, not an empty pass, so a typo cannot report a
    /// clean `completed` walk.
    ///
    /// The walk is one `mk:{M}:` prefix range per field molecule (see
    /// [`crate::db_operations::DanglingTipRepairScope`]), so its cost is the
    /// schema's rows, not the store's.
    pub async fn repair_dangling_tips_for_schema(
        &self,
        mut options: DanglingTipRepairOptions,
        requested_schema: &str,
        hash_key: Option<String>,
    ) -> Result<DanglingTipRepairReport, FoldDbError> {
        let schemas = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
        let mut names: Vec<String> = schemas
            .iter()
            .filter(|(stored, schema)| {
                stored.as_str() == requested_schema
                    || schema.name == requested_schema
                    || schema.descriptive_name.as_deref() == Some(requested_schema)
                    || schema.identity_hash.as_deref() == Some(requested_schema)
            })
            .map(|(stored, _)| stored.clone())
            .collect();
        names.sort();
        names.dedup();
        if names.is_empty() {
            return Err(FoldDbError::Database(format!(
                "repair-dangling-tips: no loaded schema matches {requested_schema:?}"
            )));
        }
        let mut molecule_uuids: Vec<String> = self
            .schema_field_molecules(&names)
            .into_iter()
            .flat_map(|(_, mols)| mols)
            .collect();
        molecule_uuids.sort();
        molecule_uuids.dedup();
        options.key_window = None;
        options.scope = Some(crate::db_operations::DanglingTipRepairScope {
            molecule_uuids,
            hash_key,
        });
        let mut report = self.repair_dangling_tips(options).await?;
        if let Some(scope) = report.scope.as_mut() {
            scope.schemas = names;
        }
        Ok(report)
    }

    /// Read the durable atom hard-delete ledger, oldest first.
    ///
    /// The audit counterpart to [`Self::gc_orphan_atoms`] and the purge verb:
    /// every hard-delete batch either appears here or did not happen. `limit`
    /// of 0 means unbounded. See [`crate::db_operations::delete_ledger`] for
    /// what a row does and does not carry.
    pub async fn list_atom_delete_ledger(
        &self,
        limit: usize,
    ) -> Result<Vec<AtomDeleteLedgerEntry>, FoldDbError> {
        self.db_ops()
            .atoms()
            .list_atom_delete_ledger(None, limit)
            .await
            .map_err(|e| FoldDbError::Database(format!("list_atom_delete_ledger: {e}")))
    }

    /// Audit `KeyMetadata.tombstoned` against atom content, optionally stamping
    /// the flag onto legacy `mk:` records whose content is already a tombstone.
    ///
    /// `schema` narrows the walk to one schema's field molecules; `None` audits
    /// the whole store. A named schema that resolves to no molecules is an error
    /// rather than a silent whole-store scan — the difference between "that
    /// schema has nothing" and "I just read your entire database" is not one to
    /// discover from the runtime.
    ///
    /// The name may be either the stored schema name (a hash on a real home) or
    /// a `descriptive_name`. A descriptive name can match several registered
    /// schemas — successive versions of the same product schema — and all of
    /// their molecules are audited together, because a page over that data is
    /// served from all of them.
    ///
    /// `max_keys` / `after_key` bound one call and resume the next; see
    /// [`crate::db_operations::AtomStore::audit_key_tombstone_flags`].
    pub async fn audit_tombstone_flags(
        &self,
        schema: Option<&str>,
        stamp: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<TombstoneFlagBackfillReport, FoldDbError> {
        let molecules = match schema {
            None => None,
            Some(name) => {
                let mut matched: Vec<(String, Schema)> = Vec::new();
                if let Ok(Some(schema)) = self.schema_manager().get_schema_metadata(name) {
                    matched.push((name.to_string(), schema));
                } else {
                    let all = self
                        .db_ops()
                        .get_all_schemas()
                        .await
                        .map_err(|e| FoldDbError::Database(format!("list schemas: {e}")))?;
                    matched
                        .extend(all.into_iter().filter(|(_, schema)| {
                            schema.descriptive_name.as_deref() == Some(name)
                        }));
                }
                if matched.is_empty() {
                    // The operator named a schema; nothing resolves to it. That
                    // is a 404 about their argument, not a server fault.
                    return Err(FoldDbError::Schema(crate::schema::SchemaError::NotFound(
                        format!("unknown schema: {name}"),
                    )));
                }
                let mut map = std::collections::HashMap::new();
                for (stored_name, schema) in &matched {
                    // A stored name is a hash on a real home; the descriptive
                    // name is what the operator asked about, so label with it.
                    let label = schema.descriptive_name.as_deref().unwrap_or(stored_name);
                    for (field_name, field) in &schema.runtime_fields {
                        if let Some(uuid) = field.common().molecule_uuid() {
                            map.insert(uuid.clone(), format!("{label}.{field_name}"));
                        }
                    }
                }
                if map.is_empty() {
                    // Resolvable but not auditable: still a fact about the
                    // caller's selector, so 400 rather than 500.
                    return Err(FoldDbError::Schema(
                        crate::schema::SchemaError::InvalidField(format!(
                            "schema {name} has no field molecules to audit"
                        )),
                    ));
                }
                Some(map)
            }
        };
        self.db_ops()
            .atoms()
            .audit_key_tombstone_flags(molecules.as_ref(), stamp, max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("audit_key_tombstone_flags", e))
    }

    /// Drain one bounded page of legacy tombstone-content tips into the same
    /// reachability-guarded erasure core used by purge.
    ///
    /// The walk and resume cursor are storage-shaped because BlindV1 hashes
    /// cannot be reversed into caller keys. `dry_run` performs the identical
    /// discovery/ownership classification without deleting anything.
    pub async fn drain_legacy_tombstones(
        &self,
        schema: Option<&str>,
        dry_run: bool,
        max_keys: usize,
        after_key: Option<&str>,
    ) -> Result<LegacyTombstoneDrainReport, FoldDbError> {
        use crate::schema::types::field::FieldKind;

        let mut schemas: Vec<(String, Schema)> = self
            .db_ops()
            .get_all_schemas()
            .await
            .map_err(|e| FoldDbError::Database(format!("list schemas for tombstone drain: {e}")))?
            .into_iter()
            .filter(|(stored_name, value)| {
                schema.is_none_or(|wanted| {
                    stored_name == wanted || value.descriptive_name.as_deref() == Some(wanted)
                })
            })
            .collect();
        if schema.is_some() && schemas.is_empty() {
            return Err(FoldDbError::Schema(crate::schema::SchemaError::NotFound(
                format!("unknown schema: {}", schema.unwrap_or_default()),
            )));
        }
        schemas.sort_by(|a, b| a.0.cmp(&b.0));

        let mut owners: HashMap<String, Vec<(String, String, FieldKind)>> = HashMap::new();
        for (schema_name, value) in &schemas {
            for (field_name, field) in &value.runtime_fields {
                if let Some(molecule) = field.common().molecule_uuid() {
                    owners.entry(molecule.clone()).or_default().push((
                        schema_name.clone(),
                        field_name.clone(),
                        field.kind,
                    ));
                }
            }
        }
        for rows in owners.values_mut() {
            rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        }
        let molecules: HashSet<String> = owners.keys().cloned().collect();
        if molecules.is_empty() {
            return Err(FoldDbError::Schema(
                crate::schema::SchemaError::InvalidField(
                    "selected schemas have no field molecules to drain".into(),
                ),
            ));
        }

        let scan = self
            .db_ops()
            .atoms()
            .scan_legacy_tombstone_slots(&molecules, max_keys.max(1), after_key, None)
            .await
            .map_err(|e| admin_op_error("scan legacy tombstones", e))?;
        let mut report = LegacyTombstoneDrainReport {
            dry_run,
            keys_scanned: scan.keys_scanned,
            keys_unreadable: scan.keys_unreadable,
            atoms_fetched: scan.atoms_fetched,
            atoms_missing: scan.atoms_missing,
            tombstones_found: scan.slots.len() as u64,
            more_remaining: scan.more_remaining,
            next_after_key: scan.next_after_key,
            ..Default::default()
        };

        let mut by_schema: HashMap<
            String,
            Vec<crate::fold_db_core::purge::StorageSlotPurgeTarget>,
        > = HashMap::new();
        for slot in scan.slots {
            let compatible = |kind: FieldKind| match kind {
                FieldKind::Single => slot.storage_hash.is_empty() && slot.storage_range.is_empty(),
                FieldKind::Hash => !slot.storage_hash.is_empty() && slot.storage_range.is_empty(),
                FieldKind::Range => slot.storage_hash.is_empty() && !slot.storage_range.is_empty(),
                FieldKind::HashRange => {
                    !slot.storage_hash.is_empty() && !slot.storage_range.is_empty()
                }
            };
            let owner = owners
                .get(&slot.molecule_uuid)
                .and_then(|rows| rows.iter().find(|(_, _, kind)| compatible(*kind)));
            let Some((schema_name, field_name, _)) = owner else {
                report.unowned_tombstones += 1;
                continue;
            };
            by_schema.entry(schema_name.clone()).or_default().push(
                crate::fold_db_core::purge::StorageSlotPurgeTarget::new(
                    field_name,
                    &slot.molecule_uuid,
                    slot.storage_hash,
                    slot.storage_range,
                ),
            );
        }
        if dry_run {
            return Ok(report);
        }

        let mut schema_names: Vec<String> = by_schema.keys().cloned().collect();
        schema_names.sort();
        for schema_name in schema_names {
            let targets = by_schema
                .remove(&schema_name)
                .expect("schema_names came from by_schema");
            let evidence = self
                .mutation_manager()
                .purge_storage_slots_guarded(&schema_name, &targets)
                .await
                .map_err(|e| {
                    FoldDbError::Database(format!(
                        "drain legacy tombstones for schema {schema_name}: {e}"
                    ))
                })?;
            let drained = evidence.len() as u64;
            report.tombstones_drained += drained;
            report.search_tombstones_queued += drained;
            report.per_schema_drained.insert(schema_name, drained);
        }
        Ok(report)
    }

    /// Rewrite fat `mk:` tip values to thin `{atom_uuid, written_at, device_id}` in place.
    ///
    /// Bounded and resumable — see
    /// [`crate::db_operations::AtomStore::migrate_thin_tips`]. Callers follow
    /// `next_after_key` while `more_remaining` is true.
    pub async fn migrate_thin_tips(
        &self,
        dry_run: bool,
        max_keys: Option<usize>,
        after_key: Option<&str>,
    ) -> Result<ThinTipMigrateReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .migrate_thin_tips(dry_run, max_keys, after_key, None)
            .await
            .map_err(|e| admin_op_error("migrate_thin_tips", e))
    }

    /// Resumable dual-readable rekey of atom bodies onto partition-prefixed
    /// keys. See [`crate::db_operations::AtomStore::rekey_atoms_to_partition_prefix`].
    /// Live primary flip of `LASTDB_ATOM_KEY_ENCODING` remains Tom-gated.
    pub async fn rekey_atoms_to_partition_prefix(
        &self,
        options: AtomPartitionRekeyOptions,
    ) -> Result<AtomPartitionRekeyReport, FoldDbError> {
        self.db_ops()
            .atoms()
            .rekey_atoms_to_partition_prefix(options)
            .await
            .map_err(|e| FoldDbError::Database(format!("rekey_atoms_to_partition_prefix: {e}")))
    }
}

fn field_molecule_uuids(schema: &Schema) -> Vec<String> {
    let mut mols = Vec::new();
    for field in schema.runtime_fields.values() {
        if let Some(u) = field.common().molecule_uuid() {
            mols.push(u.clone());
        }
    }
    mols.sort();
    mols.dedup();
    mols
}
