//! Erase meter state joins the same native snapshot as the removed data.

use super::counter::TipDebit;
use super::*;
use crate::db_operations::{AtomDeleteLedgerEntry, KeepSmallSnapshot, MoleculeTipCounterSource};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction::{self, publication};
use chrono::Utc;

struct EraseMeterPlan {
    atoms: Vec<(Option<String>, String, u64)>,
    tips: Vec<(String, String, u64, Option<MoleculeTipCounterSource>)>,
}

struct EraseAggregate {
    snapshot: KeepSmallSnapshot,
    dirty: HashSet<String>,
}

impl EraseMeterPlan {
    fn record(&self, meters: &KeepSmallMeters) {
        for (schema, atom, bytes) in &self.atoms {
            meters.forget_pending_atom(atom);
            if *bytes > 0 {
                meters.record_atom_delete(schema.as_deref(), *bytes);
            }
        }
        for (key, schema, bytes, source) in &self.tips {
            if *bytes > 0 {
                meters.record_tip_delete(Some(schema.as_str()), *bytes);
                if let Some(molecule) =
                    crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key)
                {
                    meters.record_molecule_tip_delete(molecule, source.as_ref(), *bytes);
                }
            }
            meters.forget_key(key);
        }
    }
}

impl AtomStore {
    /// Stage final audit facts with the native erase. Failure aborts the scope.
    pub(crate) async fn record_atom_delete_audit(
        &self,
        storage_prefix: Option<&str>,
        mut entry: AtomDeleteLedgerEntry,
    ) -> Result<(), SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime());
        if !crate::storage::disk_pack::transaction::publication::active(runtime) {
            return Err(SchemaError::InvalidData(
                "erase audit requires a complete native write scope".into(),
            ));
        }
        let ts = Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0);
        let base = crate::kind_partition::anchored(
            "dellog",
            &format!("{ts:020}:{}", uuid::Uuid::new_v4()),
        );
        let key = build_storage_key(storage_prefix, &base);
        entry.committed = true;
        self.raw()
            .put_item(&key, &entry)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("stage erase audit: {error}")))
    }

    /// Stage final aggregate state, never a pending operation or debit record.
    /// A cancelled or refused request drops both this candidate and its deletes.
    /// Multiple chunks read the preceding private candidate in the same scope.
    pub(crate) async fn stage_erase_meters(
        &self,
        fallback_schema: &str,
        atoms: &[(Option<String>, String, u64)],
        tips: &[TipDebit],
    ) -> Result<(), SchemaError> {
        let runtime = self
            .namespaced_store
            .as_ref()
            .and_then(|store| store.disk_pack_runtime())
            .ok_or_else(|| {
                SchemaError::InvalidData("erase meters require native storage".into())
            })?;
        if !publication::active(Some(Arc::clone(&runtime))) || self.keep_small_persist.is_none() {
            return Err(SchemaError::InvalidData(
                "erase meters require a complete native write scope and meter store".into(),
            ));
        }
        transaction::require_durable(&runtime)?;
        let _persist = self.keep_small_persist_lock.lock().await;
        self.keep_small
            .check_erase_copy_budget()
            .map_err(|detail| SchemaError::WriteResourceLimit { detail })?;
        let owner = Arc::as_ptr(&self.keep_small) as usize;
        self.reserve_erase_effect_bytes(&runtime, owner, fallback_schema, atoms, tips)?;
        let previous =
            publication::get::<EraseAggregate>(Some(Arc::clone(&runtime)), owner, "erase-meters")?;
        let candidate = KeepSmallMeters::default();
        candidate
            .import(
                previous
                    .as_ref()
                    .map_or_else(|| self.keep_small.export(), |value| value.snapshot.clone()),
            )
            .map_err(SchemaError::InvalidData)?;
        let plan = self.resolve_erase_meter_plan(fallback_schema, atoms, tips);
        plan.record(&candidate);
        let snapshot = candidate.export();
        let mut dirty = self.keep_small.dirty_schema_names();
        dirty.extend(candidate.take_dirty_schemas());
        if let Some(previous) = &previous {
            dirty.extend(previous.dirty.iter().cloned());
        }
        let mut durable = snapshot.clone();
        durable.clean_stop = Some(false);
        if durable.trust.global.state.is_complete() {
            durable.trust.global = MeterDomainTrust::incomplete("dirty_snapshot");
            durable.trust.molecules = MeterDomainTrust::incomplete("dirty_snapshot");
            for domain in durable.trust.schemas.values_mut() {
                *domain = MeterDomainTrust::incomplete("dirty_snapshot");
            }
        }
        self.write_keep_small_header_and_shards(&durable, Some(&dirty))
            .await?;
        publication::publish(
            Some(Arc::clone(&runtime)),
            owner,
            "erase-meters".into(),
            EraseAggregate { snapshot, dirty },
            || Ok(()),
        )?;
        let store = self.clone();
        publication::publish(
            Some(runtime),
            owner,
            format!("erase-meter-effect:{}", uuid::Uuid::new_v4()),
            (),
            move || {
                plan.record(&store.keep_small);
                store.keep_small_dirty.store(true, Ordering::Relaxed);
                store
                    .keep_small_clean_stop_written
                    .store(false, Ordering::Relaxed);
                Ok(())
            },
        )?;
        Ok(())
    }

    /// One scope retains at most 8 MiB of post-root debit effects. This
    /// reservation lives in the same private publication and drops on refusal.
    fn reserve_erase_effect_bytes(
        &self,
        runtime: &Arc<crate::storage::disk_pack::PackRuntime>,
        owner: usize,
        fallback_schema: &str,
        atoms: &[(Option<String>, String, u64)],
        tips: &[TipDebit],
    ) -> Result<(), SchemaError> {
        let mut bytes =
            publication::get::<usize>(Some(Arc::clone(runtime)), owner, "erase-effect-bytes")?
                .map_or(0, |value| *value);
        for (schema, atom, _) in atoms {
            bytes = bytes.saturating_add(256).saturating_add(
                schema
                    .as_ref()
                    .map_or(0, String::len)
                    .saturating_add(atom.len())
                    .saturating_mul(2),
            );
        }
        let schema_bytes = fallback_schema.len().max(
            self.keep_small
                .largest_schema_name_bytes()
                .map_err(|detail| SchemaError::WriteResourceLimit { detail })?,
        );
        for (key, _, source) in tips {
            bytes = bytes
                .saturating_add(512)
                .saturating_add(key.len().saturating_mul(2))
                .saturating_add(schema_bytes.saturating_mul(2))
                .saturating_add(
                    source
                        .as_ref()
                        .map_or(0, |value| value.atom_uuid.len().saturating_mul(2)),
                );
        }
        if bytes > 8 * 1024 * 1024 {
            return Err(SchemaError::WriteResourceLimit {
                detail: "erase post-root meter effects exceed the 8 MiB per-request cap".into(),
            });
        }
        publication::publish(
            Some(Arc::clone(runtime)),
            owner,
            "erase-effect-bytes".into(),
            bytes,
            || Ok(()),
        )?;
        Ok(())
    }

    fn resolve_erase_meter_plan(
        &self,
        fallback_schema: &str,
        atoms: &[(Option<String>, String, u64)],
        tips: &[TipDebit],
    ) -> EraseMeterPlan {
        let tips = tips
            .iter()
            .map(|(key, bytes, source)| {
                let schema = crate::atom::molecule_key_codec::molecule_uuid_from_storage_key(key)
                    .and_then(|molecule| self.keep_small.molecule_schema(molecule))
                    .unwrap_or_else(|| fallback_schema.to_string());
                (key.clone(), schema, *bytes, source.clone())
            })
            .collect();
        EraseMeterPlan {
            atoms: atoms.to_vec(),
            tips,
        }
    }
}
