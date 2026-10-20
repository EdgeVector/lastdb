//! The tips that a delete or a purge names: metadata, then one batch read.
//!
//! The cost follows the number of keys of the request, not the size of the
//! schema. Nothing here reads a whole schema, a history row or a range.

use super::delete_candidates::{delete_candidates_for_key, DeleteCandidate};
use super::*;
use crate::db_operations::atom_store::PerKeyRecord;
use crate::schema::types::Schema;

/// One stored tip that a request names, with the index of its key.
pub(super) struct Target {
    pub(super) key_index: usize,
    pub(super) candidate: DeleteCandidate,
    pub(super) record: PerKeyRecord,
}

/// The schema, the distinct keys in request order, and the stored tips.
pub(super) struct LoadedTargets {
    pub(super) schema: Schema,
    pub(super) keys: Vec<KeyValue>,
    pub(super) targets: Vec<Target>,
}

impl LoadedTargets {
    /// Indexes of `keys` that have at least one stored tip.
    pub(super) fn present(&self) -> HashSet<usize> {
        self.targets.iter().map(|target| target.key_index).collect()
    }
}

/// The schema, the distinct keys, and the physical keys to read. No store read
/// has happened yet, so a caller can take its locks first.
pub(super) struct TargetPlan {
    pub(super) schema: Schema,
    pub(super) keys: Vec<KeyValue>,
    plans: Vec<(usize, DeleteCandidate)>,
}

impl TargetPlan {
    /// Read the tip of every planned physical key in one batch.
    pub(super) async fn read(
        self,
        db_ops: &Arc<DbOperations>,
        storage_prefix: Option<&str>,
    ) -> Result<LoadedTargets, SchemaError> {
        let targets = read_targets(db_ops, self.plans, storage_prefix).await?;
        Ok(LoadedTargets {
            schema: self.schema,
            keys: self.keys,
            targets,
        })
    }
}

/// Load the schema metadata, validate and dedupe the keys, and name the
/// physical keys of every field and spelling of every key.
pub(super) fn plan_targets(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<TargetPlan, SchemaError> {
    let mut schema = schema_manager
        .get_schema_metadata(schema_name)?
        .ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found for erase"))
        })?;
    // Definitions are global; every data load uses the selected database.
    crate::fold_db_core::mutation_manager::helpers::clear_storage_prefix_on_schema(&mut schema);
    crate::fold_db_core::mutation_manager::helpers::apply_storage_prefix_to_schema(
        &mut schema,
        storage_prefix,
    );
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(keys.len());
    for key in keys {
        validate_purge_key_shape(&schema, schema_name, key)?;
        if seen.insert(describe_key(key)) {
            deduped.push(key.clone());
        }
    }
    let plans = candidate_plans(db_ops, &schema, &deduped)?;
    Ok(TargetPlan {
        schema,
        keys: deduped,
        plans,
    })
}

/// Load the schema, validate and dedupe the keys, and read the tip of every
/// field and spelling of every key in one batch.
pub(super) async fn load_targets(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<LoadedTargets, SchemaError> {
    plan_targets(db_ops, schema_manager, schema_name, keys, storage_prefix)?
        .read(db_ops, storage_prefix)
        .await
}

/// Every physical key that a normal reader accepts for each field and key.
fn candidate_plans(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    keys: &[KeyValue],
) -> Result<Vec<(usize, DeleteCandidate)>, SchemaError> {
    let mut plans = Vec::new();
    let mut seen = HashSet::new();
    for field in schema.runtime_fields.values() {
        if field.common().molecule_uuid().is_none() {
            continue;
        }
        for (key_index, key) in keys.iter().enumerate() {
            for candidate in delete_candidates_for_key(db_ops, field, key)? {
                if seen.insert(candidate.mk_key.clone()) {
                    plans.push((key_index, candidate));
                }
            }
        }
    }
    Ok(plans)
}

async fn read_targets(
    db_ops: &Arc<DbOperations>,
    plans: Vec<(usize, DeleteCandidate)>,
    storage_prefix: Option<&str>,
) -> Result<Vec<Target>, SchemaError> {
    if plans
        .iter()
        .any(|(_, candidate)| candidate.storage_prefix.as_deref() != storage_prefix)
    {
        return Err(SchemaError::InvalidData(
            "Delete candidates have different storage scopes".into(),
        ));
    }
    let storage_keys: Vec<String> = plans
        .iter()
        .map(|(_, candidate)| candidate.mk_key.clone())
        .collect();
    let records = db_ops
        .atoms()
        .delete_target_tips_with_generations(&storage_keys, storage_prefix)
        .await?;
    Ok(plans
        .into_iter()
        .zip(records)
        .filter_map(|((key_index, candidate), record)| {
            record.map(|record| Target {
                key_index,
                candidate,
                record,
            })
        })
        .collect())
}

/// The keys of `keys` that no field of `schema_name` has a tip for, in the
/// order given and without repeats. A TTL purge uses it to drop the keys that
/// a client or a peer already removed, and then retries the rest of its batch.
pub(in crate::fold_db_core) async fn absent_purge_targets(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
    storage_prefix: Option<&str>,
) -> Result<Vec<KeyValue>, SchemaError> {
    let loaded = load_targets(db_ops, schema_manager, schema_name, keys, storage_prefix).await?;
    let present = loaded.present();
    Ok(loaded
        .keys
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !present.contains(index))
        .map(|(_, key)| key)
        .collect())
}

/// Check every target in one loud erase envelope before any target is
/// changed. The lane records a successful check and replays all later durable
/// attempts with `Skip`.
pub(in crate::fold_db_core) async fn validate_purge_targets_present(
    db_ops: &Arc<DbOperations>,
    schema_manager: &Arc<SchemaCore>,
    schema_name: &str,
    keys: &[KeyValue],
    verb: HardEraseVerb,
    storage_prefix: Option<&str>,
) -> Result<PurgeTargetPresence, SchemaError> {
    if keys.is_empty() {
        return Ok(PurgeTargetPresence::Present);
    }
    let loaded = load_targets(db_ops, schema_manager, schema_name, keys, storage_prefix).await?;
    let present = loaded.present();
    if present.len() == loaded.keys.len() {
        return Ok(PurgeTargetPresence::Present);
    }
    Ok(PurgeTargetPresence::Missing(missing_targets_error(
        schema_name,
        &loaded.keys,
        &present,
        verb,
    )))
}

fn missing_targets_error(
    schema_name: &str,
    keys: &[KeyValue],
    present: &HashSet<usize>,
    verb: HardEraseVerb,
) -> SchemaError {
    // A batch of one keeps the exact wording that the compliance verb has
    // always returned. That string is the operator-facing contract.
    if keys.len() == 1 {
        return SchemaError::InvalidData(format!(
            "{} target not found: schema '{}', key {} — refusing to silently no-op {}",
            verb.miss_noun(),
            schema_name,
            describe_key(&keys[0]),
            verb.miss_suffix(),
        ));
    }
    let absent: Vec<String> = keys
        .iter()
        .enumerate()
        .filter(|(idx, _)| !present.contains(idx))
        .map(|(_, key)| describe_key(key))
        .take(10)
        .collect();
    SchemaError::InvalidData(format!(
        "{} targets not found: schema '{}', {} of {} keys absent (first: {}) — refusing to \
         silently no-op {}",
        verb.miss_noun(),
        schema_name,
        keys.len() - present.len(),
        keys.len(),
        absent.join(", "),
        verb.miss_suffix(),
    ))
}
