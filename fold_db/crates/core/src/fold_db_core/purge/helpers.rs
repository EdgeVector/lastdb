//! Pure helpers for purge planning and key matching.

use std::collections::HashSet;
use std::sync::Arc;

use crate::atom::{FieldKey, MutationEvent};
use crate::db_operations::DbOperations;
use crate::schema::types::field::FieldVariant;
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

pub(crate) use super::chain_walk::*;
pub(crate) use super::target_chain::*;

/// Translate an API-form (plaintext) [`KeyValue`] into the **storage form** the
/// loaded molecule is actually keyed by.
///
/// Purge hydrates its molecules through the full-load path
/// (`refresh_runtime_field_molecules` → `refresh_from_db` →
/// `load_all_mk_records`), which reassembles each slot by decoding the
/// `mk:{M}:{esc(hash)}\0{range}` key it was stored under. That key segment is
/// storage form: HMAC-blinded under `HashKeyEncoding::BlindV1` and OPE-encoded
/// under `RangeKeyEncoding::OpeV1`, which are the product defaults for every
/// node booted with a recovery phrase. The `keys` a purge is given arrive in
/// API form, because that is what the query path hands back.
///
/// So every molecule lookup in this module has to cross that boundary, and
/// none of them did — `hash='lastdbd-self'` was compared against a stored
/// `hash='3CrWWLdb5nvISTA1_2Vf2Q'`, nothing ever matched, and the compliance
/// verb refused every batch on a real node. Unencrypted homes are the only
/// place the two forms coincide, and that is the only configuration the rest
/// of the purge suite builds.
///
/// Both encodings are **molecule-uuid-domain-separated**, so this is per
/// field, not per schema: one API hash maps to a different storage segment in
/// every field of the same record. Pinned next door by
/// `one_api_hash_blinds_differently_per_field`.
///
/// A field with no molecule uuid has never been written and has no slot to
/// address; its key passes through unchanged rather than erroring, matching how
/// every other helper here treats that field (skip, don't fail).
pub(crate) fn storage_form_key(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    kv: &KeyValue,
) -> Result<KeyValue, SchemaError> {
    let Some(mol_uuid) = field.common().molecule_uuid() else {
        return Ok(kv.clone());
    };
    let hash = match kv.hash.as_deref() {
        Some(h) => Some(db_ops.atoms().storage_hash(mol_uuid, h)?),
        None => None,
    };
    let range = match kv.range.as_deref() {
        Some(r) => Some(db_ops.atoms().storage_range(mol_uuid, r)?),
        None => None,
    };
    Ok(KeyValue::new(hash, range))
}

/// [`storage_form_key`] for a whole batch, positionally aligned with `keys` so
/// a caller can keep using the batch index it already has for the API-form key
/// (error text, the delete-ledger fingerprint) while addressing storage with
/// the translated one.
pub(crate) fn storage_form_keys(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    keys: &[KeyValue],
) -> Result<Vec<KeyValue>, SchemaError> {
    keys.iter()
        .map(|k| storage_form_key(db_ops, field, k))
        .collect()
}

/// Same shape-check the mutation pipeline applies to Delete in
/// `mutation_manager::prepare_atoms_and_key_values`. Failing here means
/// the caller submitted a `Hash` schema purge with no hash, etc.
pub(crate) fn validate_purge_key_shape(
    schema: &Schema,
    schema_name: &str,
    key: &KeyValue,
) -> Result<(), SchemaError> {
    match &schema.schema_type {
        DeclarativeSchemaType::Hash if key.hash.is_none() => Err(SchemaError::InvalidData(
            format!("Hash schema '{schema_name}' purge has no hash key"),
        )),
        DeclarativeSchemaType::Range if key.range.is_none() => Err(SchemaError::InvalidData(
            format!("Range schema '{schema_name}' purge has no range key"),
        )),
        DeclarativeSchemaType::HashRange if key.hash.is_none() || key.range.is_none() => {
            Err(SchemaError::InvalidData(format!(
                "HashRange schema '{}' purge requires both hash and range keys, got hash={:?} range={:?}",
                schema_name, key.hash, key.range,
            )))
        }
        _ => Ok(()),
    }
}

/// Pull every field's molecule into memory if it isn't there yet. Reuses
/// the same `refresh_from_db` plumbing that the normal mutation path
/// runs in `restore_missing_molecules`.
///
/// Field loads are independent (distinct molecule UUIDs / storage prefixes).
/// Issue them concurrently: under the exclusive purge barrier a 24-field
/// schema used to pay 24 sequential full-molecule cold loads before any
/// slot was removed. The end state is unchanged — every empty field with a
/// molecule UUID is hydrated, or left empty when the store has no rows.
pub(crate) async fn refresh_runtime_field_molecules(
    db_ops: &Arc<DbOperations>,
    schema: &mut Schema,
) -> Result<(), SchemaError> {
    let plans: Vec<(String, String, Option<String>)> = schema
        .runtime_fields
        .iter()
        .filter(|(_, f)| f.common().molecule_uuid().is_some() && !f.has_molecule())
        .filter_map(|(n, f)| {
            let uuid = f.common().molecule_uuid()?.clone();
            let prefix = f.common().storage_prefix().map(str::to_string);
            Some((n.clone(), uuid, prefix))
        })
        .collect();
    if plans.is_empty() {
        return Ok(());
    }

    let loads = futures::future::try_join_all(plans.iter().map(|(name, uuid, prefix)| {
        let db_ops = Arc::clone(db_ops);
        let name = name.clone();
        let uuid = uuid.clone();
        let prefix = prefix.clone();
        async move {
            let data = db_ops
                .atoms()
                .load_molecule_per_key(&uuid, prefix.as_deref())
                .await?;
            Ok::<_, SchemaError>((name, data))
        }
    }))
    .await?;

    for (name, data) in loads {
        let Some(data) = data else {
            continue;
        };
        if let Some(field) = schema.runtime_fields.get_mut(&name) {
            // Same normalization/kind checks as `refresh_from_db`.
            let data = data.retyped_to_slot(field.kind.retype_slot());
            field.set_molecule_data(data)?;
        }
    }
    Ok(())
}

/// Load only the exact durable slots addressed by one purge batch.
///
/// The reverse-edge path does not need the complement of the target set. It
/// must therefore not materialize every `mk:` row in each field molecule.
pub(crate) async fn refresh_runtime_field_molecules_for_purge_keys(
    db_ops: &Arc<DbOperations>,
    schema: &mut Schema,
    keys: &[KeyValue],
) -> Result<(), SchemaError> {
    let mut plans = Vec::new();
    for (field_name, field) in &schema.runtime_fields {
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        let storage_keys = storage_form_keys(db_ops, field, keys)?;
        let slots: Vec<_> = storage_keys
            .iter()
            .filter_map(|key| field.disk_slot_for_key(key))
            .collect();
        plans.push((
            field_name.clone(),
            molecule_uuid,
            field.common().storage_prefix().map(str::to_string),
            slots,
        ));
    }

    let loads = futures::future::try_join_all(plans.iter().map(
        |(field_name, molecule_uuid, storage_prefix, slots)| {
            let db_ops = Arc::clone(db_ops);
            let field_name = field_name.clone();
            let molecule_uuid = molecule_uuid.clone();
            let storage_prefix = storage_prefix.clone();
            let slots = slots.clone();
            async move {
                let data = db_ops
                    .atoms()
                    .load_molecule_for_storage_slot_purge(
                        &molecule_uuid,
                        storage_prefix.as_deref(),
                        &slots,
                    )
                    .await?;
                Ok::<_, SchemaError>((field_name, data))
            }
        },
    ))
    .await?;

    for (field_name, data) in loads {
        let Some(field) = schema.runtime_fields.get_mut(&field_name) else {
            continue;
        };
        field.clear_molecule();
        if let Some(data) = data {
            field.set_molecule_data(data.retyped_to_slot(field.kind.retype_slot()))?;
        }
    }
    Ok(())
}

/// True when this schema can use the exact reverse-edge purge path.
///
/// Every molecule that can reference a same-schema content-addressed atom must
/// have a replay-complete manifest that includes mutation-history edges.
/// A v1 tip-only manifest advances one bounded, molecule-keyed history page.
/// Incomplete work keeps the guarded path for this purge and resumes later.
pub(crate) async fn atom_ref_cutover_ready(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
) -> Result<bool, SchemaError> {
    if !barrierless_purge_enabled_from(std::env::var("LASTDB_BARRIERLESS_PURGE").ok().as_deref()) {
        return Ok(false);
    }

    let mut saw_molecule = false;
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        saw_molecule = true;
        let prefix = field.common().storage_prefix();
        let Some(manifest) = db_ops
            .atoms()
            .atom_ref_molecule_manifest(molecule_uuid, prefix)
            .await?
        else {
            return Ok(false);
        };
        match (manifest.version, manifest.replay_complete) {
            (crate::db_operations::atom_store::ATOM_REF_MANIFEST_VERSION_HISTORY, true) => {}
            (crate::db_operations::atom_store::ATOM_REF_MANIFEST_VERSION_TIPS, true) => {
                let upgrade = db_ops
                    .atoms()
                    .upgrade_atom_ref_molecule_history_page(molecule_uuid, prefix, 256)
                    .await?;
                if !upgrade.complete {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(saw_molecule)
}

fn barrierless_purge_enabled_from(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("0" | "false" | "off" | "no")
    )
}

/// Insert every atom UUID a history event names (the new head and, when
/// present, the prior head it superseded) into `sink`. Centralised so the
/// purge target's candidate-atom gather and the sibling-key history-row
/// preservation guard agree on what counts as a referenced atom — drift
/// between the two would silently re-open the same content-addressed
/// purge bug they collectively close.
pub(crate) fn collect_event_atom_uuids(event: &MutationEvent, sink: &mut HashSet<String>) {
    sink.insert(event.new_atom_uuid.clone());
    if let Some(old) = &event.old_atom_uuid {
        sink.insert(old.clone());
    }
}

/// True when a stored history event refers to the same per-key slot the
/// caller is purging. `FieldKey::single()` always matches because a Single
/// field has exactly one slot per schema.
pub(crate) fn field_key_matches_value(field_key: &FieldKey, kv: &KeyValue) -> bool {
    // Single always matches (one slot per schema). Keyed slots compare
    // against the KeyValue shape of the purge target.
    field_key.is_single() || field_key.matches_key_value(kv)
}

/// The atom UUID currently at the molecule head for the field's slot
/// identified by `kv`. `None` when no entry exists or the variant
/// doesn't carry a key shape compatible with `kv`.
pub(crate) fn current_atom_for_key(field: &FieldVariant, kv: &KeyValue) -> Option<String> {
    field.current_atom_uuid(kv)
}

/// Remove the entry for `kv` from the field's molecule. Returns `true`
/// when an entry was actually dropped (so the caller knows to persist
/// the molecule). For `Single` variants, the entire molecule is taken
/// — a Single schema has exactly one "record" per schema, so purging
/// it means nuking the field's value entirely.
pub(crate) fn remove_key_from_field(field: &mut FieldVariant, kv: &KeyValue) -> bool {
    field.remove_key(kv)
}

/// Walk every still-loaded molecule across every field of the schema
/// and collect the atom UUIDs they reference. Used to decide which of
/// the purge target's atoms are safe to hard-delete: an atom still in
/// this set is referenced by another key in the same schema (atoms are
/// content-addressed by `(schema, content)`, so this can happen for
/// byte-identical writes) and must NOT be deleted, or the still-live
/// record would dangle.
pub(crate) fn collect_live_atom_uuids(schema: &Schema) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    for field in schema.runtime_fields.values() {
        field.collect_atom_uuids(&mut out);
    }
    out
}

/// True when AtomRefEdges must keep `uuid` because T0 already acked it.
/// Disk reverse edges do not see that write until its persist envelope runs.
pub(crate) fn atom_ref_edges_keep_resident_live(
    uuid: &str,
    resident_live: &HashSet<String>,
) -> bool {
    resident_live.contains(uuid)
}

/// Load an atom for hard-erase accounting. A storage error must fail the
/// verb: skipping it still leaves `uuid` in `atoms_to_delete`.
pub(crate) fn atom_load_for_hard_erase(
    loaded: Result<Option<crate::atom::Atom>, SchemaError>,
    uuid: &str,
) -> Result<Option<crate::atom::Atom>, SchemaError> {
    match loaded {
        Ok(atom) => Ok(atom),
        Err(error) => Err(SchemaError::InvalidData(format!(
            "Failed to load atom {uuid} before hard-delete: {error}"
        ))),
    }
}

/// Resident tips for this schema's molecules. A concurrent write can ack
/// a new live head before its persist envelope runs; disk molecules then
/// lag T0. Union this set into the preserve list so a purge commit that
/// walked before that write does not delete the new atom.
pub(crate) fn collect_resident_live_atom_uuids(
    db_ops: &DbOperations,
    schema: &Schema,
) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    let graph = db_ops.resident();
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        for tip in graph.tips_for_molecule(molecule_uuid) {
            if !tip.atom_uuid.is_empty() {
                out.insert(tip.atom_uuid);
            }
        }
    }
    out
}

/// Human-readable rendering of a KeyValue for log/error messages.
pub(crate) fn describe_key(kv: &KeyValue) -> String {
    match (&kv.hash, &kv.range) {
        (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
        (Some(h), None) => format!("hash='{h}'"),
        (None, Some(r)) => format!("range='{r}'"),
        (None, None) => "<single>".to_string(),
    }
}

// `FieldPurgePlan` lived here: a per-field materialised plan that only made
// sense while planning was scoped to one record. The batched path accumulates
// candidates, `history:` keys and `tv:` keys across the whole batch instead, so
// the per-field struct had no remaining reader. Removed rather than left
// `#[allow(dead_code)]` — an unused plan type in the store's most destructive
// module reads as a surface someone still maintains.
