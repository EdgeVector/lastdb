//! Mutation atom preparation and changed-key analysis.

use std::collections::{HashMap, HashSet};

use crate::atom::Atom;
use crate::db_operations::{AtomStore, ChangedKey};
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{FieldValueType, KeyValue, Mutation, Schema};
use crate::schema::SchemaError;
use sha2::{Digest, Sha256};
use tracing::warn;

use super::super::helpers::normalize_key_value_for_schema_type;
use super::super::MutationManager;

type PreparedAtomsAndKeys = (
    Vec<KeyValue>,
    Vec<(usize, String, Atom)>,
    Option<Vec<(Atom, Option<crate::atom::AtomPartition>)>>,
);

impl MutationManager {
    /// Reject a conflicting explicit HashRange key field before idempotency or
    /// any schema group can publish. Preparation repeats the same check while
    /// it adds omitted fields to its private mutation copy.
    pub(in crate::fold_db_core::mutation_manager) fn validate_hashrange_key_field_payloads(
        &self,
        mutations: &[Mutation],
    ) -> Result<(), SchemaError> {
        let mut schemas = HashMap::new();
        for mutation in mutations {
            if !schemas.contains_key(&mutation.schema_name) {
                let mut schema = self
                    .schema_manager
                    .get_schema_metadata(&mutation.schema_name)?
                    .ok_or_else(|| {
                        SchemaError::InvalidData(format!(
                            "Schema '{}' not found",
                            mutation.schema_name
                        ))
                    })?;
                if schema.runtime_fields.is_empty() {
                    schema.populate_runtime_fields()?;
                }
                schemas.insert(mutation.schema_name.clone(), schema);
            }
            let schema = &schemas[&mutation.schema_name];
            if !matches!(schema.schema_type, DeclarativeSchemaType::HashRange) {
                continue;
            }
            let key_value =
                Self::resolve_mutation_key_value(&mutation.schema_name, schema, mutation)?;
            Self::validate_hashrange_key_fields(
                &mutation.schema_name,
                schema,
                mutation,
                &key_value,
            )?;
        }
        Ok(())
    }

    pub(in crate::fold_db_core::mutation_manager) fn resolve_mutation_key_value(
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
    ) -> Result<KeyValue, SchemaError> {
        // Prefer the pre-computed key_value from the mutation (set by the
        // ingestion service with date normalization and proper field extraction).
        // Fall back to KeyValue::from_mutation() only when both fields are None.
        let mut key_value =
            if mutation.key_value.hash.is_some() || mutation.key_value.range.is_some() {
                mutation.key_value.clone()
            } else {
                let key_config = schema.key.clone();
                KeyValue::from_mutation(
                    &mutation.fields_and_values,
                    key_config.as_ref().ok_or_else(|| {
                        SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' has no key configuration. Cannot execute mutation."
                    ))
                    })?,
                )
            };

        // Safety net: if key is still empty after both extraction paths,
        // generate a deterministic content hash so the mutation is stored
        // and retrievable rather than silently lost.
        if key_value.hash.is_none() && key_value.range.is_none() {
            let mut hasher = Sha256::new();
            let mut sorted: Vec<_> = mutation.fields_and_values.iter().collect();
            sorted.sort_by_key(|(a, _)| (*a).clone());
            for (k, v) in sorted {
                hasher.update(k.as_bytes());
                hasher.update(v.to_string().as_bytes());
            }
            let fallback = format!("{:x}", hasher.finalize());
            let short = &fallback[..16];
            warn!(
                "Key resolution produced empty key for schema '{}', mutation {}; using content hash '{}'",
                schema_name, mutation.uuid, short
            );
            key_value.hash = Some(short.to_string());
        }

        normalize_key_value_for_schema_type(
            schema_name,
            &schema.schema_type,
            &mutation.uuid,
            &mut key_value,
        );

        // Validate that the key matches the schema type.
        // A mismatch means a bug upstream — fail loudly.
        match &schema.schema_type {
            DeclarativeSchemaType::Hash if key_value.hash.is_none() => Err(
                SchemaError::InvalidData(format!(
                    "Hash schema '{}' mutation {} has no hash key",
                    schema_name, mutation.uuid
                )),
            ),
            DeclarativeSchemaType::Range if key_value.range.is_none() => Err(
                SchemaError::InvalidData(format!(
                    "Range schema '{}' mutation {} has no range key",
                    schema_name, mutation.uuid
                )),
            ),
            DeclarativeSchemaType::HashRange
                if key_value.hash.is_none() || key_value.range.is_none() =>
            {
                Err(SchemaError::InvalidData(format!(
                    "HashRange schema '{}' mutation {} requires both hash and range keys, got hash={:?} range={:?}",
                    schema_name, mutation.uuid, key_value.hash, key_value.range
                )))
            }
            _ => Ok(key_value),
        }
    }

    /// Creates atoms and computes key values for all mutations in a schema group.
    /// Returns key values, atom results, and located atom bodies.
    ///
    /// This phase performs no atom write and no resident apply. The caller
    /// applies personal resident atoms under the slot gates and gives every
    /// canonical atom body to the reserved schema-lane envelope.
    pub(in crate::fold_db_core::mutation_manager) fn prepare_atoms_and_key_values(
        &self,
        schema_name: &str,
        schema: &Schema,
        schema_mutations: &mut [Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<PreparedAtomsAndKeys, SchemaError> {
        let mut atoms_to_store: Vec<Atom> = Vec::new();
        let mut atom_results: Vec<(usize, String, Atom)> = Vec::new();
        let mut mutation_key_values = Vec::with_capacity(schema_mutations.len());

        for (idx, mutation) in schema_mutations.iter_mut().enumerate() {
            let key_value = Self::resolve_mutation_key_value(schema_name, schema, mutation)?;

            Self::materialize_hashrange_key_fields(schema_name, schema, mutation, &key_value)?;

            mutation_key_values.push(key_value);

            // Create atoms in memory (no storage yet). `MutationType::Delete`
            // is peeled out of this pipeline in `write_mutations_batch_async`
            // and hard-erased via the purge path (not N tombstone atoms).
            // Defensive: if a Delete ever reaches prepare, do not synthesize
            // per-field tombstones — write only the fields the caller named
            // (usually empty → zero atoms).
            let fields_to_write: &HashMap<String, serde_json::Value> = &mutation.fields_and_values;
            let document_write =
                fields_to_write.contains_key(crate::schema::types::RECORD_SENTINEL);

            for (field_name, value) in fields_to_write {
                // Envelope writes keep the declared HashRange key fields as
                // normal atoms. They remain the row spine after compaction.
                // Other keys stay only for the document and protein fold.
                let declared_hashrange_key =
                    matches!(schema.schema_type, DeclarativeSchemaType::HashRange)
                        && schema.key.as_ref().is_some_and(|key| {
                            key.hash_field.as_ref() == Some(field_name)
                                || key.range_field.as_ref() == Some(field_name)
                        });
                if document_write
                    && field_name != crate::schema::types::RECORD_SENTINEL
                    && !declared_hashrange_key
                {
                    continue;
                }
                // Extra keys (e.g. sibling partition values for field_hash
                // protein fold) are not schema fields — keep them on the
                // mutation for fold field maps, but do not create atoms.
                if !schema.runtime_fields.contains_key(field_name) {
                    continue;
                }
                // Skip type validation for tombstone-shaped values. Without
                // this, repurposed Delete writes would fail against any
                // schema whose fields declare a concrete type (`String`,
                // `Integer`, etc.) because the reserved tombstone shape is
                // a JSON object. The validator is meant to guard the
                // application payload, not the deletion marker.
                if !crate::atom::is_tombstone_value(value) {
                    let field_type = schema.get_field_type(field_name);
                    if let Err(type_err) = field_type.validate(value) {
                        return Err(SchemaError::InvalidData(format!(
                            "Type error in field '{}' of schema '{}': {}. Expected {}, got {}",
                            field_name,
                            schema_name,
                            type_err,
                            field_type,
                            serde_json::to_string(value).unwrap_or_else(|_| "?".to_string())
                        )));
                    }
                }

                let atom = AtomStore::create_atom(
                    schema_name,
                    value.clone(),
                    mutation.source_file_name.clone(),
                    mutation.metadata.clone(),
                )?;
                atoms_to_store.push(atom.clone());
                atom_results.push((idx, field_name.clone(), atom));
            }
        }

        // Batch store all atoms at once, each with the partition of the slot
        // that owns it. Under `AtomKeyEncoding::PartitionPrefix` that co-locates
        // the body with its tips; under `Flat` the partitions are ignored.
        //
        // Pair each created atom with the mutation key + field it came from
        // (`atom_results` is parallel to the order atoms were pushed).
        let located: Vec<(crate::atom::Atom, Option<crate::atom::AtomPartition>)> = atom_results
            .iter()
            .zip(atoms_to_store)
            .map(|((mut_idx, field_name, _), atom)| {
                let key_value = &mutation_key_values[*mut_idx];
                let partition = partition_for_atom_write(
                    schema_name,
                    field_name,
                    key_value,
                    schema,
                    self.db_ops.atoms(),
                );
                (atom, partition)
            })
            .collect();

        let _ = storage_prefix;
        Ok((mutation_key_values, atom_results, Some(located)))
    }

    /// Make the declared HashRange coordinates part of the same field batch as
    /// the caller's payload.
    ///
    /// A mutation can carry its coordinates only in `key_value`. Before this
    /// normalization, that shape advanced payload molecules but left the
    /// declared hash/range-field molecules sparse. Mutating the prepared copy
    /// here keeps the request and mutation-intent wire shapes unchanged while
    /// making atom preparation, changed-key restore, resident apply, Search,
    /// and durable persistence use one complete atomic field set.
    fn materialize_hashrange_key_fields(
        schema_name: &str,
        schema: &Schema,
        mutation: &mut Mutation,
        key_value: &KeyValue,
    ) -> Result<(), SchemaError> {
        let Some(pairs) = hashrange_key_field_pairs(schema_name, schema, mutation, key_value)?
        else {
            return Ok(());
        };
        for (role, (field_name, coordinate)) in
            [KeyRole::Hash, KeyRole::Range].into_iter().zip(pairs)
        {
            if let Some(value) = validated_materialized_key_field(
                schema_name,
                schema,
                mutation,
                field_name,
                coordinate,
                role,
                true,
            )? {
                mutation
                    .fields_and_values
                    .insert(field_name.to_string(), value);
            }
        }
        Ok(())
    }

    fn validate_hashrange_key_fields(
        schema_name: &str,
        schema: &Schema,
        mutation: &Mutation,
        key_value: &KeyValue,
    ) -> Result<(), SchemaError> {
        let Some(pairs) = hashrange_key_field_pairs(schema_name, schema, mutation, key_value)?
        else {
            return Ok(());
        };
        for (role, (field_name, coordinate)) in
            [KeyRole::Hash, KeyRole::Range].into_iter().zip(pairs)
        {
            validated_materialized_key_field(
                schema_name,
                schema,
                mutation,
                field_name,
                coordinate,
                role,
                false,
            )?;
        }
        Ok(())
    }

    /// Build, per field this batch writes, the set of keys it will touch — the
    /// O(changed) unit. Each mutation upserts every field in
    /// `fields_and_values` at its `key_value`, so the touched key for a field is
    /// derived from the field variant's kind (Hash/Range/HashRange) and the
    /// mutation's key. A `Single` field has no per-key shape and is omitted
    /// (its whole-blob molecule is already O(1)). The returned map drives the
    /// O(changed) write-path molecule restore in
    /// [`Self::restore_missing_molecules`].
    pub(in crate::fold_db_core::mutation_manager) fn changed_keys_by_field(
        schema: &Schema,
        schema_mutations: &[Mutation],
        mutation_key_values: &[KeyValue],
    ) -> HashMap<String, HashSet<ChangedKey>> {
        let mut by_field: HashMap<String, HashSet<ChangedKey>> = HashMap::new();
        for (mutation, key_value) in schema_mutations.iter().zip(mutation_key_values.iter()) {
            // Delete is hard-erased outside this path; only Create/Update
            // fields land here.
            let field_names: Vec<&String> = mutation.fields_and_values.keys().collect();
            for field_name in field_names {
                let Some(field) = schema.runtime_fields.get(field_name) else {
                    continue;
                };
                let Some(changed_key) = field.changed_key_for(key_value) else {
                    continue; // Single — no per-key shape
                };
                by_field
                    .entry(field_name.clone())
                    .or_default()
                    .insert(changed_key);
            }
        }
        by_field
    }

    // `schema_has_active_share_rule` was removed: it existed only to force
    // `needs_full_load` in `restore_missing_molecules` so share fan-out could
    // rewrite from the in-memory molecule. `store_molecules_split` now re-reads
    // the primary namespace instead (fold #1224), so that full hydration had no
    // remaining consumer. Share-rule detection for fan-out still lives in
    // `persist_modified_molecules` via `list_share_rules_in_ops`.
}

/// Which HashRange coordinate a key field carries.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyRole {
    Hash,
    Range,
}

/// HashRange mutations whose explicit range-field value differed from the
/// range coordinate and kept the payload value (process lifetime).
static HASHRANGE_KEY_FIELD_CONFLICTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Count one conflict and WARN on the 1st, 2nd, 4th, 8th, … so a writer that
/// conflicts on every row (LastGit CI status) cannot flood the log.
fn note_key_field_conflict(schema_name: &str, field_name: &str, payload: &str, coordinate: &str) {
    let seen = HASHRANGE_KEY_FIELD_CONFLICTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if seen.is_power_of_two() {
        tracing::warn!(
            schema = schema_name,
            field = field_name,
            payload,
            coordinate,
            conflicts = seen,
            "HashRange range field value differs from key_value; keeping the payload value"
        );
    }
}

fn scalar_key_coordinate(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn hashrange_key_field_pairs<'a>(
    schema_name: &str,
    schema: &'a Schema,
    mutation: &Mutation,
    key_value: &'a KeyValue,
) -> Result<Option<[(&'a str, &'a str); 2]>, SchemaError> {
    if !matches!(schema.schema_type, DeclarativeSchemaType::HashRange) {
        return Ok(None);
    }
    let key = schema.key.as_ref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' has no key configuration"
        ))
    })?;
    let hash_field = key.hash_field.as_deref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' has no declared hash field"
        ))
    })?;
    let range_field = key.range_field.as_deref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' has no declared range field"
        ))
    })?;
    let hash = key_value.hash.as_deref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' mutation {} has no hash coordinate",
            mutation.uuid
        ))
    })?;
    let range = key_value.range.as_deref().ok_or_else(|| {
        SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' mutation {} has no range coordinate",
            mutation.uuid
        ))
    })?;
    Ok(Some([(hash_field, hash), (range_field, range)]))
}

fn validated_materialized_key_field(
    schema_name: &str,
    schema: &Schema,
    mutation: &Mutation,
    field_name: &str,
    coordinate: &str,
    role: KeyRole,
    note_conflict: bool,
) -> Result<Option<serde_json::Value>, SchemaError> {
    if !schema.runtime_fields.contains_key(field_name) {
        return Err(SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' key field '{field_name}' is not a runtime field"
        )));
    }
    if let Some(explicit) = mutation.fields_and_values.get(field_name) {
        // A legacy tombstone is a control marker, not a caller-supplied key
        // coordinate. Preserve it so bounded tombstone drains and old homes
        // can still remove a key-field tip.
        if crate::atom::is_tombstone_value(explicit) {
            return Ok(None);
        }
        let explicit_coordinate = scalar_key_coordinate(explicit).ok_or_else(|| {
            SchemaError::InvalidData(format!(
                "HashRange schema '{schema_name}' mutation {} key field '{field_name}' must be a scalar",
                mutation.uuid
            ))
        })?;
        if explicit_coordinate != coordinate {
            // A conflicting HASH field would store a row that partition reads
            // on its own hash no longer return, so it stays refused.
            if role == KeyRole::Hash {
                return Err(SchemaError::InvalidData(format!(
                    "HashRange schema '{schema_name}' mutation {} key field '{field_name}' conflicts with key_value: payload={explicit_coordinate:?}, coordinate={coordinate:?}",
                    mutation.uuid
                )));
            }
            // A conflicting RANGE field keeps the caller's value, as before
            // key-field materialization. Tom's primary (2026-09-29) has
            // LastgitCiStatus with `range_field = event_id` (the
            // CiStatus/CrEvent interleave), while LastGit writes range =
            // `repo:oid:context` and a separate `event_id` lease value that
            // its CAS reads back. Refusing failed every CI status write on a
            // candidate build; overwriting with the coordinate would break the
            // lease CAS. The row is still keyed by `key_value`.
            if note_conflict {
                note_key_field_conflict(schema_name, field_name, &explicit_coordinate, coordinate);
            }
        }
        return Ok(None);
    }

    let value = materialized_key_value(schema.get_field_type(field_name), coordinate).ok_or_else(
        || {
            SchemaError::InvalidData(format!(
                "HashRange schema '{schema_name}' cannot materialize key field '{field_name}' with type {} from coordinate {coordinate:?}",
                schema.get_field_type(field_name)
            ))
        },
    )?;
    if scalar_key_coordinate(&value).as_deref() != Some(coordinate) {
        return Err(SchemaError::InvalidData(format!(
            "HashRange schema '{schema_name}' key field '{field_name}' type {} cannot preserve coordinate {coordinate:?}",
            schema.get_field_type(field_name)
        )));
    }
    Ok(Some(value))
}

fn materialized_key_value(
    field_type: &FieldValueType,
    coordinate: &str,
) -> Option<serde_json::Value> {
    use serde_json::Value;

    match field_type {
        FieldValueType::Any | FieldValueType::String => Some(Value::String(coordinate.to_string())),
        FieldValueType::Integer => coordinate
            .parse::<i64>()
            .map(serde_json::Number::from)
            .or_else(|_| coordinate.parse::<u64>().map(serde_json::Number::from))
            .ok()
            .map(Value::Number),
        FieldValueType::Float | FieldValueType::Number => coordinate
            .parse::<serde_json::Number>()
            .ok()
            .map(Value::Number),
        FieldValueType::Boolean => coordinate.parse::<bool>().ok().map(Value::Bool),
        FieldValueType::OneOf(variants) => variants.iter().find_map(|variant| {
            materialized_key_value(variant, coordinate)
                .filter(|value| scalar_key_coordinate(value).as_deref() == Some(coordinate))
        }),
        FieldValueType::Null
        | FieldValueType::Array(_)
        | FieldValueType::Map(_)
        | FieldValueType::Object(_)
        | FieldValueType::SchemaRef(_) => None,
    }
}

/// Partition for a field write: same bytes the tip walk will scan.
///
/// Uses `deterministic_molecule_uuid(schema, field)` so a first write (no
/// molecule uuid on the field yet) still names the correct slot. Hash segment
/// is storage-form via the atom store's key codec (blinded when enabled).
fn partition_for_atom_write(
    schema_name: &str,
    field_name: &str,
    key_value: &KeyValue,
    schema: &Schema,
    atoms: &AtomStore,
) -> Option<crate::atom::AtomPartition> {
    use crate::atom::{deterministic_molecule_uuid, AtomPartition};
    use crate::schema::types::field::FieldKind;

    let mol_uuid = schema
        .runtime_fields
        .get(field_name)
        .and_then(|f| f.common().molecule_uuid().cloned())
        .unwrap_or_else(|| deterministic_molecule_uuid(schema_name, field_name));

    let kind = schema
        .runtime_fields
        .get(field_name)
        .map_or(FieldKind::Single, |f| f.kind);

    let api_hash = match kind {
        FieldKind::Hash | FieldKind::HashRange => key_value.hash.as_deref().unwrap_or(""),
        FieldKind::Range | FieldKind::Single => "",
    };

    let storage_hash = if api_hash.is_empty() {
        String::new()
    } else {
        match atoms.storage_hash(&mol_uuid, api_hash) {
            Ok(h) => h,
            Err(e) => {
                // Degrade to flat placement rather than fail the whole write:
                // a bad hash encoding is a locality miss, not data loss.
                warn!(
                    schema = %schema_name,
                    field = %field_name,
                    error = %e,
                    "partition_for_atom_write: storage_hash failed; body will be flat"
                );
                return None;
            }
        }
    };

    Some(AtomPartition::for_slot(&mol_uuid, &storage_hash))
}
