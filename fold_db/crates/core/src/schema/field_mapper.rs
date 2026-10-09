//! Field mapper service for schema expansion.
//!
//! When a schema is expanded (superseded by a new schema that adds fields), the new
//! target schema's shared fields carry `FieldMapper` entries pointing back at the old
//! source schema's fields. During expansion this service copies the source molecule
//! UUIDs onto the target runtime fields so that reads/writes against the new schema
//! land on the same molecules — no data migration required.
//!
//! ## When `apply_field_mappers` is called
//!
//! It is invoked explicitly via `SchemaCore::apply_field_mappers` by the schema-expansion
//! path, after `validate_field_mapper_compatibility` and before the old (source) schema is
//! blocked. It is a no-op for schemas without `field_mappers`.
//!
//! ## Circular redirect handling
//!
//! During schema expansion the old source schema is typically already `Blocked` and
//! has been recorded in the `superseded_by` map as pointing at the new target schema.
//! If we resolved the source through the normal `SchemaCore::get_schema` path we would
//! follow the redirect and end up looking at the target (the schema currently being
//! approved) — a circular lookup. To avoid this, the service reads the source schema
//! directly from `DbOperations::get_schema`, which bypasses the redirect map and
//! returns the raw stored schema (with its original molecule UUIDs intact).
//!
//! ## Schema expansion workflow
//!
//! 1. A new superset schema is created whose shared fields reference the old schema
//!    via `FieldMapper { source_schema, source_field }`.
//! 2. `apply_field_mappers` is called on the new schema (then the old schema is
//!    blocked and a `superseded_by` entry is recorded).
//! 3. For each `(target_field, mapper)` entry, the service walks the chain
//!    transitively — `mapper.source_schema/source_field`, then any mapper on that
//!    schema's field, until reaching either a persisted `field_molecule_uuids`
//!    entry or an unmapped chain root — and copies the root's `molecule_uuid`
//!    onto the target runtime field. The transitive walk is required because
//!    `populate_runtime_fields::resolve_molecule_source` only follows mappers
//!    inside the same schema, so a multi-hop chain whose intermediates haven't
//!    themselves been `apply_field_mappers`'d yet would otherwise resolve to
//!    `deterministic(<intermediate>, <field>)` — an empty molecule that misses
//!    the chain root's data.
//! 4. The mutated schema is re-synced (`sync_molecule_uuids`) and persisted.
//!
//! New fields (those without a mapper) are left untouched — they receive a fresh
//! molecule UUID on first mutation.
//!
//! ## Source schema not installed at all (vs. deleted)
//!
//! `populate_runtime_fields` already gives every mapped field a single-hop
//! deterministic fallback (`deterministic(mapper.source_schema, mapper.source_field)`)
//! at schema-load time — before this service ever runs. That fallback is the
//! *correct* molecule identity for a source schema that is an unmapped chain
//! root, whether or not that source happens to be installed on this node right
//! now. So when a chain hop's source schema cannot be found in the database at
//! all, this service stops walking that chain and leaves the field's existing
//! fallback untouched rather than failing the whole expand — there is nothing to
//! copy, but nothing has been lost either. The `FieldMapper` metadata itself is
//! never dropped — `populate_runtime_fields` stamps it onto the target field at
//! schema-load time, independent of this service — so a later call, once the
//! source is installed, walks the chain again and recovers the true root if one
//! exists deeper than one hop.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::db_operations::DbOperations;
use crate::schema::types::{Schema, SchemaError};

/// One step of a `FieldMapper` chain walk. `Hop` advances to a referenced
/// `(schema, field)`; `Terminal` ends the walk with the field's molecule_uuid
/// (`None` if the chain root never had one).
enum ChainStep {
    Hop(String, String),
    Terminal(Option<String>),
}

/// Extracts and applies `FieldMapper` entries during schema approval.
///
/// Holds a reference to `DbOperations` (for direct, redirect-bypassing schema reads
/// and for persisting the updated target schema) and a handle to the in-memory
/// schema cache owned by `SchemaCore` (so updates are visible immediately without
/// requiring a reload).
pub struct FieldMapperService {
    db_ops: Arc<DbOperations>,
    schemas_cache: Arc<RwLock<HashMap<String, Schema>>>,
}

impl FieldMapperService {
    pub fn new(
        db_ops: Arc<DbOperations>,
        schemas_cache: Arc<RwLock<HashMap<String, Schema>>>,
    ) -> Self {
        Self {
            db_ops,
            schemas_cache,
        }
    }

    /// Apply every `FieldMapper` on `schema_name`, copying molecule UUIDs from the
    /// referenced source fields onto the corresponding target runtime fields.
    ///
    /// This is a no-op when the schema has no field mappers. See the module docs for
    /// the full workflow and circular-redirect rationale.
    ///
    /// NOTE: the field-mapper molecule-UUID chain resolved here is a SEPARATE
    /// chain from the `superseded_by` supersession chain. It intentionally
    /// reads schemas raw via `db_ops.get_schema` to avoid the supersession
    /// redirect (which would break the circularity the module docs describe).
    /// It is NOT covered by the supersession-naming reframe — do not route it
    /// through `get_schema_following_supersession`.
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        let mut schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;

        let field_mappers = schema.field_mappers().cloned().unwrap_or_default();

        let mut source_cache: HashMap<String, Option<Schema>> = HashMap::new();
        let mut updated = false;

        if !field_mappers.is_empty() {
            for (target_field, mapper) in field_mappers {
                // Resolve the chain transitively rather than reading
                // `source_field.molecule_uuid` directly. `populate_runtime_fields`
                // only follows mappers inside the same schema — a single hop
                // across the schema boundary returns `deterministic(<next_schema>,
                // <next_field>)` regardless of whether that next field is *itself*
                // mapped. So in a chain V0 <- V1 <- V2 <- V3 where intermediate
                // schemas have not been apply_field_mappers'd, V2.a.molecule_uuid
                // is the stale single-hop value `deterministic("V1", "a")`, never
                // V0.a's UUID where data actually lives. Copying that onto V3.a
                // silently misroutes reads.
                let molecule_uuid = self
                    .resolve_chained_molecule_uuid(
                        mapper.source_schema(),
                        mapper.source_field(),
                        schema_name,
                        &mut source_cache,
                    )
                    .await?;

                // If the chain root never had a molecule UUID (cold schema, no
                // data written yet), skip — the target field will get a fresh
                // molecule on first mutation.
                let Some(molecule_uuid) = molecule_uuid else {
                    continue;
                };

                let Some(target_runtime_field) = schema.runtime_fields.get_mut(&target_field)
                else {
                    return Err(SchemaError::InvalidData(format!(
                    "apply_field_mappers: target field '{target_field}' missing from runtime_fields \
                     for schema '{schema_name}' (the schema's own field_mappers reference a field \
                     that isn't in runtime_fields — schema is internally inconsistent)",
                )));
                };

                target_runtime_field
                    .common_mut()
                    .set_molecule_uuid(molecule_uuid.clone());
                target_runtime_field
                    .common_mut()
                    .set_field_mappers(HashMap::from([(target_field.clone(), mapper.clone())]));

                updated = true;
            }
        } // field_mappers non-empty

        if self
            .copy_record_molecule_uuid(&mut schema, schema_name, &mut source_cache)
            .await?
        {
            updated = true;
        }

        if updated {
            schema.sync_molecule_uuids();
            self.db_ops.store_schema(schema_name, &schema).await?;
            self.schemas_cache
                .write()
                .map_err(|_| {
                    SchemaError::InvalidData("Failed to acquire schemas write lock".into())
                })?
                .insert(schema_name.to_string(), schema);
        }

        Ok(())
    }

    /// Copy the predecessor's record molecule UUID onto `schema_name` when
    /// `record_mapper` is set and the chain root already has R.
    ///
    /// No-op when there is no RecordMapper, the source is not installed, or
    /// the predecessor has no `molecule_uuid` yet (today's catalogs). Always
    /// leaves FieldMappers in place.
    pub async fn apply_record_mapper(&self, schema_name: &str) -> Result<(), SchemaError> {
        let mut schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;
        let mut source_cache: HashMap<String, Option<Schema>> = HashMap::new();
        if !self
            .copy_record_molecule_uuid(&mut schema, schema_name, &mut source_cache)
            .await?
        {
            return Ok(());
        }
        schema.sync_molecule_uuids();
        self.db_ops.store_schema(schema_name, &schema).await?;
        self.schemas_cache
            .write()
            .map_err(|_| SchemaError::InvalidData("Failed to acquire schemas write lock".into()))?
            .insert(schema_name.to_string(), schema);
        Ok(())
    }

    /// Follow `record_mapper` to the chain root and copy R when it is Some.
    /// Returns true when `schema.molecule_uuid` changed.
    async fn copy_record_molecule_uuid(
        &self,
        schema: &mut Schema,
        schema_name: &str,
        source_cache: &mut HashMap<String, Option<Schema>>,
    ) -> Result<bool, SchemaError> {
        let Some(mapper) = schema.record_mapper.clone() else {
            return Ok(false);
        };
        let Some(uuid) = self
            .resolve_chained_record_uuid(mapper.source_schema(), schema_name, source_cache)
            .await?
        else {
            return Ok(false);
        };
        if schema.molecule_uuid.as_deref() == Some(uuid.as_str()) {
            return Ok(false);
        }
        schema.molecule_uuid = Some(uuid);
        Ok(true)
    }

    /// Walk RecordMapper hops until a schema with `molecule_uuid`, or a root
    /// with none. Mapper hops win over a stale persisted UUID on an
    /// intermediate, same as FieldMapper.
    async fn resolve_chained_record_uuid(
        &self,
        initial_schema_name: &str,
        target_schema_name: &str,
        source_cache: &mut HashMap<String, Option<Schema>>,
    ) -> Result<Option<String>, SchemaError> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut current = initial_schema_name.to_string();
        loop {
            if !visited.insert(current.clone()) {
                return Err(SchemaError::InvalidData(format!(
                    "apply_record_mapper: cyclic RecordMapper chain detected at \
                     '{current}' (chain rooted at target schema '{target_schema_name}')",
                )));
            }
            let Some(schema) = self.resolve_source_schema(&current, source_cache).await? else {
                tracing::warn!(
                    target: "fold_db::schema::field_mapper",
                    source_schema = %current,
                    target_schema = %target_schema_name,
                    "apply_record_mapper: source schema not installed; no R copy this pass",
                );
                return Ok(None);
            };
            if let Some(next) = schema.record_mapper.as_ref() {
                current = next.source_schema().to_string();
                continue;
            }
            return Ok(schema
                .molecule_uuid
                .as_ref()
                .filter(|u| !u.is_empty())
                .cloned());
        }
    }

    /// Walk a `FieldMapper` chain from `(initial_schema, initial_field)` back to
    /// its root, returning the chain root's molecule_uuid.
    ///
    /// At each step:
    /// 1. If the field has an entry in the schema's `field_mappers`, advance
    ///    to that mapper's `(source_schema, source_field)`. The mapper wins
    ///    over the persisted UUID — `sync_molecule_uuids` runs on every
    ///    mutation and persists whatever `populate_runtime_fields` resolved
    ///    `runtime_fields[field].molecule_uuid` to, which for a multi-hop
    ///    chain is the in-schema *single-hop* fallback rather than the real
    ///    chain root. So a mutation against an intermediate schema fired
    ///    before its own `apply_field_mappers` writes a *stale*
    ///    `field_molecule_uuids` entry that does not match the chain root.
    ///    Following the mapper bypasses that stale state and always lands
    ///    at the unmapped root.
    /// 2. Otherwise, if the schema has a persisted entry in
    ///    `field_molecule_uuids` for the field, that UUID is authoritative —
    ///    a field with no mapper is itself the chain root, so its persisted
    ///    UUID is the chain root's UUID.
    /// 3. Otherwise the field is the chain root: return its runtime
    ///    molecule_uuid (set deterministically by `populate_runtime_fields`).
    ///
    /// Cycle detection via a `(schema, field)` visited set surfaces a clear
    /// error rather than spinning forever.
    ///
    /// Returns `Ok(None)` both for a genuine cold chain root (schema loaded,
    /// field never mutated) and for a hop whose schema is not installed on
    /// this node at all — see the module docs' "source schema not installed
    /// at all" section for why the latter is safe to treat as "nothing to
    /// improve" rather than an error.
    ///
    /// `target_schema_name` is only used for error context — it is the schema
    /// whose mapper kicked off this walk.
    async fn resolve_chained_molecule_uuid(
        &self,
        initial_schema_name: &str,
        initial_field_name: &str,
        target_schema_name: &str,
        source_cache: &mut HashMap<String, Option<Schema>>,
    ) -> Result<Option<String>, SchemaError> {
        let mut visited: HashSet<(String, String)> = HashSet::new();
        let mut current_schema = initial_schema_name.to_string();
        let mut current_field = initial_field_name.to_string();

        loop {
            if !visited.insert((current_schema.clone(), current_field.clone())) {
                return Err(SchemaError::InvalidData(format!(
                    "apply_field_mappers: cyclic FieldMapper chain detected at \
                     '{current_schema}.{current_field}' (chain rooted at target schema \
                     '{target_schema_name}')",
                )));
            }

            let next_step: ChainStep = {
                let Some(schema) = self
                    .resolve_source_schema(&current_schema, source_cache)
                    .await?
                else {
                    tracing::warn!(
                        target: "fold_db::schema::field_mapper",
                        source_schema = %current_schema,
                        target_schema = %target_schema_name,
                        "apply_field_mappers: source schema not installed; carrying the \
                         FieldMapper forward unresolved (no molecule_uuid copy this pass)",
                    );
                    return Ok(None);
                };

                if let Some(next_mapper) = schema
                    .field_mappers
                    .as_ref()
                    .and_then(|m| m.get(&current_field))
                {
                    ChainStep::Hop(
                        next_mapper.source_schema().to_string(),
                        next_mapper.source_field().to_string(),
                    )
                } else if let Some(uuid) = schema
                    .field_molecule_uuids
                    .as_ref()
                    .and_then(|m| m.get(&current_field))
                    .cloned()
                {
                    return Ok(Some(uuid));
                } else {
                    let Some(field) = schema.runtime_fields.get(&current_field) else {
                        return Err(SchemaError::InvalidData(format!(
                            "apply_field_mappers: source field '{current_schema}.{current_field}' \
                             missing from runtime_fields for schema '{target_schema_name}' (mapper \
                             points at a field that doesn't exist in the source schema — this is a \
                             malformed FieldMapper)",
                        )));
                    };
                    ChainStep::Terminal(field.common().molecule_uuid().cloned())
                }
            };

            match next_step {
                ChainStep::Hop(schema, field) => {
                    current_schema = schema;
                    current_field = field;
                }
                ChainStep::Terminal(uuid) => return Ok(uuid),
            }
        }
    }

    /// Load a source schema for field mapping, caching the result for the duration
    /// of a single `apply_field_mappers` call.
    ///
    /// Uses `DbOperations::get_schema` directly (bypassing any `superseded_by`
    /// redirect) because during schema expansion the source schema is typically
    /// already blocked and redirected to the target currently being approved.
    /// Following that redirect would produce a circular lookup. We need the raw
    /// source schema with its original molecule UUIDs.
    ///
    /// Returns `Ok(None)` if the source schema does not exist in the database at
    /// all (dangling mapper — deleted, or never installed on this node). The
    /// caller treats that as "nothing to improve" rather than an error: see the
    /// module docs' "source schema not installed at all" section. Returns
    /// `Err(SchemaError::InvalidData)` only for a genuine load failure.
    async fn resolve_source_schema<'a>(
        &self,
        source_schema_name: &str,
        source_cache: &'a mut HashMap<String, Option<Schema>>,
    ) -> Result<Option<&'a Schema>, SchemaError> {
        if !source_cache.contains_key(source_schema_name) {
            let fetched = self
                .db_ops
                .get_schema(source_schema_name)
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "apply_field_mappers: failed to load source schema '{source_schema_name}': {e}",
                    ))
                })?;
            source_cache.insert(source_schema_name.to_string(), fetched);
        }
        Ok(source_cache
            .get(source_schema_name)
            .and_then(|opt| opt.as_ref()))
    }
}
