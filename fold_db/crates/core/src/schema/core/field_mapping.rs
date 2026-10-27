//! Expansion resolves mapper chains against raw stored schemas, never supersession
//! redirects. A mapper takes precedence over an intermediate persisted UUID.
//! Missing sources keep the runtime fallback and mapper metadata for a later retry.

use std::collections::{HashMap, HashSet};

use super::SchemaCore;
use crate::schema::types::{Schema, SchemaError};

mod rules;
use rules::{chain_step, ChainStep};

impl SchemaCore {
    /// Resolve shared-field molecules after mapper compatibility validation.
    /// New fields keep their molecules. Repeated calls remain safe.
    pub async fn apply_field_mappers(&self, schema_name: &str) -> Result<(), SchemaError> {
        let mut schema = self.db_ops.get_schema(schema_name).await?.ok_or_else(|| {
            SchemaError::InvalidData(format!("Schema '{schema_name}' not found in database"))
        })?;
        let field_mappers = schema.field_mappers().cloned().unwrap_or_default();
        let mut source_cache = HashMap::new();
        let mut resolved = false;

        for (target_field, mapper) in field_mappers {
            let Some(molecule_uuid) = self
                .resolve_chained_molecule_uuid(
                    mapper.source_schema(),
                    mapper.source_field(),
                    schema_name,
                    &mut source_cache,
                )
                .await?
            else {
                continue;
            };

            // Resolve the source before checking the target, preserving error order.
            let Some(target) = schema.runtime_fields.get_mut(&target_field) else {
                return Err(SchemaError::InvalidData(format!(
                    "apply_field_mappers: target field '{target_field}' missing from runtime_fields \
                     for schema '{schema_name}' (the schema's own field_mappers reference a field \
                     that isn't in runtime_fields — schema is internally inconsistent)",
                )));
            };
            target.common_mut().set_molecule_uuid(molecule_uuid);
            target
                .common_mut()
                .set_field_mappers(HashMap::from([(target_field, mapper)]));
            // A resolved mapper requires a write even when its UUID is unchanged.
            resolved = true;
        }

        if resolved {
            schema.sync_molecule_uuids();
            self.db_ops.store_schema(schema_name, &schema).await?;
            // Mapper adoption retains its immediate management-cache publication.
            // Native reloads use publish_schema_cache instead.
            self.schemas
                .write()
                .map_err(|_| {
                    SchemaError::InvalidData("Failed to acquire schemas write lock".into())
                })?
                .insert(schema_name.to_string(), schema);
        }
        Ok(())
    }

    async fn resolve_chained_molecule_uuid(
        &self,
        initial_schema_name: &str,
        initial_field_name: &str,
        target_schema_name: &str,
        source_cache: &mut HashMap<String, Option<Schema>>,
    ) -> Result<Option<String>, SchemaError> {
        let mut visited = HashSet::new();
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
            // Both present and absent sources are cached for this adoption call.
            if !source_cache.contains_key(&current_schema) {
                let source = self.db_ops.get_schema(&current_schema).await.map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "apply_field_mappers: failed to load source schema '{current_schema}': {e}",
                    ))
                })?;
                source_cache.insert(current_schema.clone(), source);
            }
            let source = source_cache.get(&current_schema).and_then(Option::as_ref);
            match chain_step(source, &current_field) {
                ChainStep::Follow(schema, field) => {
                    current_schema = schema;
                    current_field = field;
                }
                ChainStep::Resolved(uuid) => return Ok(uuid),
                ChainStep::MissingSource => {
                    tracing::warn!(
                        target: "fold_db::schema::field_mapper",
                        source_schema = %current_schema,
                        target_schema = %target_schema_name,
                        "apply_field_mappers: source schema not installed; carrying the \
                         FieldMapper forward unresolved (no molecule_uuid copy this pass)",
                    );
                    return Ok(None);
                }
                ChainStep::MissingField => {
                    return Err(SchemaError::InvalidData(format!(
                        "apply_field_mappers: source field '{current_schema}.{current_field}' \
                         missing from runtime_fields for schema '{target_schema_name}' (mapper \
                         points at a field that doesn't exist in the source schema — this is a \
                         malformed FieldMapper)",
                    )));
                }
            }
        }
    }
}
