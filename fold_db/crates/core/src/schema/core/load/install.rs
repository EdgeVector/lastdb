//! Durable schema reload preparation and delayed native cache publication.

use super::{Schema, SchemaCore, SchemaError, SchemaState};

impl SchemaCore {
    /// Reload over a stored row: merge molecule maps, persist, cache, and keep
    /// the stored schema state.
    pub(super) async fn refresh_existing_schema(
        &self,
        name: &str,
        mut schema: Schema,
        mut existing_schema: Schema,
    ) -> Result<(), SchemaError> {
        // A same-hash load is a metadata refresh, not a no-op. This matters
        // when a local/older definition was cached before the catalog
        // identity gained field_mappers: keeping the mapper-less copy makes
        // an expansion look empty even though the identity hash matches.
        //
        // Preserve molecule UUIDs for fields owned by this schema, but let
        // incoming catalog mappers win for mapped fields. Otherwise a UUID
        // created by a premature local mint would override the mapper and
        // keep reads pointed at the wrong molecule.
        let mapped_fields: std::collections::HashSet<String> = schema
            .field_mappers
            .as_ref()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let mut merged_molecules = schema.field_molecule_uuids.take().unwrap_or_default();
        if let Some(existing_molecules) = existing_schema.field_molecule_uuids.take() {
            for (field, molecule_uuid) in existing_molecules {
                if !mapped_fields.contains(&field) {
                    merged_molecules.entry(field).or_insert(molecule_uuid);
                }
            }
        }
        schema.field_molecule_uuids = (!merged_molecules.is_empty()).then_some(merged_molecules);
        schema.populate_runtime_fields()?;
        schema.sync_molecule_uuids();
        schema.clear_runtime_molecules();

        // Persist the refreshed catalog metadata as well as caching it so
        // mapper repair survives a daemon restart.
        self.db_ops.store_schema(name, &schema).await?;

        // Preserve existing state from database
        // `get_schema_state` reports an unreadable durable row as missing.
        // Keep the state boot already cached for this schema in that case,
        // so a reload cannot silently turn a Blocked schema Available.
        let existing_state = self.db_ops.get_schema_state(name).await?;
        let state = match existing_state {
            Some(state) => state,
            None => self.get_schema_state_cached(name)?,
        };
        self.publish_schema_cache(name, schema, state)?;
        Ok(())
    }

    /// New schema - persist to database and update in-memory caches.
    pub(super) async fn install_new_schema(
        &self,
        name: &str,
        schema: Schema,
    ) -> Result<(), SchemaError> {
        self.db_ops.store_schema(name, &schema).await?;
        self.db_ops
            .store_schema_state(name, &SchemaState::Available)
            .await?;

        self.publish_schema_cache(name, schema, SchemaState::Available)?;
        Ok(())
    }
}
