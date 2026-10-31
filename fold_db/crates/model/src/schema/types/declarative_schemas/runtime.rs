use super::DeclarativeSchemaDefinition;
use std::collections::HashMap;

impl DeclarativeSchemaDefinition {
    /// All ordinary runtime fields, in deterministic name order.
    pub fn runtime_field_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.runtime_fields.keys().cloned().collect();
        names.sort();
        names
    }

    /// `&self` twin of [`Self::clone_for_read`], for the **concurrent** read path.
    ///
    /// `clone_for_read` takes `&mut self` purely as an optimization: it
    /// `mem::take`s the hydrated `runtime_fields` aside so the derived `Clone`
    /// never deep-copies a molecule, then restores them. That `&mut` borrow is
    /// what forced the schema cache (`SchemaCore::schemas`) to be held under an
    /// *exclusive* lock on every read, serializing concurrent readers into a
    /// lock convoy (the bug this card disambiguated and fixed). This variant
    /// returns the identical molecule-stripped clone WITHOUT mutating `self`, so
    /// callers hold only a *shared* `RwLock::read()` guard and N readers clone
    /// their schema in parallel.
    ///
    /// Why a plain `self.clone()` is already cheap here: schemas are stored in
    /// the cache molecule-free — `load_schema_internal` calls
    /// `clear_runtime_molecules()` before inserting (#950, "drop materialized
    /// molecules from schema cache → keyed reads O(1)"), so the cached entry's
    /// fields already carry `molecule: None`. The derived `Clone` therefore
    /// copies no molecule bytes. We still rebuild `runtime_fields` through
    /// `cloned_without_molecule()` as belt-and-suspenders so the contract is
    /// identical to `clone_for_read` even if a caller ever hands this a
    /// not-yet-cleared schema: the returned copy is guaranteed molecule-free.
    /// The executor re-hydrates molecules from storage regardless, so a stripped
    /// clone is exactly what it needs.
    #[must_use]
    pub fn clone_for_read_shared(&self) -> Self {
        let mut cloned = self.clone();
        for field in cloned.runtime_fields.values_mut() {
            *field = field.cloned_without_molecule();
        }
        cloned
    }

    /// Populates runtime_fields from declarative schema definition
    /// This is called after deserializing from database to ensure runtime state is initialized
    /// Also regenerates transform metadata (hash mappings, inputs, source schemas) which are not persisted
    pub fn populate_runtime_fields(&mut self) -> Result<(), crate::schema::SchemaError> {
        use crate::schema::types::field::FieldVariant;

        self.validate_definition()
            .map_err(crate::schema::SchemaError::InvalidData)?;

        let mut runtime_fields = HashMap::new();
        let mut add_field = |field_name: String| {
            let schema_type = self.schema_type.clone();
            runtime_fields.insert(field_name, FieldVariant::from_schema_type(schema_type));
        };

        if let Some(field_list) = self.fields.clone() {
            for field_name in field_list {
                add_field(field_name);
            }
        }

        if let Some(transform_map) = self.transform_fields.clone() {
            for (field_name, _) in transform_map {
                add_field(field_name);
            }
        }

        self.runtime_fields = runtime_fields;

        // Restore persisted molecule UUIDs from field_molecule_uuids if
        // available. Otherwise derive the UUID from this schema's own name and
        // the field name: each schema owns the values of its fields.
        let persisted = self.field_molecule_uuids.clone().unwrap_or_default();
        let schema_name = self.name.clone();
        for (field_name, field) in &mut self.runtime_fields {
            let mol_uuid = persisted.get(field_name).cloned().unwrap_or_else(|| {
                crate::atom::deterministic_molecule_uuid(&schema_name, field_name)
            });
            field.common_mut().set_molecule_uuid(mol_uuid);
        }

        // Regenerate transform metadata that isn't persisted (marked with #[serde(skip)])
        // This is needed when schemas are loaded from the database
        self.regenerate_metadata();

        Ok(())
    }

    /// Copies molecule UUIDs from runtime_fields into the persisted field_molecule_uuids map.
    /// Called after mutations so that the UUIDs survive serialization to DB.
    pub fn sync_molecule_uuids(&mut self) {
        let mut uuids = HashMap::new();
        for (field_name, field) in &self.runtime_fields {
            if let Some(uuid) = field.common().molecule_uuid() {
                uuids.insert(field_name.clone(), uuid.clone());
            }
        }
        if !uuids.is_empty() {
            self.field_molecule_uuids = Some(uuids);
        }
    }

    /// Drop every field's materialized in-memory molecule, keeping each field's
    /// `molecule_uuid` so the molecule re-hydrates lazily from disk on the next
    /// read/write.
    pub fn clear_runtime_molecules(&mut self) {
        for field in self.runtime_fields.values_mut() {
            field.clear_molecule();
        }
    }
}
