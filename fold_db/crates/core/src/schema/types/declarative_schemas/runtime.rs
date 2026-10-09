use super::{DeclarativeSchemaDefinition, FieldMapper};
use std::collections::HashMap;

impl DeclarativeSchemaDefinition {
    /// Clone this schema for the **query read path** without deep-copying the
    /// in-memory hydrated molecules on its `runtime_fields`.
    ///
    /// The query executor needs an owned, mutable schema to drive
    /// `resolve_value` (it temporarily mutates each field while hydrating), but
    /// it never reads the molecule it was handed — every read re-hydrates from
    /// storage keyed off `FieldCommon::molecule_uuid`. A plain `.clone()` of the
    /// registry's cached schema therefore copies every field's full hydrated
    /// molecule (one Sled value holding every key for that field) only for it to
    /// be overwritten before first use — an O(field cardinality) per-query cost
    /// that re-introduced linear scaling on top of the now-O(1) keyed storage
    /// read (#905). This clone strips those molecules (each becomes `None`),
    /// keeping the per-query schema copy flat in the field's data size while
    /// preserving all structure the executor relies on.
    ///
    /// Takes `&mut self` so the heavy molecules are never themselves
    /// deep-copied: they are moved out (`std::mem::take`) before the derived
    /// `Clone` runs and restored immediately after, leaving `self` observably
    /// unchanged. Callers hold the registry mutex, so the brief `&mut` borrow of
    /// the cached schema is local to the read.
    #[must_use]
    pub fn clone_for_read(&mut self) -> Self {
        // 1. Build molecule-stripped field clones up front (cheap — FieldCommon
        //    only, no molecule bytes).
        let stripped_fields: HashMap<String, crate::schema::types::field::FieldVariant> = self
            .runtime_fields
            .iter()
            .map(|(name, field)| (name.clone(), field.cloned_without_molecule()))
            .collect();

        // 2. Clone the rest of the schema without the molecules: move the
        //    hydrated `runtime_fields` aside, clone (now cheap), then restore so
        //    `self` is unchanged.
        let original_fields = std::mem::take(&mut self.runtime_fields);
        let mut cloned = self.clone();
        self.runtime_fields = original_fields;

        // 3. Hand the clone the stripped fields.
        cloned.runtime_fields = stripped_fields;
        cloned
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
        use std::collections::HashMap;

        // Reject cross-app FieldMappers before deriving any deterministic
        // molecule UUIDs from them — see method docs for the threat model.
        self.validate_field_mapper_ownership()?;
        self.validate_record_mapper_ownership()?;

        // Reject FieldMappers whose target field isn't declared in `fields`
        // or `transform_fields` — see method docs for why this asymmetry
        // is a real bug, not cosmetic.
        self.validate_field_mapper_targets()?;

        let default_field_mappers = HashMap::new();

        let mut runtime_fields = HashMap::new();
        let mut add_field = |field_name: String| {
            let schema_type = self.schema_type.clone();
            runtime_fields.insert(
                field_name,
                FieldVariant::from_schema_type(schema_type, default_field_mappers.clone()),
            );
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

        if let Some(field_mappers) = &self.field_mappers {
            for (field_name, mapper) in field_mappers {
                if let Some(field) = self.runtime_fields.get_mut(field_name) {
                    let mut mapper_map = HashMap::new();
                    mapper_map.insert(field_name.clone(), mapper.clone());
                    field.common_mut().set_field_mappers(mapper_map);
                }
            }
        }

        // Restore persisted molecule UUIDs from field_molecule_uuids if available.
        // Otherwise derive the UUID deterministically:
        //
        //  - A field with a `FieldMapper` resolves to its mapper's *source*
        //    schema/field. This makes mapper resolution durable: the schema
        //    service wire JSON carries `field_mappers` but not
        //    `field_molecule_uuids`, and `apply_field_mappers` only runs once,
        //    during schema expansion. Without routing mapped fields to the
        //    source molecule here, a reload of an already-expanded mapped
        //    schema (node restart re-sync, schema refresh) would fall back to
        //    this schema's own empty molecule and leave every mapped field
        //    permanently unreadable.
        //  - An unmapped field derives from this schema's own name + field name.
        //
        // See `crate::schema::field_mapper` for the one-shot approval path.
        let persisted = self.field_molecule_uuids.clone().unwrap_or_default();
        let field_mappers = self.field_mappers.clone().unwrap_or_default();
        let schema_name = self.name.clone();
        for (field_name, field) in &mut self.runtime_fields {
            let mol_uuid = if let Some(uuid) = persisted.get(field_name) {
                uuid.clone()
            } else {
                let (src_schema, src_field) =
                    Self::resolve_molecule_source(&schema_name, &field_mappers, field_name);
                crate::atom::deterministic_molecule_uuid(src_schema, src_field)
            };
            field.common_mut().set_molecule_uuid(mol_uuid);
        }

        self.ensure_record_molecule_runtime_field();

        // Regenerate transform metadata that isn't persisted (marked with #[serde(skip)])
        // This is needed when schemas are loaded from the database
        self.regenerate_metadata();

        Ok(())
    }

    /// Attach the runtime-only record molecule field when `molecule_uuid` is set.
    ///
    /// `RECORD_SENTINEL` is not a declared catalog field. Compact and document
    /// writes tip R through this runtime field.
    pub fn ensure_record_molecule_runtime_field(&mut self) {
        use crate::schema::types::field::FieldVariant;
        use crate::schema::types::RECORD_SENTINEL;
        use std::collections::HashMap;

        let Some(record_uuid) = self.molecule_uuid.clone() else {
            self.runtime_fields.remove(RECORD_SENTINEL);
            return;
        };
        if let Some(field) = self.runtime_fields.get_mut(RECORD_SENTINEL) {
            field.common_mut().set_molecule_uuid(record_uuid);
            return;
        }
        let mut field = FieldVariant::from_schema_type(self.schema_type.clone(), HashMap::new());
        field.common_mut().set_molecule_uuid(record_uuid);
        self.runtime_fields
            .insert(RECORD_SENTINEL.to_string(), field);
    }

    /// Enforce app_identity v3.1 molecule isolation at schema load time.
    fn validate_field_mapper_ownership(&self) -> Result<(), crate::schema::SchemaError> {
        use crate::schema::SchemaError;

        let Some(owner) = self.owner_app_id.as_deref().filter(|s| !s.is_empty()) else {
            return Ok(());
        };

        let Some(mappers) = self.field_mappers.as_ref() else {
            return Ok(());
        };

        for (target_field, mapper) in mappers {
            let (source_owner_opt, _) = Self::parse_canonical_name(mapper.source_schema());
            match source_owner_opt.as_deref() {
                Some(source_owner) if source_owner != owner => {
                    return Err(SchemaError::InvalidData(format!(
                        "schema '{}' (owner_app_id='{}') declares a FieldMapper at field \
                         '{}' pointing at source schema '{}', which is owned by a different \
                         app ('{}'). Cross-app FieldMappers are forbidden because they would \
                         bypass app_identity Read gating by routing reads onto another app's \
                         molecules.",
                        self.canonical_name(),
                        owner,
                        target_field,
                        mapper.source_schema(),
                        source_owner,
                    )));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Same isolation as FieldMapper: a RecordMapper must not name another
    /// app's schema. That would route the record molecule onto a foreign R.
    fn validate_record_mapper_ownership(&self) -> Result<(), crate::schema::SchemaError> {
        use crate::schema::SchemaError;

        let Some(owner) = self.owner_app_id.as_deref().filter(|s| !s.is_empty()) else {
            return Ok(());
        };
        let Some(mapper) = self.record_mapper.as_ref() else {
            return Ok(());
        };
        let (source_owner_opt, _) = Self::parse_canonical_name(mapper.source_schema());
        match source_owner_opt.as_deref() {
            Some(source_owner) if source_owner != owner => {
                return Err(SchemaError::InvalidData(format!(
                    "schema '{}' (owner_app_id='{}') declares a RecordMapper \
                     pointing at source schema '{}', which is owned by a different \
                     app ('{}'). Cross-app RecordMappers are forbidden because they \
                     would bypass app_identity Read gating by routing onto another \
                     app's record molecule.",
                    self.canonical_name(),
                    owner,
                    mapper.source_schema(),
                    source_owner,
                )));
            }
            _ => {}
        }
        Ok(())
    }

    /// Reject `FieldMapper` entries whose target field isn't declared in
    /// `fields` or `transform_fields`.
    fn validate_field_mapper_targets(&self) -> Result<(), crate::schema::SchemaError> {
        use crate::schema::SchemaError;

        let Some(mappers) = self.field_mappers.as_ref() else {
            return Ok(());
        };
        if mappers.is_empty() {
            return Ok(());
        }

        let known_fields: std::collections::HashSet<&str> = self
            .fields
            .iter()
            .flatten()
            .map(String::as_str)
            .chain(
                self.transform_fields
                    .iter()
                    .flatten()
                    .map(|(name, _)| name.as_str()),
            )
            .collect();

        for target_field in mappers.keys() {
            if !known_fields.contains(target_field.as_str()) {
                return Err(SchemaError::InvalidData(format!(
                    "schema '{}' declares a FieldMapper for target field '{}' \
                     that is not present in `fields` or `transform_fields`. \
                     Dangling FieldMappers are silently dropped during \
                     `populate_runtime_fields` but cause an \
                     'internally inconsistent' error at schema expansion; \
                     reject at parse time instead.",
                    self.canonical_name(),
                    target_field,
                )));
            }
        }

        Ok(())
    }

    /// Resolve the `(schema, field)` whose deterministic molecule UUID a field
    /// should adopt when no persisted `field_molecule_uuids` entry is available.
    fn resolve_molecule_source<'a>(
        schema_name: &'a str,
        field_mappers: &'a HashMap<String, FieldMapper>,
        field_name: &'a str,
    ) -> (&'a str, &'a str) {
        let mut cur_schema = schema_name;
        let mut cur_field = field_name;
        let mut visited = std::collections::HashSet::new();
        // Only mappers belonging to *this* schema are inspectable here; a hop
        // into a different schema ends the walk. `visited` guards mapper cycles.
        while cur_schema == schema_name && visited.insert(cur_field) {
            match field_mappers.get(cur_field) {
                Some(mapper) => {
                    cur_schema = mapper.source_schema();
                    cur_field = mapper.source_field();
                }
                None => break,
            }
        }
        (cur_schema, cur_field)
    }

    /// Copies molecule UUIDs from runtime_fields into the persisted field_molecule_uuids map.
    /// Called after mutations so that the UUIDs survive serialization to DB.
    pub fn sync_molecule_uuids(&mut self) {
        let mut uuids = HashMap::new();
        for (field_name, field) in &self.runtime_fields {
            if field_name == crate::schema::types::RECORD_SENTINEL {
                continue;
            }
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
