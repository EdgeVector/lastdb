use super::*;
use crate::atom::file_pointer::blob_refs_of_atom;
use crate::schema::types::aggregate::{
    AGGREGATE_GUARD_TOKEN_FIELD, AGGREGATE_MEMBER_RESERVED_FIELDS, AGGREGATE_VALID_FIELD,
};
use crate::schema::types::aggregate_state::aggregate_guard_key;
use crate::schema::types::field::FieldKind;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{MutationType, Schema};
use std::collections::BTreeMap;

impl MutationManager {
    /// Reject the entire unsupported request before attribution or clock writes.
    pub(in crate::fold_db_core::mutation_manager) async fn admit_logical_capture(
        &self,
        mutations: &[Mutation],
        storage_prefix: Option<&str>,
    ) -> Result<Option<BTreeSet<String>>, SchemaError> {
        if self.logical_capture_runtime().is_none() {
            return Ok(None);
        }
        if storage_prefix.is_some() {
            return Err(unsupported("database or share namespaces"));
        }
        if mutations.len() > MAX_MUTATIONS {
            return Err(unsupported("more than 128 mutations"));
        }
        let bytes = crate::storage::disk_pack::logical_capture::preflight_outcome_size(mutations)?;
        if bytes > MAX_REQUEST_BYTES {
            return Err(unsupported("requests above 256 KiB"));
        }
        let mut schemas = BTreeMap::new();
        let mut guards = BTreeSet::new();
        let record_home = self.db_ops.atoms().record_home();
        for mutation in mutations {
            require_local_put(mutation, record_home)?;
            if !schemas.contains_key(&mutation.schema_name) {
                let schema = self
                    .schema_manager
                    .get_schema_metadata(&mutation.schema_name)?
                    .ok_or_else(|| unsupported("a missing schema"))?;
                require_plain_schema(&schema)?;
                schemas.insert(mutation.schema_name.clone(), schema);
            }
            let schema = &schemas[&mutation.schema_name];
            let key = self.resolve_mutation_key_value(&mutation.schema_name, schema, mutation)?;
            let hash = key
                .hash
                .as_deref()
                .ok_or_else(|| unsupported("a missing hash"))?;
            guards.insert(aggregate_guard_key(None, &mutation.schema_name, hash));
        }
        self.require_unshared_schemas(&schemas, &guards.into_iter().collect::<Vec<_>>())
            .await?;
        Ok(Some(
            mutations
                .iter()
                .map(|mutation| mutation.uuid.clone())
                .collect(),
        ))
    }

    pub(in crate::fold_db_core::mutation_manager) async fn require_unshared_schemas(
        &self,
        schemas: &BTreeMap<String, Schema>,
        guards: &[String],
    ) -> Result<(), SchemaError> {
        if schemas
            .values()
            .map(|schema| schema.runtime_fields.len())
            .sum::<usize>()
            > MAX_OUTCOMES
        {
            return Err(unsupported("more than 1024 declared fields in one request"));
        }
        let molecules: Vec<String> = schemas
            .values()
            .flat_map(|schema| schema.runtime_fields.values())
            .filter_map(|field| field.common().molecule_uuid().cloned())
            .collect();
        let pointers = molecules
            .iter()
            .map(|molecule| {
                crate::atom::molecule_key_codec::molecule_generation_pointer_key(molecule)
                    .into_bytes()
            })
            .collect();
        if self
            .db_ops
            .open_namespace("main")
            .await?
            .exists_many(pointers)
            .await?
            .into_iter()
            .any(|exists| exists)
        {
            return Err(unsupported("existing molecule generations"));
        }
        if self.db_ops.metadata().contains_any(guards).await? {
            return Err(unsupported("aggregate-enrolled source keys"));
        }
        // This conservative check is bounded. Even an inactive rule needs the
        // complete sharing protocol before this capture slice can admit writes.
        for name in ["share_rules", "share_subscriptions"] {
            let namespace = self.db_ops.open_namespace(name).await?;
            if !namespace.scan_prefix_paged(b"", 1).await?.is_empty() {
                return Err(unsupported("a home with sharing rules or subscriptions"));
            }
        }
        Ok(())
    }
}

/// A format 7 home also captures a local delete and a local purge: the record
/// outcome carries the delete floor, so a peer applies a purge as a delete.
fn require_local_put(mutation: &Mutation, record_home: bool) -> Result<(), SchemaError> {
    let local_kind = match mutation.mutation_type {
        MutationType::Create | MutationType::Update => true,
        MutationType::Delete | MutationType::Purge => record_home,
        _ => false,
    };
    if !local_kind
        || mutation.imported_written_at.is_some()
        || mutation.provenance.is_some()
        || mutation.aggregate_set.is_some()
        || mutation.aggregate_derived_internal
        || mutation.erase_tips_before_nanos.is_some()
        || mutation.imported_version.is_some()
        || !mutation.origin_device.is_empty()
        || mutation.logical_counter != 0
        || !mutation.author_clock_writer_id.is_empty()
        || !mutation.author_clock_signature.is_empty()
        || mutation.author_clock_signature_version != 0
    {
        return Err(unsupported(
            "imports, erasures, aggregate writes, or supplied author clocks",
        ));
    }
    if !blob_refs_of_atom(&serde_json::Value::Null, mutation.metadata.as_ref()).is_empty()
        || mutation
            .fields_and_values
            .values()
            .any(|value| !blob_refs_of_atom(value, mutation.metadata.as_ref()).is_empty())
    {
        return Err(unsupported("file blob references"));
    }
    Ok(())
}

pub(in crate::fold_db_core::mutation_manager) fn require_plain_schema(
    schema: &Schema,
) -> Result<(), SchemaError> {
    let kind = match schema.schema_type {
        DeclarativeSchemaType::Hash => FieldKind::Hash,
        DeclarativeSchemaType::HashRange => FieldKind::HashRange,
        _ => return Err(unsupported("layouts other than Hash and HashRange")),
    };
    if schema.identity_hash.as_ref().is_none_or(String::is_empty) {
        return Err(unsupported("a schema without a canonical identity"));
    }
    for (name, field) in &schema.runtime_fields {
        if name == AGGREGATE_VALID_FIELD
            || name == AGGREGATE_GUARD_TOKEN_FIELD
            || AGGREGATE_MEMBER_RESERVED_FIELDS.contains(&name.as_str())
            || field.kind != kind
            || field.common().storage_prefix().is_some()
            || field.common().molecule_uuid().is_none()
            || !schema.field_hashes.contains_key(name)
        {
            return Err(unsupported("aggregate or noncanonical fields"));
        }
    }
    Ok(())
}
