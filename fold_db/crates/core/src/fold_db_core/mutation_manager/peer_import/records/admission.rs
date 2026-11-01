//! A peer record outcome is admitted under the contract of local record
//! capture: a plain schema with the same identities, a canonical key, typed
//! values without file blob references, and stamps of one device that name
//! mutations of the event.

use super::super::*;
use crate::atom::file_pointer::blob_refs_of_atom;
use crate::schema::types::aggregate_state::aggregate_guard_key;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::logical_capture::AcceptedRecordOutcome;
use lastdb_model::record_value::FieldStamp;
use std::collections::{BTreeSet, HashMap, HashSet};

impl MutationManager {
    /// Validate every record outcome of the page, and return its schemas.
    pub(in crate::fold_db_core::mutation_manager) async fn admit_peer_records(
        &self,
        events: &[CaptureEvent],
    ) -> Result<BTreeMap<String, Schema>, SchemaError> {
        let mut schemas = BTreeMap::new();
        let mut guards = BTreeSet::new();
        let mut declared_fields = 0usize;
        for event in events {
            let mut keys = HashSet::new();
            let mut device = None;
            for outcome in &event.records {
                if !schemas.contains_key(&outcome.schema_name) {
                    let schema = self.peer_record_schema(&outcome.schema_name).await?;
                    declared_fields = declared_fields
                        .checked_add(schema.runtime_fields.len())
                        .filter(|count| {
                            *count <= super::super::super::logical_capture::MAX_OUTCOMES
                        })
                        .ok_or_else(|| unsupported("more than 1024 declared fields in one page"))?;
                    schemas.insert(outcome.schema_name.clone(), schema);
                }
                let schema = &schemas[&outcome.schema_name];
                self.validate_peer_record(schema, outcome)?;
                for stamp in outcome.stamps() {
                    validate_stamp(stamp, event, &mut device)?;
                }
                if !keys.insert((&outcome.schema_name, &outcome.key)) {
                    return Err(unsupported("repeated records in one event"));
                }
                guards.insert(aggregate_guard_key(
                    None,
                    &outcome.schema_name,
                    outcome.key.hash.as_deref().expect("validated hash"),
                ));
            }
        }
        self.require_unshared_schemas(&schemas, &guards.into_iter().collect::<Vec<_>>())
            .await?;
        Ok(schemas)
    }

    async fn peer_record_schema(&self, name: &str) -> Result<Schema, SchemaError> {
        let schema = self.load_schema_for_delta(name, None).await?;
        super::super::super::logical_capture::require_plain_schema(&schema)?;
        Ok(schema)
    }

    fn validate_peer_record(
        &self,
        schema: &Schema,
        outcome: &AcceptedRecordOutcome,
    ) -> Result<(), SchemaError> {
        let revision = schema.alternate_key_revision();
        if outcome.alternate_key_revision.as_deref() != Some(revision.as_str())
            || schema.identity_hash.as_deref() != Some(outcome.schema_identity.as_str())
        {
            return Err(unsupported(
                "a schema identity or alternate revision mismatch",
            ));
        }
        let valid_shape = match schema.schema_type {
            DeclarativeSchemaType::Hash => {
                outcome.key.hash.is_some() && outcome.key.range.is_none()
            }
            DeclarativeSchemaType::HashRange => {
                outcome.key.hash.is_some() && outcome.key.range.is_some()
            }
            _ => false,
        };
        if !valid_shape
            || self
                .db_ops
                .molecule_keys()
                .canonical_key(schema, outcome.key.clone())?
                != outcome.key
        {
            return Err(unsupported("noncanonical or missing key coordinates"));
        }
        if outcome.fields.is_empty() && outcome.meta.is_none() && outcome.deleted.is_none() {
            return Err(unsupported("an empty record outcome"));
        }
        for (name, field) in &outcome.fields {
            if !schema.runtime_fields.contains_key(name)
                || schema.field_hashes.get(name) != Some(&field.identity)
            {
                return Err(unsupported("an unknown field or a field identity mismatch"));
            }
            let Some(value) = &field.value else { continue };
            crate::atom::enforce_atom_content_limit(&outcome.schema_name, value)?;
            if !blob_refs_of_atom(value, None).is_empty() {
                return Err(unsupported("file blob references"));
            }
            schema
                .get_field_type(name)
                .validate(value)
                .map_err(|_| unsupported("a field value outside its declared type"))?;
            self.validate_record_key_field(schema, &outcome.key, name, value)?;
        }
        if let Some(meta) = &outcome.meta {
            let metadata: HashMap<String, String> = meta.metadata.clone().into_iter().collect();
            if !blob_refs_of_atom(&serde_json::Value::Null, Some(&metadata)).is_empty() {
                return Err(unsupported("file blob references"));
            }
        }
        Ok(())
    }

    /// A key field value names the key coordinate that it materialized.
    fn validate_record_key_field(
        &self,
        schema: &Schema,
        key: &KeyValue,
        field: &str,
        value: &serde_json::Value,
    ) -> Result<(), SchemaError> {
        let Some(declared) = &schema.key else {
            return Ok(());
        };
        let coordinate = if declared.hash_field.as_deref() == Some(field) {
            scalar_key_coordinate(value).map(std::borrow::Cow::into_owned)
        } else if declared.range_field.as_deref() == Some(field) {
            let range = scalar_key_coordinate(value)
                .ok_or_else(|| unsupported("a nonscalar key field value"))?;
            self.db_ops
                .molecule_keys()
                .canonical_key(
                    schema,
                    KeyValue::new(key.hash.clone(), Some(range.into_owned())),
                )?
                .range
        } else {
            return Ok(());
        };
        let expected = if declared.hash_field.as_deref() == Some(field) {
            key.hash.as_deref()
        } else {
            key.range.as_deref()
        };
        if coordinate.as_deref() != expected {
            return Err(unsupported(
                "a key field value that differs from its coordinates",
            ));
        }
        Ok(())
    }
}

/// The local author contract of a stamp. The stamps of one event share the
/// device of its author.
fn validate_stamp<'a>(
    stamp: &'a FieldStamp,
    event: &CaptureEvent,
    device: &mut Option<&'a str>,
) -> Result<(), SchemaError> {
    if stamp.written_at == 0
        || stamp.logical_counter == 0
        || stamp.device_id.is_empty()
        || !event.mutation_ids.contains(&stamp.mutation_id)
        || device.is_some_and(|device| device != stamp.device_id)
    {
        return Err(unsupported(
            "a stamp outside the captured local author contract",
        ));
    }
    *device = Some(stamp.device_id.as_str());
    Ok(())
}
