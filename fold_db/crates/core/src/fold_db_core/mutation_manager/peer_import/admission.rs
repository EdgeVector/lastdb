//! The first peer slice accepts only the same plain facts as local capture.

use super::*;
use crate::atom::file_pointer::blob_refs_of_atom;
use crate::atom::Atom;
use crate::atom::AtomClock;
use crate::schema::types::aggregate_state::aggregate_guard_key;
use crate::schema::types::key_value::scalar_key_coordinate;
use crate::schema::types::schema::DeclarativeSchemaType;
use crate::schema::types::Schema;
use std::collections::{BTreeSet, HashSet};

impl MutationManager {
    pub(super) async fn require_peer_replay_floor(
        &self,
        events: &[CaptureEvent],
    ) -> Result<(), SchemaError> {
        if events.iter().all(|event| event.outcomes.is_empty()) {
            return Ok(());
        }
        let cutoff_nanos = self.db_ops.atoms().minimum_replay_time().await?;
        if cutoff_nanos > 0
            && events
                .iter()
                .flat_map(|event| &event.outcomes)
                .any(|outcome| outcome.tip.written_at <= cutoff_nanos)
        {
            return Err(SchemaError::ReplayBeforeDeleteCutoff { cutoff_nanos });
        }
        Ok(())
    }

    pub(super) async fn admit_peer_outcomes(
        &self,
        events: &[CaptureEvent],
    ) -> Result<(), SchemaError> {
        let mut schemas = BTreeMap::new();
        let mut guards = BTreeSet::new();
        let mut declared_fields = 0usize;
        for event in events {
            let mut slots = HashSet::new();
            for outcome in &event.outcomes {
                if !schemas.contains_key(&outcome.schema_name) {
                    let schema = self
                        .load_schema_for_delta(&outcome.schema_name, None)
                        .await?;
                    declared_fields = declared_fields
                        .checked_add(schema.runtime_fields.len())
                        .filter(|count| *count <= super::super::logical_capture::MAX_OUTCOMES)
                        .ok_or_else(|| unsupported("more than 1024 declared fields in one page"))?;
                    super::super::logical_capture::require_plain_schema(&schema)?;
                    schemas.insert(outcome.schema_name.clone(), schema);
                }
                let schema = &schemas[&outcome.schema_name];
                self.validate_peer_outcome(schema, outcome, event)?;
                if !slots.insert((&outcome.molecule_uuid, &outcome.key)) {
                    return Err(unsupported("repeated field slots in one event"));
                }
                guards.insert(aggregate_guard_key(
                    None,
                    &outcome.schema_name,
                    outcome.key.hash.as_deref().expect("validated hash"),
                ));
            }
        }
        self.require_unshared_schemas(&schemas, &guards.into_iter().collect::<Vec<_>>())
            .await
    }

    fn validate_peer_outcome(
        &self,
        schema: &Schema,
        outcome: &AcceptedFieldOutcome,
        event: &CaptureEvent,
    ) -> Result<(), SchemaError> {
        let field = schema
            .runtime_fields
            .get(&outcome.field_name)
            .ok_or_else(|| unsupported("an unknown canonical field"))?;
        let revision = schema
            .uses_independent_fields()
            .then(|| schema.alternate_key_revision());
        if outcome.alternate_key_revision.as_deref() != revision.as_deref() {
            return Err(unsupported("an alternate declaration revision mismatch"));
        }
        if outcome.field_identity.is_empty()
            || outcome.molecule_uuid.is_empty()
            || schema.identity_hash.as_deref() != Some(outcome.schema_identity.as_str())
            || schema.field_hashes.get(&outcome.field_name) != Some(&outcome.field_identity)
            || field.common().molecule_uuid() != Some(&outcome.molecule_uuid)
        {
            return Err(unsupported(
                "a schema, field, or molecule identity mismatch",
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
        validate_atom(outcome)?;
        self.validate_peer_key_field(schema, outcome)?;
        schema
            .get_field_type(&outcome.field_name)
            .validate(outcome.atom.content())
            .map_err(|_| unsupported("a field value outside its declared type"))?;
        let tip = &outcome.tip;
        if tip.written_at == 0
            || tip.logical_counter == 0
            || tip.device_id.is_empty()
            || !event.mutation_ids.contains(&tip.mutation_uuid)
            || !tip.writer_pubkey().is_empty()
            || !tip.signature().is_empty()
            || tip.signature_version() != 0
            || tip.provenance().is_some()
            || !tip.prev_tip_id().is_empty()
        {
            return Err(unsupported(
                "a tip outside the captured local author contract",
            ));
        }
        Ok(())
    }

    fn validate_peer_key_field(
        &self,
        schema: &Schema,
        outcome: &AcceptedFieldOutcome,
    ) -> Result<(), SchemaError> {
        let Some(key) = &schema.key else {
            return Ok(());
        };
        for (field, coordinate, is_range) in [
            (
                key.hash_field.as_deref(),
                outcome.key.hash.as_deref(),
                false,
            ),
            (
                key.range_field.as_deref(),
                outcome.key.range.as_deref(),
                true,
            ),
        ] {
            if field != Some(outcome.field_name.as_str()) {
                continue;
            }
            let value = scalar_key_coordinate(outcome.atom.content())
                .ok_or_else(|| unsupported("a nonscalar key-field atom"))?;
            let value = if is_range {
                self.db_ops
                    .molecule_keys()
                    .canonical_key(
                        schema,
                        crate::schema::types::KeyValue::new(
                            outcome.key.hash.clone(),
                            Some(value.into_owned()),
                        ),
                    )?
                    .range
            } else {
                Some(value.into_owned())
            };
            if value.as_deref() != coordinate {
                return Err(unsupported(
                    "a key-field atom that differs from its coordinates",
                ));
            }
        }
        Ok(())
    }
}

fn validate_atom(outcome: &AcceptedFieldOutcome) -> Result<(), SchemaError> {
    let atom = &outcome.atom;
    crate::atom::enforce_atom_content_limit(&outcome.schema_name, atom.content())?;
    if atom.source_schema_name() != outcome.schema_name
        || Atom::new(outcome.schema_name.clone(), atom.content().clone()).uuid() != atom.uuid()
        || outcome.tip.atom_uuid != atom.uuid()
    {
        return Err(unsupported("an atom content identity mismatch"));
    }
    if atom.is_tombstone()
        || outcome
            .metadata
            .as_ref()
            .is_some_and(|meta| meta.tombstoned)
        || !blob_refs_of_atom(atom.content(), atom.metadata()).is_empty()
        || outcome.metadata.as_ref().is_some_and(|metadata| {
            !blob_refs_of_atom(atom.content(), metadata.metadata.as_ref()).is_empty()
        })
    {
        return Err(unsupported("tombstones or file blob references"));
    }
    Ok(())
}
