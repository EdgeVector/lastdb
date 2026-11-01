//! Record capture: the staged records of one request and their certified
//! outcomes (event version 2).

use super::collector::with_record_candidates;
use super::*;
use crate::db_operations::record_store::RecordChange;
use crate::schema::types::{KeyValue, Schema};
use crate::storage::disk_pack::logical_capture::{
    preflight_outcome_size, AcceptedRecordOutcome, RecordFieldOutcome,
};
use lastdb_model::record_value::{FieldStamp, RecordMeta, StoredRecord};
use std::collections::BTreeMap;

/// The record that the request staged last at each record storage key.
pub(in crate::fold_db_core::mutation_manager) type RecordCandidates =
    BTreeMap<String, RecordCandidate>;

pub(in crate::fold_db_core::mutation_manager) struct RecordCandidate {
    schema_name: String,
    schema_identity: String,
    alternate_key_revision: String,
    /// The canonical identity of each stamped field of `record`.
    field_identities: BTreeMap<String, String>,
    key: KeyValue,
    record: StoredRecord,
}

impl MutationManager {
    /// Keep the staged value of each changed record. A later group of the same
    /// request replaces it, so the request certifies its final records.
    pub(in crate::fold_db_core::mutation_manager) fn collect_logical_records(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        changes: &[RecordChange],
    ) -> Result<(), SchemaError> {
        let identity = schema
            .identity_hash
            .as_deref()
            .filter(|identity| !identity.is_empty())
            .ok_or_else(|| unsupported("a missing schema identity"))?;
        let revision = schema.alternate_key_revision();
        with_record_candidates(|candidates| {
            for (key, change) in keys.iter().zip(changes) {
                let field_identities = change
                    .after
                    .stamps
                    .keys()
                    .map(|field| {
                        schema
                            .field_hashes
                            .get(field)
                            .map(|identity| (field.clone(), identity.clone()))
                            .ok_or_else(|| unsupported("a field without a canonical identity"))
                    })
                    .collect::<Result<_, _>>()?;
                candidates.insert(
                    change.key.clone(),
                    RecordCandidate {
                        schema_name: schema.name.clone(),
                        schema_identity: identity.to_owned(),
                        alternate_key_revision: revision.clone(),
                        field_identities,
                        key: key.clone(),
                        record: change.after.clone(),
                    },
                );
            }
            Ok(())
        })
    }
}

/// The outcome of each staged record: the parts whose stamp names a mutation
/// of this request. A part that a newer stamp kept names another mutation and
/// stays out, so the event carries only the effects that this request made.
pub(super) fn record_outcomes(
    candidates: RecordCandidates,
    mutation_ids: &[String],
) -> Result<Vec<AcceptedRecordOutcome>, SchemaError> {
    let own = |stamp: &FieldStamp| mutation_ids.contains(&stamp.mutation_id);
    let mut outcomes = Vec::with_capacity(candidates.len());
    let (mut bytes, mut field_count) = (0usize, 0usize);
    for candidate in candidates.into_values() {
        let RecordCandidate {
            schema_name,
            schema_identity,
            alternate_key_revision,
            mut field_identities,
            key,
            record,
        } = candidate;
        let StoredRecord {
            mut fields,
            stamps,
            meta,
            deleted,
            ..
        } = record;
        let fields = stamps
            .into_iter()
            .filter(|(_, stamp)| own(stamp))
            .map(|(field, stamp)| {
                let outcome = RecordFieldOutcome {
                    identity: field_identities.remove(&field).unwrap_or_default(),
                    value: fields.remove(&field),
                    stamp,
                };
                (field, outcome)
            })
            .collect::<BTreeMap<_, _>>();
        let outcome = AcceptedRecordOutcome {
            schema_name,
            schema_identity,
            alternate_key_revision: Some(alternate_key_revision),
            key,
            fields,
            // A file blob key names a blob of this device; a peer refuses it.
            meta: meta
                .filter(|meta| own(&meta.stamp))
                .map(RecordMeta::without_file_refs),
            deleted: deleted.filter(|stamp| own(stamp)),
        };
        if outcome.fields.is_empty() && outcome.meta.is_none() && outcome.deleted.is_none() {
            continue;
        }
        field_count += outcome.fields.len();
        if field_count > MAX_OUTCOMES {
            return Err(unsupported("more than 1024 final fields"));
        }
        bytes = bytes.saturating_add(preflight_outcome_size(&outcome)?);
        if bytes > 1024 * 1024 {
            return Err(unsupported("final record outcomes above 1 MiB"));
        }
        outcomes.push(outcome);
    }
    Ok(outcomes)
}
