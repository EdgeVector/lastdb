//! One precondition covers every column of the record.
use crate::schema::types::{cas::CasExpectation, Mutation, MutationType};
use crate::schema::SchemaError;
use lastdb_model::record_value::StoredRecord;

pub(super) fn check_expectation(
    mutation: &Mutation,
    record: Option<&StoredRecord>,
) -> Result<(), SchemaError> {
    let current =
        record.filter(|record| record.is_live() || mutation.mutation_type == MutationType::Purge);
    let actual = current.map(|record| record.stamp.revision());
    let matches = match &mutation.expected {
        CasExpectation::Absent => !record.is_some_and(StoredRecord::is_live),
        CasExpectation::Revision { revision } => actual.as_ref() == Some(revision),
    };
    if !matches {
        return Err(SchemaError::CasConflict {
            schema: mutation.schema_name.clone(),
            key: mutation.key_value.to_storage_key(),
            expected: match &mutation.expected {
                CasExpectation::Absent => "<absent>".into(),
                CasExpectation::Revision { revision } => revision.clone(),
            },
            actual,
        });
    }
    if matches!(
        mutation.mutation_type,
        MutationType::Delete | MutationType::Purge
    ) && current
        .filter(|record| record.is_live())
        .is_some_and(|record| record.alternates != mutation.alternates)
    {
        let message = "deletion must supply the record's complete current alternate keys";
        // A contributor submission is already durable in the inbox. A wrong
        // alternate set cannot succeed on retry, so record it as a reject.
        if mutation.contribution.is_some() {
            return Err(SchemaError::InvalidField(message.into()));
        }
        return Err(SchemaError::InvalidData(message.into()));
    }
    if mutation.erase_written_before_nanos.is_some_and(|cutoff| {
        record
            .and_then(StoredRecord::written_at)
            .is_some_and(|written| written >= cutoff)
    }) {
        return Err(SchemaError::InvalidData(
            "record is newer than the erasure cutoff".into(),
        ));
    }
    Ok(())
}

/// Replace and delete from a verified contributor may commit only when the live
/// row was stamped with that writer's grant digest. Absence stays a CAS conflict.
pub(super) fn require_contribution_author(
    mutation: &Mutation,
    record: Option<&StoredRecord>,
) -> Result<(), SchemaError> {
    if !matches!(
        mutation.mutation_type,
        MutationType::Replace | MutationType::Delete
    ) {
        return Ok(());
    }
    let Some(writer) = mutation
        .contribution
        .as_ref()
        .map(|admission| admission.writer.as_str())
    else {
        return Ok(());
    };
    let Some(live) = record.filter(|record| record.is_live()) else {
        return Ok(());
    };
    let author = live
        .meta
        .as_ref()
        .and_then(|meta| meta.metadata.get("share_author"))
        .map(String::as_str);
    if author == Some(writer) {
        return Ok(());
    }
    Err(SchemaError::InvalidField(
        "share contribution can change only a record its writer created".into(),
    ))
}
