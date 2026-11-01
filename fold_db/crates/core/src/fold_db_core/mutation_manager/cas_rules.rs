//! Storage-free decisions for conditional writes.

use crate::schema::types::{cas::CasExpectation, Mutation};
use crate::schema::SchemaError;

/// A selected field's live value and the mutation that wrote it. A removed
/// field is absent.
#[derive(Default)]
pub(super) struct CurrentHead {
    pub(super) mutation_uuid: Option<String>,
    pub(super) value: Option<serde_json::Value>,
}

pub(super) fn check_expectation(
    mutation: &Mutation,
    expected: &CasExpectation,
    head: CurrentHead,
) -> Result<(), SchemaError> {
    let conflict = |exp: String, actual: Option<String>| SchemaError::CasConflict {
        schema: mutation.schema_name.clone(),
        field: expected.field().to_string(),
        key: mutation.key_value.to_storage_key(),
        expected: exp,
        actual,
    };
    match expected {
        CasExpectation::Mutation { uuid, .. } => match head.mutation_uuid {
            Some(current) if &current == uuid => Ok(()),
            current => Err(conflict(uuid.clone(), current)),
        },
        CasExpectation::Absent { .. } => {
            if let Some(value) = head.value {
                return Err(conflict("<absent>".to_string(), Some(value.to_string())));
            }
            Ok(())
        }
        CasExpectation::Value { value, .. } => match head.value {
            Some(current) if &current == value => Ok(()),
            Some(current) => Err(conflict(value.to_string(), Some(current.to_string()))),
            None => Err(conflict(value.to_string(), None)),
        },
    }
}
