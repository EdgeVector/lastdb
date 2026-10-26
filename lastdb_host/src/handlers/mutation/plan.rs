//! Pure request policy. No host, storage, identity generation, or task access.

use fold_db::schema::types::{KeyValue, MutationType};

use crate::HostError;

use super::MutationComponents;

/// Resolved request data and its validated write policy.
///
/// Schema resolution happens before this plan is built, preserving rejection
/// precedence. Field validation and key reads remain explicit service calls.
pub(super) struct MutationPlan {
    pub components: MutationComponents,
    pub canonical: String,
}

impl MutationPlan {
    pub fn new(components: MutationComponents, canonical: String) -> Result<Self, HostError> {
        reject_illegal_must_exist(components.mutation_type, components.must_exist)?;
        if let Some(prefix) = components.key_range_prefix.as_deref() {
            validate_key_range_prefix(
                prefix,
                components.mutation_type,
                &components.key_value,
                components.aggregate_set.is_some(),
            )?;
        }
        Ok(Self {
            components,
            canonical,
        })
    }
}

/// Request-shape gate for `must_exist`, mirroring
/// `fold_db::schema::types::Mutation::reject_illegal_must_exist` in the core crate.
///
/// `Update` accepts the flag: it is the opt-in that turns today's silent
/// upsert into a loud miss, so a narrow update cannot mint a row that no wide
/// reader can see. `Create` stays refused — it is an upsert by contract, and
/// `expected: Absent` is the create-if-absent precondition.
pub(super) fn reject_illegal_must_exist(
    mutation_type: MutationType,
    must_exist: Option<bool>,
) -> Result<(), HostError> {
    match (mutation_type, must_exist) {
        (MutationType::Create, Some(_)) => Err(HostError::new(
            400,
            "must_exist is only valid on delete or update".to_string(),
        )),
        (MutationType::Purge, Some(false)) => Err(HostError::new(
            400,
            "purge cannot set must_exist: false".to_string(),
        )),
        _ => Ok(()),
    }
}

/// Reject a `key_range_prefix` delete that combines unsupported fields.
fn validate_key_range_prefix(
    prefix: &str,
    mutation_type: MutationType,
    key_value: &KeyValue,
    has_aggregate_set: bool,
) -> Result<(), HostError> {
    if !matches!(mutation_type, MutationType::Delete) {
        return Err(HostError::new(
            400,
            "key_range_prefix is only valid on delete",
        ));
    }
    if prefix.is_empty() {
        return Err(HostError::new(400, "key_range_prefix must not be empty"));
    }
    if key_value.hash.as_deref().is_none_or(str::is_empty) {
        return Err(HostError::new(
            400,
            "key_range_prefix requires key_value.hash",
        ));
    }
    if key_value.range.is_some() {
        return Err(HostError::new(
            400,
            "key_range_prefix cannot be combined with key_value.range",
        ));
    }
    if has_aggregate_set {
        return Err(HostError::new(
            400,
            "key_range_prefix does not support aggregate_set",
        ));
    }
    Ok(())
}
