//! Pure request policy. No host, storage, identity generation, or task access.

use fold_db::fold_db_core::mutation_manager::CloudCapturePolicy;
use fold_db::schema::types::{KeyValue, MutationType};

use crate::HostError;

use super::cloud::{cloud_capture_policy, reject_illegal_must_exist};
use super::MutationComponents;

/// Resolved request data and its validated write policy.
///
/// Schema resolution happens before this plan is built, preserving rejection
/// precedence. Field validation and key reads remain explicit service calls.
pub(super) struct MutationPlan {
    pub components: MutationComponents,
    pub canonical: String,
    pub cloud_policy: CloudCapturePolicy,
}

impl MutationPlan {
    pub fn new(components: MutationComponents, canonical: String) -> Result<Self, HostError> {
        reject_illegal_must_exist(components.mutation_type, components.must_exist)?;
        let cloud_policy = cloud_capture_policy(
            components.mutation_type,
            components.durability,
            components.cloud_publication,
            components.must_exist,
        )?;
        if let Some(prefix) = components.key_range_prefix.as_deref() {
            validate_key_range_prefix(
                prefix,
                components.mutation_type,
                &components.key_value,
                components.cloud_publication.is_some(),
                components.aggregate_set.is_some(),
            )?;
        }
        Ok(Self {
            components,
            canonical,
            cloud_policy,
        })
    }
}

/// Reject a `key_range_prefix` delete that combines unsupported fields.
fn validate_key_range_prefix(
    prefix: &str,
    mutation_type: MutationType,
    key_value: &KeyValue,
    has_cloud_publication: bool,
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
    if has_cloud_publication {
        return Err(HostError::new(
            400,
            "key_range_prefix does not support cloud_publication",
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
