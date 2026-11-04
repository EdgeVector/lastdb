//! Pure request shape, independent of storage or identity generation.
use super::MutationComponents;
use crate::HostError;

pub(super) struct MutationPlan {
    pub components: MutationComponents,
    pub canonical: String,
}
impl MutationPlan {
    pub fn new(components: MutationComponents, canonical: String) -> Result<Self, HostError> {
        validate_identity(&components)?;
        Ok(Self {
            components,
            canonical,
        })
    }
}
pub(super) fn validate_identity(components: &MutationComponents) -> Result<(), HostError> {
    uuid::Uuid::parse_str(&components.mutation_id).map_err(|_| {
        HostError::new(
            400,
            "mutation_id must be a stable UUID for this complete request",
        )
    })?;
    Ok(())
}
