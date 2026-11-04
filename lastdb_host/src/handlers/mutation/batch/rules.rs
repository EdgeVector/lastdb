//! Pure batch rules. No host, schema lookup, identity generation, or write.

use fold_db::schema::types::operations::MutationConvergence;

use crate::HostError;

use super::super::MutationComponents;

/// Reject an empty batch and select the whole batch's convergence policy.
pub(super) fn batch_convergence(
    components: &[MutationComponents],
) -> Result<MutationConvergence, HostError> {
    if components.is_empty() {
        return Err(HostError::new(
            400,
            "Mutation batch must contain at least one mutation",
        ));
    }
    if components
        .iter()
        .any(|component| component.convergence.waits())
    {
        Ok(MutationConvergence::Sync)
    } else {
        Ok(MutationConvergence::Async)
    }
}
