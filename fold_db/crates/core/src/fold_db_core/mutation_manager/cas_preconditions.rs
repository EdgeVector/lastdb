//! The must-exist update predicate of the precondition gate.

use crate::schema::types::Mutation;

use super::MutationManager;

impl MutationManager {
    /// Is this a user `Update` carrying `must_exist: true`?
    ///
    /// The one predicate the precondition gate keys on, so the guarded set
    /// and the check can never disagree about which mutations are guarded.
    pub(super) fn is_must_exist_update(mutation: &Mutation) -> bool {
        matches!(
            mutation.mutation_type,
            crate::schema::types::operations::MutationType::Update
        ) && mutation.must_exist == Some(true)
    }
}
