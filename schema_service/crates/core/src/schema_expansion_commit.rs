//! Validate an expansion pair inside the same conditional definition commit.

use schema_types::{FoldDbError, FoldDbResult, Schema};
use std::collections::HashMap;

/// A predecessor and its successor in one batch are an expansion. Compare the
/// predecessor's complete durable definition before changing its marker, so
/// two independent instances cannot silently branch the same predecessor.
/// Replaying the same pair after a lost reply is an idempotent success.
pub fn validate_expansion_pairs(
    existing: &HashMap<String, Schema>,
    incoming: &[Schema],
) -> FoldDbResult<()> {
    for predecessor in incoming {
        let Some(target) = predecessor.superseded_by.as_deref() else {
            continue;
        };
        // Self-markers are deliberate deprecation, not an expansion pair.
        if target == predecessor.name {
            continue;
        }
        let Some(successor) = incoming.iter().find(|schema| schema.name == target) else {
            continue;
        };
        let conflict = || {
            FoldDbError::Config(format!(
                "schema_expansion_write_conflict: predecessor or successor of '{}' changed; refresh the schema before retry",
                predecessor.name
            ))
        };
        // A validated snapshot can introduce a complete chain into a fresh
        // registry. Only an existing predecessor has a revision to compare.
        let Some(stored) = existing.get(&predecessor.name) else {
            continue;
        };
        if stored
            .superseded_by
            .as_deref()
            .is_some_and(|current| current != target)
        {
            return Err(conflict());
        }
        let mut expected = stored.clone();
        expected.superseded_by = Some(target.to_owned());
        if !same_definition(&expected, predecessor) {
            return Err(conflict());
        }
        // A retry must not erase a later expansion of the original successor.
        if existing
            .get(target)
            .is_some_and(|stored| !same_definition(stored, successor))
        {
            return Err(conflict());
        }
    }
    Ok(())
}

fn same_definition(left: &Schema, right: &Schema) -> bool {
    left == right && left.identity_hash_algo_version == right.identity_hash_algo_version
}
