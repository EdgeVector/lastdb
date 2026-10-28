//! Deterministic vocabulary checks, shared with conditional persistence.

use crate::types::CanonicalField;
use schema_types::{FoldDbError, FoldDbResult};
use std::collections::BTreeSet;

fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_lowercase)
        .collect()
}

pub(crate) fn validate_description(name: &str, field: &CanonicalField) -> FoldDbResult<()> {
    if field.description.len() > 4096 || words(&field.description).len() < 3 {
        return Err(FoldDbError::Config(format!(
            "invalid_canonical_description: '{name}' requires a specific description of 3 or more words, at most 4096 bytes"
        )));
    }
    Ok(())
}

pub(crate) fn same_description(left: &str, right: &str) -> bool {
    words(left) == words(right)
}

fn overlap(left: &str, right: &str) -> f32 {
    let left: BTreeSet<_> = words(left).into_iter().collect();
    let right: BTreeSet<_> = words(right).into_iter().collect();
    let union = left.union(&right).count();
    if union == 0 {
        return 0.0;
    }
    left.intersection(&right).count() as f32 / union as f32
}

fn different_qualifiers(left: &str, right: &str) -> bool {
    let left = words(left);
    let right = words(right);
    [
        &["created", "updated", "deleted"][..],
        &["start", "end"][..],
        &["source", "target", "destination"][..],
        &["minimum", "maximum"][..],
        &["first", "last"][..],
        &["public", "private"][..],
        &["latitude", "longitude"][..],
        &["seconds", "milliseconds", "microseconds"][..],
        &["bytes", "bits"][..],
    ]
    .iter()
    .any(|qualifiers| {
        let a: BTreeSet<_> = left
            .iter()
            .filter(|word| qualifiers.contains(&word.as_str()))
            .collect();
        let b: BTreeSet<_> = right
            .iter()
            .filter(|word| qualifiers.contains(&word.as_str()))
            .collect();
        !a.is_empty() && !b.is_empty() && a.is_disjoint(&b)
    })
}

/// Vectors confirm a lexical candidate; they cannot erase units or key roles.
pub(crate) fn semantic_candidate(
    name: &str,
    field: &CanonicalField,
    other_name: &str,
    other: &CanonicalField,
) -> bool {
    field.field_type == other.field_type
        && !different_qualifiers(name, other_name)
        && overlap(name, other_name) >= 0.5
        && overlap(&field.description, &other.description) >= 0.35
}

pub(crate) fn reuse_error(name: &str, other_name: &str) -> FoldDbError {
    FoldDbError::Config(format!(
        "canonical_field_reuse_required: '{name}' duplicates '{other_name}'; reuse that concept or describe a distinct subject and property"
    ))
}
