//! One name rule and vocabulary matching helpers for canonical fields and schema advice.

use crate::CanonicalField;
use schema_types::{FoldDbError, FoldDbResult};
use std::collections::{BTreeSet, HashMap};

/// Validation rule version; excluded from field identity hashes.
pub const NAME_POLICY_VERSION: u32 = 1;

const APP_ABBREVIATIONS: &[&str] = &[
    "bcl", "bes", "bfc", "bge", "bll", "bln", "blp", "btd", "bts", "clu", "ctd", "eph", "ie",
    "klh", "liv", "nh", "prk", "psi", "rle", "tmb",
];

const SHORT_WORDS: &[&str] = &[
    "at", "by", "in", "of", "on", "or", "to", "id", "ip", "cm", "kg", "km", "mm", "ms", "ns", "ph",
    "us",
];

/// Syntax gate for canonical field names.
pub fn validate_new_field_name(name: &str) -> Result<(), String> {
    let words: Vec<_> = name.split('_').collect();
    let reason = if name.len() > 96 || name.is_empty() {
        Some("use 1 through 96 characters")
    } else if words.iter().any(|word| {
        word.is_empty()
            || !word.as_bytes()[0].is_ascii_lowercase()
            || !word
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    }) {
        Some("use lowercase words separated by single underscores")
    } else if words
        .iter()
        .filter(|word| {
            !matches!(
                **word,
                "of" | "to" | "by" | "in" | "at" | "for" | "and" | "the"
            )
        })
        .count()
        < 2
    {
        Some("name both the subject and its property")
    } else if words.iter().any(|word| word.len() == 1) {
        Some("spell out each concept; a one-letter key role is not a concept")
    } else if words
        .iter()
        .any(|word| word.len() == 2 && !SHORT_WORDS.contains(word))
    {
        Some("spell out short names; use standard key terms, connectives or units")
    } else if words.iter().enumerate().any(|(index, word)| {
        APP_ABBREVIATIONS.contains(word)
            && !(*word == "psi"
                && index > 0
                && words[index - 1] == "pressure"
                && index + 1 == words.len())
    }) {
        Some("spell out app abbreviations")
    } else if words
        .iter()
        .any(|word| matches!(*word, "generic" | "entity" | "item"))
    {
        Some("replace the generic subject with the actual concept")
    } else {
        None
    };
    if let Some(reason) = reason {
        Err(format!(
            "invalid_canonical_name: '{name}': {reason}; for example 'file_size_bytes'"
        ))
    } else {
        Ok(())
    }
}

/// Validate against existing definitions and earlier entries in the batch.
pub fn canonical_fields_after_insert(
    existing: &HashMap<String, CanonicalField>,
    fields: &[(String, CanonicalField)],
) -> FoldDbResult<HashMap<String, CanonicalField>> {
    let mut candidate = existing.clone();
    for (name, field) in fields {
        validate_insert(&candidate, name, field)?;
        candidate.insert(name.clone(), field.clone());
    }
    Ok(candidate)
}

fn validate_insert(
    existing: &HashMap<String, CanonicalField>,
    name: &str,
    field: &CanonicalField,
) -> FoldDbResult<()> {
    validate_new_field_name(name).map_err(FoldDbError::Config)?;
    if field.name_policy_version != NAME_POLICY_VERSION {
        return Err(FoldDbError::Config(format!(
            "invalid_canonical_policy: '{name}' requires name policy {NAME_POLICY_VERSION}"
        )));
    }
    if field.description.trim().is_empty() || field.version == 0 {
        return Err(FoldDbError::Config(format!(
            "canonical_field_definition_conflict: '{name}' requires a description and a positive identity version"
        )));
    }
    if field.description.len() > 4096 || words(&field.description).len() < 3 {
        return Err(FoldDbError::Config(format!(
            "invalid_canonical_description: '{name}' requires a specific description of 3 or more words, at most 4096 bytes"
        )));
    }
    if existing.get(name).is_some_and(|stored| stored != field) {
        return Err(FoldDbError::Config(format!(
            "canonical_field_conflict: '{name}' already has another definition; reset it explicitly before replacement"
        )));
    }
    for (other_name, other) in existing {
        if other_name != name && same_description(&other.description, &field.description) {
            return Err(FoldDbError::Config(format!(
                "canonical_field_reuse_required: '{name}' duplicates '{other_name}'; reuse that concept or describe a distinct subject and property"
            )));
        }
    }
    Ok(())
}

fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(str::to_lowercase)
        .collect()
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

/// Name overlap of a canonical name close enough to suggest in advice.
pub(crate) fn name_overlap(name: &str, other_name: &str) -> Option<f32> {
    if name == other_name || different_qualifiers(name, other_name) {
        return None;
    }
    let score = overlap(name, other_name);
    (score >= 0.5).then_some(score)
}
