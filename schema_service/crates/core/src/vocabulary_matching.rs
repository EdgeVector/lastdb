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

pub(crate) fn name_has_definition_support(name: &str, description: &str) -> bool {
    let description_words = words(description);
    let description: Vec<_> = description_words
        .iter()
        .map(|word| meaning_word(word))
        .collect();
    words(name).iter().any(|raw_word| {
        let word = meaning_word(raw_word);
        word.len() > 2
            && !matches!(
                word,
                "field"
                    | "value"
                    | "data"
                    | "json"
                    | "text"
                    | "string"
                    | "integer"
                    | "number"
                    | "object"
                    | "array"
                    | "type"
            )
            && (description.contains(&word) || acronym_matches(raw_word, &description_words))
    })
}

/// Meaning checks accept ordinary plurals and common equivalent terms. Keep
/// identity/duplicate normalization and unit/role separation unchanged.
fn meaning_word(word: &str) -> &str {
    let singular = if word.len() > 3 && !word.ends_with("ss") && !word.ends_with("us") {
        word.strip_suffix('s').unwrap_or(word)
    } else {
        word
    };
    match singular {
        "telephone" => "phone",
        "photograph" => "photo",
        "timestamp" | "datetime" | "duration" | "second" | "minute" | "hour" => "time",
        _ => singular,
    }
}

fn acronym_matches(word: &str, description: &[String]) -> bool {
    (3..=5).contains(&word.len())
        && word.bytes().all(|byte| byte.is_ascii_lowercase())
        && description.windows(word.len()).any(|phrase| {
            phrase
                .iter()
                .filter_map(|part| part.chars().next())
                .collect::<String>()
                == word
        })
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
