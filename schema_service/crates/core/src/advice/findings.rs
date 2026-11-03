//! The findings for one draft: per field, then the similar schemas.

use super::{draft, SchemaAdviceRequest, SchemaAdviceResponse};
use super::{finding, AdviceEvidence, AdviceFinding, AdviceKind, Draft, SchemaAdviceReply};
use crate::field_name_policy::validate_new_field_name;
use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use crate::vocabulary_matching::{name_overlap, same_description};
use schema_types::{DataClassification, FieldValueType, FoldDbResult, Schema};
use std::collections::{BTreeSet, HashMap};

const MAX_NEAR_MATCHES: usize = 3;
const MAX_SIMILAR_SCHEMAS: usize = 3;
/// Share of the draft's fields that another app's schema must carry before
/// advice names it.
const SIMILAR_SCHEMA_COVERAGE: f64 = 0.6;

/// How many catalog schemas carry each field hash.
fn field_hash_use(schemas: &HashMap<String, Schema>) -> HashMap<&str, usize> {
    let mut used = HashMap::new();
    for schema in schemas.values() {
        for hash in schema.field_hashes.values() {
            *used.entry(hash.as_str()).or_insert(0) += 1;
        }
    }
    used
}

/// The finding for a field whose name is in the vocabulary.
fn canonical_finding(
    name: &str,
    canonical: &CanonicalField,
    draft: &Draft,
    used: &HashMap<&str, usize>,
) -> AdviceFinding {
    let hash = canonical.field_hash(name);
    let mut differences = Vec::new();
    if let Some(kind) = draft
        .types
        .get(name)
        .filter(|kind| **kind != canonical.field_type)
    {
        differences.push(format!(
            "here it is {kind}; the canonical `{name}` is {}",
            canonical.field_type
        ));
    }
    if let Some(text) = draft
        .descriptions
        .get(name)
        .filter(|text| !same_description(text, &canonical.description))
    {
        differences.push(format!(
            "the description \"{text}\" differs from the canonical \"{}\"",
            canonical.description
        ));
    }
    let version = draft.versions.get(name).copied().unwrap_or(1);
    if version != canonical.version {
        differences.push(format!(
            "version {version} differs from the canonical version {}",
            canonical.version
        ));
    }
    let evidence = AdviceEvidence {
        suggested_type: (draft.types.get(name) != Some(&canonical.field_type))
            .then(|| canonical.field_type.clone()),
        canonical_field_hash: Some(hash),
        ..AdviceEvidence::default()
    };
    if !differences.is_empty() {
        let message = format!(
            "`{name}` means something else here: {}. Rename the field, or use the canonical definition.",
            differences.join("; ")
        );
        return finding(
            AdviceKind::SameNameDifferentMeaning,
            name,
            message,
            evidence,
        );
    }
    let count = evidence
        .canonical_field_hash
        .as_deref()
        .and_then(|hash| used.get(hash))
        .copied()
        .unwrap_or(0);
    let exact = draft.types.contains_key(name) && draft.descriptions.contains_key(name);
    let message = if exact {
        format!("`{name}` matches the canonical field used by {count} schemas.")
    } else {
        format!(
            "`{name}` is the canonical field used by {count} schemas; give it the canonical type and description \"{}\" to match it exactly.",
            canonical.description
        )
    };
    finding(AdviceKind::CanonicalMatch, name, message, evidence)
}

/// Canonical names close to `name`, best first. A known draft type must
/// equal the canonical type.
fn near_names<'a>(
    name: &str,
    kind: Option<&FieldValueType>,
    vocabulary: &'a HashMap<String, CanonicalField>,
) -> Vec<(&'a str, &'a CanonicalField)> {
    let mut near: Vec<(f32, &str, &CanonicalField)> = vocabulary
        .iter()
        .filter(|(_, field)| kind.is_none_or(|kind| *kind == field.field_type))
        .filter_map(|(other, field)| {
            name_overlap(name, other).map(|score| (score, other.as_str(), field))
        })
        .collect();
    near.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    near.into_iter()
        .take(MAX_NEAR_MATCHES)
        .map(|(_, other, field)| (other, field))
        .collect()
}

fn classification_label(label: &DataClassification) -> String {
    format!("{} ({})", label.sensitivity_name(), label.data_domain)
}

/// Schemas of other apps that carry most of the draft's fields, best first.
fn similar_schemas(
    draft: &Draft,
    app_id: Option<&str>,
    schemas: &HashMap<String, Schema>,
) -> Vec<AdviceFinding> {
    let wanted: BTreeSet<&str> = draft.fields.iter().map(String::as_str).collect();
    if wanted.len() < 2 {
        return Vec::new();
    }
    let mut similar: Vec<(usize, &Schema, &str)> = schemas
        .values()
        .filter(|schema| schema.superseded_by.is_none())
        .filter_map(|schema| {
            let owner = schema.owner_app_id.as_deref()?;
            if app_id == Some(owner) {
                return None;
            }
            let shared = schema
                .fields
                .iter()
                .flatten()
                .filter(|field| wanted.contains(field.as_str()))
                .count();
            let coverage = shared as f64 / wanted.len() as f64;
            (shared >= 2 && coverage >= SIMILAR_SCHEMA_COVERAGE).then_some((shared, schema, owner))
        })
        .collect();
    similar.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.name.cmp(&b.1.name)));
    similar
        .into_iter()
        .take(MAX_SIMILAR_SCHEMAS)
        .map(|(shared, schema, owner)| {
            let label = schema.descriptive_name.as_deref().unwrap_or(&schema.name);
            AdviceFinding {
                kind: AdviceKind::SimilarSchema,
                field: None,
                message: format!(
                    "`{label}` from app `{owner}` has {shared} of these {} fields.",
                    wanted.len()
                ),
                evidence: AdviceEvidence {
                    schema_identity: schema.identity_hash.clone(),
                    app_id: Some(owner.to_string()),
                    ..AdviceEvidence::default()
                },
            }
        })
        .collect()
}

impl SchemaServiceState {
    /// Findings for one draft, in field order, then the similar schemas.
    ///
    /// # Errors
    /// A draft advice cannot read, or a poisoned lock.
    pub fn schema_advice(
        &self,
        request: &SchemaAdviceRequest,
    ) -> FoldDbResult<Result<SchemaAdviceResponse, String>> {
        let draft = match draft(&request.schema) {
            Ok(draft) => draft,
            Err(message) => return Ok(Err(message)),
        };
        let vocabulary = read_lock(&self.canonical_fields, "canonical_fields")?;
        let schemas = read_lock(&self.schemas, "schemas")?;
        let used = field_hash_use(&schemas);
        let mut findings = Vec::new();
        for name in &draft.fields {
            if let Err(reason) = validate_new_field_name(name) {
                let reason = reason
                    .strip_prefix("invalid_canonical_name: ")
                    .unwrap_or(&reason);
                findings.push(finding(
                    AdviceKind::Abbreviation,
                    name,
                    reason.to_string(),
                    AdviceEvidence::default(),
                ));
            }
            let canonical = vocabulary.get(name.as_str());
            let near = if canonical.is_some() {
                Vec::new()
            } else {
                near_names(name, draft.types.get(name), &vocabulary)
            };
            match canonical {
                Some(canonical) => findings.push(canonical_finding(name, canonical, &draft, &used)),
                None => findings.extend(near.iter().map(|(other, field)| {
                    finding(
                        AdviceKind::NearMatch,
                        name,
                        format!(
                            "Consider `{other}` instead of `{name}`: {}",
                            field.description
                        ),
                        AdviceEvidence {
                            canonical_field_hash: Some(field.field_hash(other)),
                            suggested_name: Some((*other).to_string()),
                            suggested_type: Some(field.field_type.clone()),
                            ..AdviceEvidence::default()
                        },
                    )
                })),
            }
            if !draft.types.contains_key(name) {
                let suggested = canonical.or_else(|| near.first().map(|(_, field)| *field));
                let message = suggested.map_or_else(
                    || format!("`{name}` has no type. Every field needs one."),
                    |field| {
                        format!(
                            "`{name}` has no type. Suggested type: {} (from the vocabulary).",
                            field.field_type
                        )
                    },
                );
                findings.push(finding(
                    AdviceKind::MissingType,
                    name,
                    message,
                    AdviceEvidence {
                        suggested_type: suggested.map(|field| field.field_type.clone()),
                        ..AdviceEvidence::default()
                    },
                ));
            }
            if let Some(label) = canonical
                .filter(|_| !draft.classified.contains(name))
                .and_then(|field| field.classification.as_ref())
            {
                findings.push(finding(
                    AdviceKind::SuggestedClassification,
                    name,
                    format!(
                        "`{name}` is usually classified as {}.",
                        classification_label(label)
                    ),
                    AdviceEvidence {
                        suggested_classification: Some(label.clone()),
                        ..AdviceEvidence::default()
                    },
                ));
            }
        }
        findings.extend(similar_schemas(&draft, request.app_id.as_deref(), &schemas));
        Ok(Ok(SchemaAdviceResponse { findings }))
    }

    /// [`Self::schema_advice`], classified for HTTP.
    #[must_use]
    pub fn schema_advice_reply(&self, request: &SchemaAdviceRequest) -> SchemaAdviceReply {
        match self.schema_advice(request) {
            Ok(Ok(response)) => SchemaAdviceReply::Ready(response),
            Ok(Err(message)) => SchemaAdviceReply::Invalid(message),
            Err(error) => SchemaAdviceReply::Failed(format!("Failed to compute advice: {error}")),
        }
    }
}
