//! Findings for one draft schema against the curated canonical vocabulary.

use super::{
    draft, finding, AdviceEvidence, AdviceFinding, AdviceKind, Draft, SchemaAdviceReply,
    SchemaAdviceRequest, SchemaAdviceResponse,
};
use crate::field_name_policy::validate_new_field_name;
use crate::types::{CanonicalField, DataClassification};
use crate::vocabulary_matching::{name_overlap, same_description};
use crate::SchemaServiceState;
use schema_types::FieldValueType;
use std::collections::HashMap;

const MAX_NEAR_MATCHES: usize = 3;

fn canonical_finding(name: &str, canonical: &CanonicalField, draft: &Draft) -> AdviceFinding {
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
    let exact = draft.types.contains_key(name) && draft.descriptions.contains_key(name);
    let message = if exact {
        format!("`{name}` matches the canonical field.")
    } else {
        format!(
            "`{name}` is a canonical field; give it the canonical type and description \"{}\" to match it exactly.",
            canonical.description
        )
    };
    finding(AdviceKind::CanonicalMatch, name, message, evidence)
}

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

impl SchemaServiceState {
    /// Findings for one draft, in field order.
    ///
    /// # Errors
    /// Returns a validation message when `request.schema` cannot be parsed as a draft.
    pub fn schema_advice(
        &self,
        request: &SchemaAdviceRequest,
    ) -> Result<SchemaAdviceResponse, String> {
        let _ = &request.app_id;
        let draft = draft(&request.schema)?;
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
            let canonical = self.canonical_fields.get(name.as_str());
            let near = if canonical.is_some() {
                Vec::new()
            } else {
                near_names(name, draft.types.get(name), &self.canonical_fields)
            };
            if let Some(canonical) = canonical {
                findings.push(canonical_finding(name, canonical, &draft));
            }
            findings.extend(near.iter().map(|&(other, field)| {
                finding(
                    AdviceKind::NearMatch,
                    name,
                    format!(
                        "Consider `{other}` instead of `{name}`: {}",
                        field.description
                    ),
                    AdviceEvidence {
                        canonical_field_hash: Some(field.field_hash(other)),
                        suggested_name: Some(other.to_string()),
                        suggested_type: Some(field.field_type.clone()),
                        ..AdviceEvidence::default()
                    },
                )
            }));
            if !draft.types.contains_key(name) {
                let suggested = canonical.or_else(|| near.first().map(|&(_, field)| field));
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
        Ok(SchemaAdviceResponse { findings })
    }

    /// [`Self::schema_advice`], classified for HTTP.
    #[must_use]
    pub fn schema_advice_reply(&self, request: &SchemaAdviceRequest) -> SchemaAdviceReply {
        match self.schema_advice(request) {
            Ok(response) => SchemaAdviceReply::Ready(response),
            Err(message) => SchemaAdviceReply::Invalid(message),
        }
    }
}
