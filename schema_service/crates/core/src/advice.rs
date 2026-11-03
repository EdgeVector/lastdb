//! Advice for one draft schema declaration (`POST /v1/advice/schema`,
//! `design-lastdb-advisory-schema-service` section 9).
//!
//! A node declares its schemas locally, so nothing here can block a declare.
//! Advice reads the canonical vocabulary and the catalog, writes nothing, and
//! returns findings: canonical matches, names that mean something else here,
//! near names, name policy problems, missing types, usual classifications, and
//! schemas of other apps that cover most of the same fields.

use crate::field_name_policy::validate_new_field_name;
use crate::lock_helpers::read_lock;
use crate::state::SchemaServiceState;
use crate::types::CanonicalField;
use crate::vocabulary_matching::{name_overlap, same_description};
use schema_types::{DataClassification, FieldValueType, FoldDbResult, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

/// The most fields one draft may name. A larger draft is refused before any
/// matching runs.
pub const MAX_ADVICE_FIELDS: usize = 512;
const MAX_NEAR_MATCHES: usize = 3;
const MAX_SIMILAR_SCHEMAS: usize = 3;
/// Share of the draft's fields that another app's schema must carry before
/// advice names it.
const SIMILAR_SCHEMA_COVERAGE: f64 = 0.6;

/// One draft declaration: the JSON an app puts in `lastdb-app.json`
/// `schemas`, as written. `app_id`, when given, leaves that app's own schemas
/// out of the similar-schema findings.
#[derive(Debug, Deserialize)]
pub struct SchemaAdviceRequest {
    #[serde(default)]
    pub app_id: Option<String>,
    pub schema: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdviceKind {
    CanonicalMatch,
    SameNameDifferentMeaning,
    NearMatch,
    Abbreviation,
    MissingType,
    SuggestedClassification,
    SimilarSchema,
}

/// The catalog facts behind a finding. Every member is optional.
#[derive(Debug, Default, Serialize)]
pub struct AdviceEvidence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_field_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_identity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_type: Option<FieldValueType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_classification: Option<DataClassification>,
}

/// One suggestion. No finding blocks anything.
#[derive(Debug, Serialize)]
pub struct AdviceFinding {
    pub kind: AdviceKind,
    /// The field the finding is about; `None` for a schema-level finding.
    pub field: Option<String>,
    pub message: String,
    pub evidence: AdviceEvidence,
}

#[derive(Debug, Serialize)]
pub struct SchemaAdviceResponse {
    pub findings: Vec<AdviceFinding>,
}

pub enum SchemaAdviceReply {
    Ready(SchemaAdviceResponse),
    Invalid(String),
    Failed(String),
}

impl SchemaAdviceReply {
    /// The response, or the HTTP status and error body.
    ///
    /// # Errors
    /// HTTP 400 for a draft advice cannot read, HTTP 500 for a lock failure.
    pub fn into_http(self) -> Result<SchemaAdviceResponse, (u16, Value)> {
        match self {
            Self::Ready(response) => Ok(response),
            Self::Invalid(message) => Err((400, crate::error_only_body(&message))),
            Self::Failed(message) => Err((500, crate::error_only_body(&message))),
        }
    }
}

/// The parts of a draft that advice reads. Parsing is loose on purpose: a
/// member advice does not understand is ignored, and a type that does not
/// parse counts as missing, so any manifest entry gets advice.
struct Draft {
    fields: Vec<String>,
    types: HashMap<String, FieldValueType>,
    descriptions: HashMap<String, String>,
    versions: HashMap<String, u32>,
    classified: BTreeSet<String>,
}

/// The name and value of each member of the object `member`, if it is one.
fn members<'a>(
    object: &'a serde_json::Map<String, Value>,
    member: &str,
) -> impl Iterator<Item = (&'a String, &'a Value)> {
    object
        .get(member)
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
}

fn draft(schema: &Value) -> Result<Draft, String> {
    let object = schema
        .as_object()
        .ok_or("schema must be a JSON object, one entry of lastdb-app.json schemas")?;
    let fields: Vec<String> = object
        .get("fields")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();
    if fields.len() > MAX_ADVICE_FIELDS {
        return Err(format!(
            "schema has {} fields; advice reads at most {MAX_ADVICE_FIELDS}",
            fields.len()
        ));
    }
    let types: HashMap<String, FieldValueType> = members(object, "field_types")
        .filter_map(|(name, kind)| {
            serde_json::from_value(kind.clone())
                .ok()
                .map(|kind| (name.clone(), kind))
        })
        .collect();
    let descriptions = members(object, "field_descriptions")
        .filter_map(|(name, text)| text.as_str().map(|text| (name.clone(), text.to_string())))
        .collect();
    let versions = members(object, "field_versions")
        .filter_map(|(name, version)| {
            version
                .as_u64()
                .and_then(|version| u32::try_from(version).ok())
                .map(|version| (name.clone(), version))
        })
        .collect();
    let classified = members(object, "field_data_classifications")
        .chain(members(object, "field_classifications"))
        .map(|(name, _)| name.clone())
        .collect();
    Ok(Draft {
        fields,
        types,
        descriptions,
        versions,
        classified,
    })
}

fn finding(
    kind: AdviceKind,
    field: &str,
    message: String,
    evidence: AdviceEvidence,
) -> AdviceFinding {
    AdviceFinding {
        kind,
        field: Some(field.to_string()),
        message,
        evidence,
    }
}

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
