//! Advice for one draft schema declaration (`POST /v1/advice/schema`).
//!
//! A node declares its schemas locally, so nothing here can block a declare.
//! Advice reads the canonical vocabulary and returns findings: canonical
//! matches, names that mean something else here, near names, name policy
//! problems, missing types, and usual classifications.

use crate::DataClassification;
use schema_types::{DeclarativeSchemaDefinition, FieldValueType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

mod findings;
mod matching;

/// Maximum fields one draft may name before advice refuses it.
const MAX_ADVICE_FIELDS: usize = 512;

/// One draft declaration: the JSON an app puts in `lastdb-app.json` `schemas`.
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

/// Signed registry listing behind a catalog suggestion.
#[derive(Debug, Serialize)]
pub struct SchemaProvenance {
    pub app_id: String,
    pub schema_name: String,
    pub identity_hash: String,
    pub source: String,
    pub sha: String,
    pub channel: String,
    pub api_version: u32,
}

/// Catalog facts behind a finding. Every member is optional.
#[derive(Debug, Default, Serialize)]
pub struct AdviceEvidence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_field_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_type: Option<FieldValueType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_classification: Option<DataClassification>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaProvenance>,
}

/// One suggestion. No finding blocks anything.
#[derive(Debug, Serialize)]
pub struct AdviceFinding {
    pub kind: AdviceKind,
    /// Field the finding is about; `None` for a schema-level finding.
    pub field: Option<String>,
    pub message: String,
    pub evidence: AdviceEvidence,
}

#[derive(Debug, Serialize)]
pub struct SchemaAdviceResponse {
    pub findings: Vec<AdviceFinding>,
}

/// One normalized definition from a publisher schema push, with the listing that push was checked against.
pub struct CatalogSchema<'a> {
    pub definition: &'a DeclarativeSchemaDefinition,
    pub app_id: &'a str,
    pub source: &'a str,
    pub sha: &'a str,
    pub channel: &'a str,
    pub api_version: u32,
}

impl SchemaAdviceResponse {
    /// Add up to three schema examples using distinctly named matching fields.
    pub fn add_catalog_examples<'a>(
        &mut self,
        request: &SchemaAdviceRequest,
        catalog: impl IntoIterator<Item = CatalogSchema<'a>>,
    ) {
        let Ok(draft) = draft(&request.schema) else {
            return;
        };
        let fields: BTreeSet<_> = draft.fields.iter().map(String::as_str).collect();
        if fields.len() < 2 {
            return;
        }
        let normalized: Vec<_> = fields
            .iter()
            .map(|&field| (field, matching::field_name(field)))
            .collect();

        let mut matches = Vec::new();
        for row in catalog {
            if request.app_id.as_deref() == Some(row.app_id) {
                continue;
            }
            let Some(id) = row.definition.identity_hash.as_deref() else {
                continue;
            };
            let candidate: BTreeSet<_> = row
                .definition
                .fields
                .iter()
                .flatten()
                .map(String::as_str)
                .collect();
            let shared = shared_fields(&draft, &normalized, &candidate, row.definition);
            if shared >= 2 && shared * 2 >= fields.len() {
                let score = shared * 1024 / (fields.len() + candidate.len() - shared);
                matches.push((score, shared, id, row));
            }
        }
        matches.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| (&a.3.definition.name, a.2).cmp(&(&b.3.definition.name, b.2)))
        });
        let mut seen = HashSet::new();
        self.findings.extend(
            matches
                .into_iter()
                .filter(|(_, _, id, row)| seen.insert((*id, row.definition.name.as_str())))
                .take(3)
                .map(|(_, shared, id, row)| similar_finding(&row, id, shared, fields.len())),
        );
    }
}

fn shared_fields(
    draft: &Draft,
    fields: &[(&str, matching::FieldName)],
    candidate: &BTreeSet<&str>,
    definition: &DeclarativeSchemaDefinition,
) -> usize {
    let mut by_name = BTreeMap::<_, Vec<_>>::new();
    for &field in candidate {
        by_name
            .entry(matching::field_name(field).normalized)
            .or_default()
            .push(field);
    }
    let mut matched = BTreeSet::new();
    for (field, name) in fields {
        let Some(candidates) = by_name.get(name.normalized.as_str()) else {
            continue;
        };
        let agrees = candidates.iter().any(|&other| {
            draft
                .types
                .get(*field)
                .is_none_or(|expected| definition.field_types.get(other) == Some(expected))
                && matching::descriptions_compatible(
                    draft.descriptions.get(*field).map(String::as_str),
                    definition.field_descriptions.get(other).map(String::as_str),
                )
        });
        if agrees {
            matched.insert(name.normalized.as_str());
        }
    }
    matched.len()
}

fn similar_finding(
    row: &CatalogSchema<'_>,
    id: &str,
    shared: usize,
    total: usize,
) -> AdviceFinding {
    AdviceFinding {
        kind: AdviceKind::SimilarSchema,
        field: None,
        message: format!(
            "`{}` from `{}` matches {shared} of {total} draft fields.",
            row.definition.name, row.app_id
        ),
        evidence: AdviceEvidence {
            schema: Some(SchemaProvenance {
                app_id: row.app_id.to_owned(),
                schema_name: row.definition.name.clone(),
                identity_hash: id.to_owned(),
                source: row.source.to_owned(),
                sha: row.sha.to_owned(),
                channel: row.channel.to_owned(),
                api_version: row.api_version,
            }),
            ..AdviceEvidence::default()
        },
    }
}

struct Draft {
    fields: Vec<String>,
    types: HashMap<String, FieldValueType>,
    descriptions: HashMap<String, String>,
    versions: HashMap<String, u32>,
    classified: BTreeSet<String>,
}

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
