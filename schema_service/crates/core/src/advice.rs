//! Advice for one draft schema declaration (`POST /v1/advice/schema`).
//!
//! A node declares its schemas locally, so nothing here can block a declare.
//! Advice reads the canonical vocabulary and returns findings: canonical
//! matches, names that mean something else here, near names, name policy
//! problems, missing types, and usual classifications.

use crate::types::DataClassification;
use schema_types::FieldValueType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

mod findings;

/// Maximum fields one draft may name before advice refuses it.
pub const MAX_ADVICE_FIELDS: usize = 512;

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

pub enum SchemaAdviceReply {
    Ready(SchemaAdviceResponse),
    Invalid(String),
}

impl SchemaAdviceReply {
    /// Convert to response or HTTP status + JSON error body.
    ///
    /// # Errors
    /// HTTP 400 when `request.schema` cannot be parsed as a draft object.
    pub fn into_http(self) -> Result<SchemaAdviceResponse, (u16, Value)> {
        match self {
            Self::Ready(response) => Ok(response),
            Self::Invalid(message) => Err((400, crate::error_only_body(&message))),
        }
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
