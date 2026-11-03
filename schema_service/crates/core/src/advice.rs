//! Advice for one draft schema declaration (`POST /v1/advice/schema`,
//! `design-lastdb-advisory-schema-service` section 9).
//!
//! A node declares its schemas locally, so nothing here can block a declare.
//! Advice reads the canonical vocabulary and the catalog, writes nothing, and
//! returns findings: canonical matches, names that mean something else here,
//! near names, name policy problems, missing types, usual classifications, and
//! schemas of other apps that cover most of the same fields.

use schema_types::{DataClassification, FieldValueType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

mod findings;

/// The most fields one draft may name. A larger draft is refused before any
/// matching runs.
pub const MAX_ADVICE_FIELDS: usize = 512;

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
