use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) mod temporal;

/// Strongly typed value types for schema fields.
///
/// Every declared field names one of these types, and every write is checked
/// against it. A declaration with a field that has no type is refused.
/// `Any` remains only until Brain's two file pointer fields move to `File`;
/// then it is deleted (`decision-2026-11-02-field-types-no-any`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
pub enum FieldValueType {
    // ── Primitives ──────────────────────────────────────
    String,
    Integer,
    Float,
    Number, // accepts either integer or float
    Boolean,
    Null,

    // ── Compound ────────────────────────────────────────
    /// Homogeneous typed array: `Array(String)`, `Array(Integer)`
    Array(Box<Self>),
    /// Typed key-value map (JSON keys are always strings):
    /// `Map(String, Number)` means `{"a": 1, "b": 2}`
    Map(Box<Self>),
    /// Typed struct with named fields:
    /// `Object({"name": String, "age": Integer})`
    Object(BTreeMap<String, Self>),

    // ── References ──────────────────────────────────────
    /// Reference to another schema. The string is the schema name.
    /// Enforced: the field value must be a reference object or array
    /// of reference objects pointing to this specific schema.
    SchemaRef(String),

    // ── Union ───────────────────────────────────────────
    /// Union type: matches if the value satisfies any variant.
    /// Common use: `OneOf([String, Null])` for nullable strings.
    OneOf(Vec<Self>),

    // ── Escape hatch ────────────────────────────────────
    /// Accepts any JSON value. Used for backward compatibility
    /// with existing schemas that have no type declarations.
    Any,

    // Append new variants to retain the existing enum discriminants.
    /// Gregorian calendar date, encoded as a strict `YYYY-MM-DD` JSON string.
    Date,
    /// RFC 3339 instant with an offset and at most nine fractional digits.
    DateTime,
    /// A file blob pointer: `{"$lastdb_file": {"blob_ref": "...", ...}}`, the
    /// value a local or personal file blob put returns.
    File,
}

/// The member that marks a file blob pointer. `lastdb_model` depends on this
/// crate, so the name is repeated here rather than imported.
const LASTDB_FILE_KEY: &str = "$lastdb_file";

impl FieldValueType {
    /// Validate a JSON value against this type. Returns Ok(()) if valid,
    /// Err with a human-readable message if not.
    pub fn validate(&self, value: &serde_json::Value) -> Result<(), String> {
        match self {
            Self::Any => Ok(()),
            Self::File => validate_file_pointer(value),
            Self::Date => temporal::validate_date(value),
            Self::DateTime => temporal::validate_datetime(value),
            Self::Null => value
                .is_null()
                .then_some(())
                .ok_or_else(|| type_err("Null", value)),
            Self::String => value
                .is_string()
                .then_some(())
                .ok_or_else(|| type_err("String", value)),
            Self::Boolean => value
                .is_boolean()
                .then_some(())
                .ok_or_else(|| type_err("Boolean", value)),
            Self::Integer => (value.is_number() && value.as_i64().is_some())
                .then_some(())
                .ok_or_else(|| type_err("Integer", value)),
            Self::Float => (value.is_number() && value.as_f64().is_some())
                .then_some(())
                .ok_or_else(|| type_err("Float", value)),
            Self::Number => value
                .is_number()
                .then_some(())
                .ok_or_else(|| type_err("Number", value)),
            Self::Array(element_type) => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| format!("expected Array, got {}", json_type_name(value)))?;
                for (i, elem) in arr.iter().enumerate() {
                    element_type
                        .validate(elem)
                        .map_err(|e| format!("Array[{i}]: {e}"))?;
                }
                Ok(())
            }
            Self::Map(value_type) => {
                let obj = value
                    .as_object()
                    .ok_or_else(|| format!("expected Map, got {}", json_type_name(value)))?;
                for (k, v) in obj {
                    value_type
                        .validate(v)
                        .map_err(|e| format!("Map[\"{k}\"]: {e}"))?;
                }
                Ok(())
            }
            Self::Object(field_types) => {
                let obj = value
                    .as_object()
                    .ok_or_else(|| format!("expected Object, got {}", json_type_name(value)))?;
                for (field_name, field_type) in field_types {
                    let field_value = obj.get(field_name).unwrap_or(&serde_json::Value::Null);
                    field_type
                        .validate(field_value)
                        .map_err(|e| format!(".{field_name}: {e}"))?;
                }
                Ok(())
            }
            Self::SchemaRef(schema_name) => {
                // A schema reference must be either:
                // - A single ref object: {"schema": "X", "key": {...}}
                // - An array of ref objects
                if let Some(arr) = value.as_array() {
                    for (i, elem) in arr.iter().enumerate() {
                        validate_ref_object(elem, schema_name)
                            .map_err(|e| format!("SchemaRef[{i}]: {e}"))?;
                    }
                    Ok(())
                } else if value.is_object() {
                    validate_ref_object(value, schema_name)
                } else {
                    Err(format!(
                        "expected SchemaRef({}), got {}",
                        schema_name,
                        json_type_name(value)
                    ))
                }
            }
            Self::OneOf(variants) => {
                for variant in variants {
                    if variant.validate(value).is_ok() {
                        return Ok(());
                    }
                }
                Err(format!(
                    "value does not match any variant of OneOf({}), got {}",
                    self,
                    json_type_name(value)
                ))
            }
        }
    }
}

fn validate_ref_object(value: &serde_json::Value, expected_schema: &str) -> Result<(), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "expected reference object".to_string())?;
    let schema = obj
        .get("schema")
        .and_then(|s| s.as_str())
        .ok_or_else(|| "reference object missing 'schema' field".to_string())?;
    if schema != expected_schema {
        return Err(format!(
            "expected reference to schema '{expected_schema}', got '{schema}'"
        ));
    }
    if !obj.contains_key("key") {
        return Err("reference object missing 'key' field".to_string());
    }
    Ok(())
}

/// A pointer is an object whose `$lastdb_file` member names its blob, the same
/// shape `lastdb_model::file_pointer::blob_ref_from_value` reads. The record
/// store checks the access metadata itself.
fn validate_file_pointer(value: &serde_json::Value) -> Result<(), String> {
    let obj = value.as_object().ok_or_else(|| type_err("File", value))?;
    let inner = obj
        .get(LASTDB_FILE_KEY)
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("expected File: an object with a '{LASTDB_FILE_KEY}' object"))?;
    if inner
        .get("blob_ref")
        .and_then(serde_json::Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(format!(
            "expected File: '{LASTDB_FILE_KEY}' has no 'blob_ref'"
        ));
    }
    Ok(())
}

fn type_err(expected: &str, got: &serde_json::Value) -> String {
    format!("expected {expected}, got {}", json_type_name(got))
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "Null",
        serde_json::Value::Bool(_) => "Boolean",
        serde_json::Value::Number(_) => "Number",
        serde_json::Value::String(_) => "String",
        serde_json::Value::Array(_) => "Array",
        serde_json::Value::Object(_) => "Object",
    }
}

impl std::fmt::Display for FieldValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::Date => write!(f, "Date"),
            Self::DateTime => write!(f, "DateTime"),
            Self::File => write!(f, "File"),
            Self::Integer => write!(f, "Integer"),
            Self::Float => write!(f, "Float"),
            Self::Number => write!(f, "Number"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Null => write!(f, "Null"),
            Self::Array(t) => write!(f, "Array<{t}>"),
            Self::Map(v) => write!(f, "Map<String, {v}>"),
            Self::Object(fields) => {
                write!(f, "{{")?;
                for (i, (k, v)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Self::SchemaRef(s) => write!(f, "Ref<{s}>"),
            Self::OneOf(variants) => {
                for (i, v) in variants.iter().enumerate() {
                    if i > 0 {
                        write!(f, " | ")?;
                    }
                    write!(f, "{v}")?;
                }
                Ok(())
            }
            Self::Any => write!(f, "Any"),
        }
    }
}
