//! Expand one manifest schema entry from the field-object form.
//!
//! `fields` as a JSON object is rewritten, in place, into the field list and
//! `field_*` maps [`crate::DeclarativeSchemaDefinition`] has always stored.
//! `fields` as an array is left unchanged. Call this on a manifest entry
//! before deserializing it. This does not normalize the declaration and does
//! not hash it.
//!
//! Object keys are stored in sorted order. `serde_json` reads an object as a
//! sorted map, and the portable definition id keeps the `fields` array order.
//! An older entry matches this form when its `fields` array is already sorted.
//! The identity hash sorts the names itself.
//!
//! `scripts/release/stable_place.py` repeats this rewrite. Keep the two together.

use crate::FieldValueType;
use serde_json::{Map, Value};

const KNOWN: &[&str] = &[
    "classifications",
    "declaration",
    "description",
    "hash",
    "interest_category",
    "type",
    "version",
];

#[derive(Default)]
struct FieldMaps {
    types: Map<String, Value>,
    descriptions: Map<String, Value>,
    classifications: Map<String, Value>,
    interest_categories: Map<String, Value>,
    versions: Map<String, Value>,
    declarations: Map<String, Value>,
    hashes: Map<String, Value>,
}

struct Expanded {
    names: Vec<Value>,
    maps: FieldMaps,
}

impl FieldMaps {
    fn write_into(self, object: &mut Map<String, Value>) {
        for (key, map) in [
            ("field_types", self.types),
            ("field_descriptions", self.descriptions),
            ("field_classifications", self.classifications),
            ("field_interest_categories", self.interest_categories),
            ("field_versions", self.versions),
            ("field_declarations", self.declarations),
            ("field_hashes", self.hashes),
        ] {
            if !map.is_empty() {
                object.insert(key.to_owned(), Value::Object(map));
            }
        }
    }
}

/// Rewrite a field-object schema entry into the stored field list and maps.
///
/// # Errors
///
/// Returns an error that names the schema when the object form is mixed with
/// `field_*` maps, or when a field object is not a typed field.
pub fn expand_manifest_schema(entry: &mut Value) -> Result<(), String> {
    let label = schema_label(entry);
    let Some(expanded) = prepare(entry, &label)? else {
        return Ok(());
    };
    let Some(object) = entry.as_object_mut() else {
        return Err(format!("schema '{label}' is not an object"));
    };
    object.insert("fields".to_owned(), Value::Array(expanded.names));
    expanded.maps.write_into(object);
    Ok(())
}

fn prepare(entry: &Value, label: &str) -> Result<Option<Expanded>, String> {
    let Some(object) = entry.as_object() else {
        return Ok(None);
    };
    if !object.get("fields").is_some_and(Value::is_object) {
        return Ok(None);
    }
    if let Some(key) = object.keys().find(|key| key.starts_with("field_")) {
        return Err(mixed_form(label, key));
    }
    let Some(fields) = object.get("fields").and_then(Value::as_object) else {
        return Ok(None);
    };
    expanded_fields(label, fields).map(Some)
}

fn expanded_fields(label: &str, fields: &Map<String, Value>) -> Result<Expanded, String> {
    let mut order: Vec<&str> = fields.keys().map(String::as_str).collect();
    order.sort_unstable();
    let mut maps = FieldMaps::default();
    for name in &order {
        let Some(spec) = fields.get(*name).and_then(Value::as_object) else {
            return Err(format!(
                "schema '{label}' field '{name}' must be an object with a type"
            ));
        };
        read_field(label, name, spec, &mut maps)?;
    }
    let names = order
        .into_iter()
        .map(|name| Value::String(name.to_owned()))
        .collect();
    Ok(Expanded { names, maps })
}

fn read_field(
    label: &str,
    field: &str,
    spec: &Map<String, Value>,
    maps: &mut FieldMaps,
) -> Result<(), String> {
    require_known(label, field, spec)?;
    require_type(spec, label, field)?;
    check_string(spec, "description", label, field)?;
    check_string(spec, "interest_category", label, field)?;
    check_string(spec, "declaration", label, field)?;
    check_string(spec, "hash", label, field)?;
    check_classifications(spec, label, field)?;
    check_version(spec, label, field)?;
    copy_present(spec, "type", &mut maps.types, field);
    copy_present(spec, "description", &mut maps.descriptions, field);
    copy_present(spec, "classifications", &mut maps.classifications, field);
    copy_present(
        spec,
        "interest_category",
        &mut maps.interest_categories,
        field,
    );
    copy_present(spec, "version", &mut maps.versions, field);
    copy_present(spec, "declaration", &mut maps.declarations, field);
    copy_present(spec, "hash", &mut maps.hashes, field);
    Ok(())
}

fn require_known(label: &str, field: &str, spec: &Map<String, Value>) -> Result<(), String> {
    let Some(key) = spec.keys().find(|key| !KNOWN.contains(&key.as_str())) else {
        return Ok(());
    };
    Err(format!(
        "schema '{label}' field '{field}' has unknown property '{key}'"
    ))
}

fn require_type(spec: &Map<String, Value>, label: &str, field: &str) -> Result<(), String> {
    let Some(kind) = spec.get("type").filter(|kind| !kind.is_null()) else {
        return Err(format!("schema '{label}' field '{field}' needs a type"));
    };
    serde_json::from_value::<FieldValueType>(kind.clone()).map_err(|error| {
        format!("schema '{label}' field '{field}' type is not a field type: {error}")
    })?;
    Ok(())
}

fn check_string(
    spec: &Map<String, Value>,
    prop: &str,
    label: &str,
    field: &str,
) -> Result<(), String> {
    match spec.get(prop) {
        None | Some(Value::String(_)) => Ok(()),
        Some(_) => Err(format!(
            "schema '{label}' field '{field}' property '{prop}' must be a string"
        )),
    }
}

fn check_classifications(
    spec: &Map<String, Value>,
    label: &str,
    field: &str,
) -> Result<(), String> {
    let Some(value) = spec.get("classifications") else {
        return Ok(());
    };
    let message = || {
        format!(
            "schema '{label}' field '{field}' property 'classifications' must be an array of strings"
        )
    };
    let Some(items) = value.as_array() else {
        return Err(message());
    };
    items
        .iter()
        .all(Value::is_string)
        .then_some(())
        .ok_or_else(message)
}

fn check_version(spec: &Map<String, Value>, label: &str, field: &str) -> Result<(), String> {
    let Some(value) = spec.get("version") else {
        return Ok(());
    };
    serde_json::from_value::<u32>(value.clone()).map_err(|_| {
        format!(
            "schema '{label}' field '{field}' property 'version' must be an integer from 0 through 4294967295"
        )
    })?;
    Ok(())
}

fn copy_present(spec: &Map<String, Value>, prop: &str, into: &mut Map<String, Value>, field: &str) {
    if let Some(value) = spec.get(prop) {
        into.insert(field.to_owned(), value.clone());
    }
}

fn schema_label(entry: &Value) -> String {
    entry
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("<unnamed>")
        .to_owned()
}

fn mixed_form(label: &str, key: &str) -> String {
    format!(
        "schema '{label}' uses the field object form and also sets '{key}'; write each field once, or keep the older field list and field_* maps"
    )
}
