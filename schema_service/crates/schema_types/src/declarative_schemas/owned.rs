//! Owned app declaration normalization, wire deserialization, and key-field types.

use super::DeclarativeSchemaDefinition;
use crate::{DeclarativeSchemaType, FieldValueType};
use serde::Deserialize as _;
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedDeclaration {
    pub namespace: String,
    pub local_schema_id: String,
}

impl DeclarativeSchemaDefinition {
    pub fn normalize_owned_declaration(
        &mut self,
        namespace_label: &str,
        namespace: &str,
    ) -> Result<OwnedDeclaration, String> {
        let namespace = namespace.trim().to_string();
        if namespace.is_empty() || namespace.contains('/') {
            return Err(format!(
                "{namespace_label} must be non-empty and must not contain '/'"
            ));
        }
        let raw_name = self.name.trim();
        if raw_name.is_empty() {
            return Err("schema.name must be non-empty".into());
        }
        let local_name = match raw_name.split_once('/') {
            Some((owner, local)) if owner == namespace && !local.is_empty() => local,
            Some(_) => {
                return Err(format!(
                    "schema.name namespace must match request.{namespace_label}"
                ))
            }
            None => raw_name,
        };
        let local_schema_id = format!("{namespace}/{local_name}");
        self.owner_app_id = Some(namespace.clone());
        if self
            .descriptive_name
            .as_deref()
            .is_none_or(|n| n.trim().is_empty())
        {
            self.descriptive_name = Some(local_name.to_string());
        }
        self.name.clone_from(&local_schema_id);
        self.validate_owned_schema()?;
        self.compute_identity_hash();
        self.validate_definition()
            .map_err(|error| format!("Invalid data: {error}"))?;
        if self.get_identity_hash().is_none() {
            return Err("schema identity hash missing after compute_identity_hash".into());
        }
        self.require_typed_fields()?;
        Ok(OwnedDeclaration {
            namespace,
            local_schema_id,
        })
    }

    pub fn normalize_wire(&mut self) {
        self.hash = None;
        self.superseded_by = None;
        if self.identity_hash.is_none() && self.owner_app_id.is_some() {
            self.compute_identity_hash();
        }
    }

    pub(super) fn require_typed_fields(&self) -> Result<(), String> {
        let fields = self.fields.as_deref().unwrap_or_default();
        if (fields.is_empty() && self.record_keys.is_none())
            || fields.iter().any(|f| f.trim().is_empty())
        {
            return Err("schema.fields must be non-empty without blank names".into());
        }
        let untyped: Vec<&str> = fields
            .iter()
            .filter(|f| !self.field_types.contains_key(*f))
            .map(String::as_str)
            .collect();
        untyped.is_empty().then_some(()).ok_or_else(|| {
            format!(
                "every field needs a type in schema.field_types; these fields have none: {}",
                untyped.join(", ")
            )
        })
    }

    #[must_use]
    pub fn key_field_type(&self, path: &str) -> FieldValueType {
        self.resolve_field_type(path).unwrap_or(FieldValueType::Any)
    }

    fn resolve_field_type(&self, path: &str) -> Option<FieldValueType> {
        self.field_types.get(path).cloned().or_else(|| {
            let (root, child) = path.split_once('.')?;
            nested_type(self.field_types.get(root)?, child)
        })
    }

    pub fn validate_key_field_types(&self) -> Result<(), String> {
        for path in self.key.iter().filter_map(|k| k.hash_field.as_deref()) {
            let temporal = if let Some(ty) = self.field_types.get(path) {
                temporal_at_path(ty, None)
            } else if let Some((root, child)) = path.split_once('.') {
                self.field_types
                    .get(root)
                    .is_some_and(|ty| temporal_at_path(ty, Some(child)))
            } else {
                false
            };
            if temporal {
                return Err(format!(
                    "key field '{path}' uses Date or DateTime outside a typed range contract"
                ));
            }
        }
        Ok(())
    }
}

impl serde::Serialize for DeclarativeSchemaDefinition {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        decode_definition(Map::<String, Value>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

fn decode_definition(mut map: Map<String, Value>) -> Result<DeclarativeSchemaDefinition, String> {
    for name in ["field_key_id", "record_mapper"] {
        if map.contains_key(name) {
            return Err(format!("schema-level '{name}' is not supported"));
        }
    }
    if let Some(raw_fields) = map.remove("fields") {
        if let Some(fields) = normalize_fields(raw_fields)? {
            map.insert(
                "fields".into(),
                Value::Array(fields.into_iter().map(Value::String).collect()),
            );
        }
    }
    let had_schema_type = map.get("schema_type").is_some_and(|v| !v.is_null());
    if !had_schema_type {
        map.insert("schema_type".into(), Value::String("Single".into()));
    }
    let mut schema =
        DeclarativeSchemaDefinition::deserialize(Value::Object(map)).map_err(|e| e.to_string())?;
    if let Some(keys) = &schema.record_keys {
        keys.validate()?;
        let kind = match keys.primary.kind {
            crate::record_keys::RecordKeyKind::Hash => DeclarativeSchemaType::Hash,
            crate::record_keys::RecordKeyKind::HashRange => DeclarativeSchemaType::HashRange,
        };
        if schema.key.is_some()
            || !schema.alternate_keys.is_empty()
            || (had_schema_type && schema.schema_type != kind)
        {
            return Err(
                "record_keys cannot be mixed with field-derived or conflicting key declarations"
                    .into(),
            );
        }
        schema.schema_type = kind;
    } else if !had_schema_type {
        let (h, r) = schema.key.as_ref().map_or((false, false), |k| {
            (k.hash_field.is_some(), k.range_field.is_some())
        });
        schema.schema_type = match (h, r) {
            (true, true) => DeclarativeSchemaType::HashRange,
            (true, false) => DeclarativeSchemaType::Hash,
            (false, true) => DeclarativeSchemaType::Range,
            (false, false) => DeclarativeSchemaType::Single,
        };
    }
    if schema.identity_hash.is_none() && (schema.owner_app_id.is_some() || schema.file_required) {
        schema.compute_identity_hash();
    }
    schema.validate_definition()?;
    Ok(schema)
}

fn normalize_fields(val: Value) -> Result<Option<Vec<String>>, String> {
    match val {
        Value::Null => Ok(None),
        Value::Array(arr) => arr
            .into_iter()
            .map(|item| match item {
                Value::String(s) => Ok(s),
                _ => Err("Invalid fields array; expected strings".into()),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Value::Object(obj) => {
            let mut names: Vec<String> = obj.into_keys().collect();
            names.sort();
            Ok(Some(names))
        }
        _ => Err("Invalid fields; expected array or object map".into()),
    }
}

fn nested_type(field_type: &FieldValueType, path: &str) -> Option<FieldValueType> {
    match field_type {
        FieldValueType::Object(fields) => Some(
            fields
                .get(path)
                .cloned()
                .or_else(|| {
                    let (root, child) = path.split_once('.')?;
                    nested_type(fields.get(root)?, child)
                })
                .unwrap_or(FieldValueType::Any),
        ),
        FieldValueType::Map(value_type) => Some((**value_type).clone()),
        FieldValueType::Any => Some(FieldValueType::Any),
        FieldValueType::OneOf(variants) => {
            let mut types: Vec<_> = variants
                .iter()
                .filter_map(|ty| nested_type(ty, path))
                .collect();
            match types.len() {
                0 => None,
                1 => types.pop(),
                _ => Some(FieldValueType::OneOf(types)),
            }
        }
        _ => None,
    }
}

fn temporal_at_path(field_type: &FieldValueType, child: Option<&str>) -> bool {
    match (field_type, child) {
        (FieldValueType::Date | FieldValueType::DateTime, None) => true,
        (FieldValueType::OneOf(variants), _) => variants.iter().any(|v| temporal_at_path(v, child)),
        (FieldValueType::Object(fields), Some(path)) => match fields.get(path) {
            Some(ty) => temporal_at_path(ty, None),
            None => path.split_once('.').is_some_and(|(r, sub)| {
                fields
                    .get(r)
                    .is_some_and(|ty| temporal_at_path(ty, Some(sub)))
            }),
        },
        (FieldValueType::Map(value_type), Some(path)) => {
            temporal_at_path(value_type, None)
                || path
                    .split_once('.')
                    .is_some_and(|(_, sub)| temporal_at_path(value_type, Some(sub)))
        }
        _ => false,
    }
}
