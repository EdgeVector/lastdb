//! Owned app declaration normalization and key-field type resolution.

use super::DeclarativeSchemaDefinition;
use crate::FieldValueType;
use serde_json::Value;
use std::collections::BTreeMap;

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
        let raw_name = self.name.trim().to_string();
        if raw_name.is_empty() {
            return Err("schema.name must be non-empty".into());
        }
        let local_name = match raw_name.split_once('/') {
            Some((owner, local)) if owner == namespace && !local.is_empty() => local.to_string(),
            Some((_owner, _local)) => {
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
            .is_none_or(|name| name.trim().is_empty())
        {
            let short = local_schema_id.rsplit('/').next().unwrap_or_default();
            self.descriptive_name = Some(short.to_string());
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

    pub fn reset_field_hashes(&mut self) {
        self.field_hashes.clear();
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
            .filter(|field| !self.field_types.contains_key(*field))
            .map(String::as_str)
            .collect();
        if untyped.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "every field needs a type in schema.field_types; these fields have none: {}",
                untyped.join(", ")
            ))
        }
    }

    #[must_use]
    pub fn key_field_type(&self, path: &str) -> FieldValueType {
        if let Some(field_type) = self.field_types.get(path) {
            return field_type.clone();
        }
        path.split_once('.')
            .and_then(|(root, child)| {
                self.field_types
                    .get(root)
                    .and_then(|ty| nested_type(ty, child))
            })
            .unwrap_or(FieldValueType::Any)
    }

    pub fn validate_key_field_types(&self) -> Result<(), String> {
        for path in self
            .key
            .iter()
            .flat_map(|key| key.hash_field.iter().map(String::as_str))
        {
            let temporal = if let Some(field_type) = self.field_types.get(path) {
                temporal_at_path(field_type, None)
            } else if let Some((root, child)) = path.split_once('.') {
                self.field_types
                    .get(root)
                    .is_some_and(|field_type| temporal_at_path(field_type, Some(child)))
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

#[inline(never)]
pub fn schema_from_value(value: Value) -> Result<DeclarativeSchemaDefinition, serde_json::Error> {
    serde_json::from_value(value)
}

fn nested_type(field_type: &FieldValueType, path: &str) -> Option<FieldValueType> {
    match field_type {
        FieldValueType::Object(fields) => Some(
            fields
                .get(path)
                .cloned()
                .or_else(|| {
                    path.split_once('.').and_then(|(root, child)| {
                        fields.get(root).and_then(|ty| nested_type(ty, child))
                    })
                })
                .unwrap_or(FieldValueType::Any),
        ),
        FieldValueType::Map(value_type) => Some((**value_type).clone()),
        FieldValueType::Any => Some(FieldValueType::Any),
        FieldValueType::OneOf(variants) => {
            let types: Vec<_> = variants
                .iter()
                .filter_map(|ty| nested_type(ty, path))
                .collect();
            match types.as_slice() {
                [] => None,
                [only] => Some(only.clone()),
                _ => Some(FieldValueType::OneOf(types)),
            }
        }
        _ => None,
    }
}

fn temporal_at_path(field_type: &FieldValueType, child: Option<&str>) -> bool {
    match (field_type, child) {
        (FieldValueType::Date | FieldValueType::DateTime, None) => true,
        (FieldValueType::OneOf(variants), _) => variants
            .iter()
            .any(|variant| temporal_at_path(variant, child)),
        (FieldValueType::Object(fields), Some(path)) => temporal_in_object(fields, path),
        (FieldValueType::Map(value_type), Some(path)) => {
            temporal_at_path(value_type, None)
                || path
                    .split_once('.')
                    .is_some_and(|(_, child)| temporal_at_path(value_type, Some(child)))
        }
        _ => false,
    }
}

fn temporal_in_object(fields: &BTreeMap<String, FieldValueType>, path: &str) -> bool {
    if let Some(field_type) = fields.get(path) {
        return temporal_at_path(field_type, None);
    }
    path.split_once('.').is_some_and(|(root, child)| {
        fields
            .get(root)
            .is_some_and(|field_type| temporal_at_path(field_type, Some(child)))
    })
}
