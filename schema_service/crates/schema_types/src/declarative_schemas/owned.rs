//! Owned app declaration normalization, wire deserialization, and key-field types.

use super::DeclarativeSchemaDefinition;
use crate::{DeclarativeSchemaType, FieldValueType, KeyConfig};
use std::collections::{BTreeMap, HashMap};

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

#[derive(serde::Deserialize)]
struct DeclarativeSchemaDefinitionHelper {
    name: String,
    descriptive_name: Option<String>,
    #[serde(default)]
    purpose_statement: Option<String>,
    schema_type: Option<DeclarativeSchemaType>,
    key: Option<KeyConfig>,
    #[serde(default)]
    alternate_keys: BTreeMap<String, crate::AlternateKey>,
    #[serde(default)]
    record_keys: Option<crate::record_keys::RecordKeys>,
    #[serde(default)]
    file_required: bool,
    fields: Option<serde_json::Value>,
    #[serde(default)]
    field_classifications: HashMap<String, Vec<String>>,
    #[serde(default)]
    field_descriptions: HashMap<String, String>,
    #[serde(default)]
    field_interest_categories: HashMap<String, String>,
    #[serde(default)]
    field_types: HashMap<String, FieldValueType>,
    #[serde(default)]
    field_hashes: HashMap<String, String>,
    #[serde(default)]
    field_declarations: HashMap<String, String>,
    #[serde(default)]
    field_versions: HashMap<String, u32>,
    identity_hash: Option<String>,
    #[serde(default)]
    identity_hash_algo_version: Option<u32>,
    #[serde(default)]
    superseded_by: Option<String>,
    #[serde(default)]
    owner_app_id: Option<String>,
    #[serde(flatten)]
    unknown: HashMap<String, serde::de::IgnoredAny>,
}

impl DeclarativeSchemaDefinitionHelper {
    fn into_definition(self) -> Result<DeclarativeSchemaDefinition, String> {
        for name in ["field_key_id", "record_mapper"] {
            if self.unknown.contains_key(name) {
                return Err(format!("schema-level '{name}' is not supported"));
            }
        }
        let fields = normalize_fields(self.fields)?;
        let schema_type = if let Some(keys) = &self.record_keys {
            keys.validate()?;
            let kind = match keys.primary.kind {
                crate::record_keys::RecordKeyKind::Hash => DeclarativeSchemaType::Hash,
                crate::record_keys::RecordKeyKind::HashRange => DeclarativeSchemaType::HashRange,
            };
            if self.key.is_some()
                || !self.alternate_keys.is_empty()
                || self
                    .schema_type
                    .as_ref()
                    .is_some_and(|declared| declared != &kind)
            {
                return Err(
                    "record_keys cannot be mixed with field-derived or conflicting key declarations"
                        .into(),
                );
            }
            kind
        } else {
            match (&self.schema_type, &self.key) {
                (Some(st), _) => st.clone(),
                (None, Some(k)) => match (k.hash_field.is_some(), k.range_field.is_some()) {
                    (true, true) => DeclarativeSchemaType::HashRange,
                    (true, false) => DeclarativeSchemaType::Hash,
                    (false, true) => DeclarativeSchemaType::Range,
                    (false, false) => DeclarativeSchemaType::Single,
                },
                (None, None) => DeclarativeSchemaType::Single,
            }
        };

        let mut schema = DeclarativeSchemaDefinition {
            name: self.name,
            descriptive_name: self.descriptive_name,
            purpose_statement: self.purpose_statement,
            schema_type,
            key: self.key,
            alternate_keys: self.alternate_keys,
            record_keys: self.record_keys,
            file_required: self.file_required,
            fields,
            hash: None,
            field_classifications: self.field_classifications,
            field_descriptions: self.field_descriptions,
            field_interest_categories: self.field_interest_categories,
            field_types: self.field_types,
            field_hashes: self.field_hashes,
            field_versions: self.field_versions,
            field_declarations: self.field_declarations,
            identity_hash: self.identity_hash,
            identity_hash_algo_version: self.identity_hash_algo_version,
            superseded_by: self.superseded_by,
            owner_app_id: self.owner_app_id,
        };

        if schema.identity_hash.is_none() && (schema.owner_app_id.is_some() || schema.file_required)
        {
            schema.compute_identity_hash();
        }
        schema.validate_definition()?;
        Ok(schema)
    }
}

impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        DeclarativeSchemaDefinitionHelper::deserialize(deserializer)?
            .into_definition()
            .map_err(serde::de::Error::custom)
    }
}

fn normalize_fields(fields: Option<serde_json::Value>) -> Result<Option<Vec<String>>, String> {
    let Some(val) = fields else {
        return Ok(None);
    };
    if let Some(arr) = val.as_array() {
        arr.iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "Invalid fields array; expected strings".to_string())
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    } else if let Some(obj) = val.as_object() {
        let mut names: Vec<String> = obj.keys().cloned().collect();
        names.sort();
        Ok(Some(names))
    } else {
        Err("Invalid fields; expected array or object map".into())
    }
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
