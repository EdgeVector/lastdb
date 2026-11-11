use super::DeclarativeSchemaDefinition;
use crate::schema_type::DeclarativeSchemaType;
use crate::FieldValueType;
use crate::KeyConfig;
use std::collections::HashMap;

#[derive(serde::Deserialize)]
struct DeclarativeSchemaDefinitionHelper {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptive_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    purpose_statement: Option<String>,
    // Allow schema_type to be omitted; we will derive from key if missing
    schema_type: Option<DeclarativeSchemaType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<KeyConfig>,
    #[serde(default)]
    alternate_keys: std::collections::BTreeMap<String, crate::AlternateKey>,
    #[serde(default)]
    record_keys: Option<crate::record_keys::RecordKeys>,
    #[serde(default)]
    file_required: bool,
    // Accept either an array of strings or an object map and normalize later
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    identity_hash: Option<String>,
    #[serde(default)]
    identity_hash_algo_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    superseded_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    owner_app_id: Option<String>,
    // Preserve unrelated unknown-field behavior, but refuse removed storage
    // configuration even when its JSON value is null. Stored Schema Service
    // records can carry keys of retired features; those keys land here and
    // have no effect.
    #[serde(flatten)]
    unknown: HashMap<String, serde::de::IgnoredAny>,
}

impl DeclarativeSchemaDefinitionHelper {
    /// Build the definition through the constructor, then restore every
    /// persisted field the constructor does not derive.
    fn into_definition(self) -> Result<DeclarativeSchemaDefinition, String> {
        for name in ["field_key_id", "record_mapper"] {
            if self.unknown.contains_key(name) {
                return Err(format!("schema-level '{name}' is not supported"));
            }
        }
        let normalized_fields = normalize_fields(self.fields)?;

        // Determine schema_type if omitted
        let normalized_schema_type = if let Some(keys) = &self.record_keys {
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
                return Err("record_keys cannot be mixed with field-derived or conflicting key declarations".into());
            }
            kind
        } else {
            match (&self.schema_type, &self.key) {
                (Some(st), _) => st.clone(),
                (None, Some(k)) => {
                    let has_hash = k.hash_field.is_some();
                    let has_range = k.range_field.is_some();
                    if has_hash && has_range {
                        DeclarativeSchemaType::HashRange
                    } else if has_hash {
                        DeclarativeSchemaType::Hash
                    } else if has_range {
                        DeclarativeSchemaType::Range
                    } else {
                        DeclarativeSchemaType::Single
                    }
                }
                (None, None) => DeclarativeSchemaType::Single,
            }
        };

        let mut schema = DeclarativeSchemaDefinition::new(
            self.name,
            normalized_schema_type,
            self.key,
            normalized_fields,
        );

        schema.descriptive_name = self.descriptive_name;
        schema.purpose_statement = self.purpose_statement;

        for (field_name, classifications) in self.field_classifications {
            schema
                .field_classifications
                .insert(field_name, classifications);
        }

        schema.field_descriptions = self.field_descriptions;
        schema.field_interest_categories = self.field_interest_categories;
        schema.field_types = self.field_types;
        schema.field_hashes = self.field_hashes;
        schema.field_declarations = self.field_declarations;
        schema.field_versions = self.field_versions;
        schema.identity_hash = self.identity_hash;
        schema.identity_hash_algo_version = self.identity_hash_algo_version;
        schema.superseded_by = self.superseded_by;
        schema.owner_app_id = self.owner_app_id;
        schema.alternate_keys = self.alternate_keys;
        schema.record_keys = self.record_keys;
        schema.file_required = self.file_required;

        // Recompute identity_hash if owner_app_id is present and the helper
        // didn't carry an explicit one. owner_app_id participates in
        // identity_hash; a deserialized schema must reflect that.
        if schema.identity_hash.is_none() && (schema.owner_app_id.is_some() || schema.file_required)
        {
            schema.compute_identity_hash();
        }

        schema.validate_definition()?;
        Ok(schema)
    }
}

impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper: DeclarativeSchemaDefinitionHelper =
            serde::Deserialize::deserialize(deserializer)?;
        helper.into_definition().map_err(serde::de::Error::custom)
    }
}

fn normalize_fields(fields: Option<serde_json::Value>) -> Result<Option<Vec<String>>, String> {
    Ok(match fields {
        None => None,
        Some(val) => {
            if let Some(arr) = val.as_array() {
                let mut out: Vec<String> = Vec::new();
                for item in arr {
                    if let Some(s) = item.as_str() {
                        out.push(s.to_string());
                    } else {
                        return Err("Invalid fields array; expected strings".to_string());
                    }
                }
                Some(out)
            } else if let Some(obj) = val.as_object() {
                let mut names: Vec<String> = obj.keys().cloned().collect();
                names.sort();
                Some(names)
            } else {
                return Err("Invalid fields; expected array or object map".to_string());
            }
        }
    })
}
