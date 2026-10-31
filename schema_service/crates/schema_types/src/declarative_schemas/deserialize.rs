use super::{DeclarativeSchemaDefinition, SchemaSource};
use crate::schema_type::DeclarativeSchemaType;
use crate::DataClassification;
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
    #[serde(default, rename = "independent_fields")]
    owned_values: bool,
    // Accept either an array of strings or an object map and normalize later
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transform_fields: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    field_molecule_uuids: Option<HashMap<String, String>>,
    #[serde(default)]
    field_classifications: HashMap<String, Vec<String>>,
    #[serde(default)]
    field_descriptions: HashMap<String, String>,
    #[serde(default)]
    field_data_classifications: HashMap<String, DataClassification>,
    #[serde(default)]
    field_interest_categories: HashMap<String, String>,
    #[serde(default)]
    ref_fields: HashMap<String, String>,
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
    trust_domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    owner_app_id: Option<String>,
    #[serde(default)]
    source: SchemaSource,
    // Preserve unrelated unknown-field behavior, but refuse removed storage
    // configuration even when its JSON value is null. Stored Schema Service
    // records can still carry `field_mappers` from the retired cross-schema
    // value sharing; that key lands here and has no effect.
    #[serde(flatten)]
    unknown: HashMap<String, serde::de::IgnoredAny>,
}

impl DeclarativeSchemaDefinitionHelper {
    /// Build the definition through the constructor, then restore every
    /// persisted field the constructor does not derive.
    fn into_definition(self) -> Result<DeclarativeSchemaDefinition, String> {
        for name in ["molecule_uuid", "record_mapper"] {
            if self.unknown.contains_key(name) {
                return Err(format!(
                    "schema-level '{name}' is not supported; each field uses its own molecule"
                ));
            }
        }
        let normalized_fields = normalize_fields(self.fields)?;

        // Determine schema_type if omitted
        let normalized_schema_type = match (&self.schema_type, &self.key) {
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
        };

        // Use the constructor to create the actual struct with generated mappings
        let mut schema = DeclarativeSchemaDefinition::new(
            self.name,
            normalized_schema_type,
            self.key,
            normalized_fields,
            self.transform_fields,
        );

        // Preserve descriptive_name and field_molecule_uuids from deserialization
        schema.descriptive_name = self.descriptive_name;
        schema.purpose_statement = self.purpose_statement;
        schema.field_molecule_uuids = self.field_molecule_uuids;

        // Merge classifications from helper
        for (field_name, classifications) in self.field_classifications {
            schema
                .field_classifications
                .insert(field_name, classifications);
        }

        // Preserve field_descriptions, field_data_classifications, field_interest_categories, ref_fields, field_types and identity_hash
        schema.field_descriptions = self.field_descriptions;
        schema.field_data_classifications = self.field_data_classifications;
        schema.field_interest_categories = self.field_interest_categories;
        schema.ref_fields = self.ref_fields;
        schema.field_types = self.field_types;
        schema.field_hashes = self.field_hashes;
        schema.field_declarations = self.field_declarations;
        schema.field_versions = self.field_versions;
        schema.identity_hash = self.identity_hash;
        schema.identity_hash_algo_version = self.identity_hash_algo_version;
        schema.superseded_by = self.superseded_by;
        schema.trust_domain = self.trust_domain;
        schema.owner_app_id = self.owner_app_id;
        schema.source = self.source;
        schema.alternate_keys = self.alternate_keys;
        schema.owned_values = self.owned_values;

        // Recompute identity_hash if owner_app_id is present and the helper
        // didn't carry an explicit one. owner_app_id participates in
        // identity_hash; a deserialized schema must reflect that.
        if schema.identity_hash.is_none() && schema.owner_app_id.is_some() {
            schema.compute_identity_hash();
        }

        schema.validate_definition()?;
        Ok(schema)
    }
}

// Custom deserializer for DeclarativeSchemaDefinition that uses the constructor
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
                // Expect array of strings
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
                // Accept object map and use keys as field names
                let mut names: Vec<String> = obj.keys().cloned().collect();
                names.sort();
                Some(names)
            } else {
                return Err("Invalid fields; expected array or object map".to_string());
            }
        }
    })
}
