use super::{DeclarativeSchemaDefinition, FieldMapper, RecordMapper, SchemaSource};
use crate::schema_type::DeclarativeSchemaType;
use crate::DataClassification;
use crate::FieldValueType;
use crate::KeyConfig;
use std::collections::HashMap;

// Custom deserializer for DeclarativeSchemaDefinition that uses the constructor
impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Define a temporary struct for deserialization
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
            // Accept either an array of strings or an object map and normalize later
            #[serde(skip_serializing_if = "Option::is_none")]
            fields: Option<serde_json::Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            transform_fields: Option<HashMap<String, String>>,
            #[serde(skip_serializing_if = "Option::is_none", default)]
            field_mappers: Option<HashMap<String, FieldMapper>>,
            #[serde(skip_serializing_if = "Option::is_none", default)]
            record_mapper: Option<RecordMapper>,
            #[serde(skip_serializing_if = "Option::is_none", default)]
            molecule_uuid: Option<String>,
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
            #[serde(skip_serializing_if = "Option::is_none", default)]
            trust_domain: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none", default)]
            owner_app_id: Option<String>,
            #[serde(default)]
            source: SchemaSource,
        }

        // Deserialize into the helper struct
        let helper: DeclarativeSchemaDefinitionHelper =
            serde::Deserialize::deserialize(deserializer)?;

        // Normalize fields into Option<Vec<String>> supporting multiple shapes
        let normalized_fields: Option<Vec<String>> = match helper.fields {
            None => None,
            Some(val) => {
                if let Some(arr) = val.as_array() {
                    // Expect array of strings
                    let mut out: Vec<String> = Vec::new();
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            out.push(s.to_string());
                        } else {
                            return Err(serde::de::Error::custom(
                                "Invalid fields array; expected strings",
                            ));
                        }
                    }
                    Some(out)
                } else if let Some(obj) = val.as_object() {
                    // Accept object map and use keys as field names
                    let mut names: Vec<String> = obj.keys().cloned().collect();
                    names.sort();
                    Some(names)
                } else {
                    return Err(serde::de::Error::custom(
                        "Invalid fields; expected array or object map",
                    ));
                }
            }
        };

        // Determine schema_type if omitted
        let normalized_schema_type = match (&helper.schema_type, &helper.key) {
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
        let mut schema = Self::new(
            helper.name,
            normalized_schema_type,
            helper.key,
            normalized_fields,
            helper.transform_fields,
            helper.field_mappers,
        );

        // Preserve descriptive_name and field_molecule_uuids from deserialization
        schema.descriptive_name = helper.descriptive_name;
        schema.purpose_statement = helper.purpose_statement;
        schema.field_molecule_uuids = helper.field_molecule_uuids;
        schema.record_mapper = helper.record_mapper;
        schema.molecule_uuid = helper.molecule_uuid;

        // Merge classifications from helper
        for (field_name, classifications) in helper.field_classifications {
            schema
                .field_classifications
                .insert(field_name, classifications);
        }

        // Preserve field_descriptions, field_data_classifications, field_interest_categories, ref_fields, field_types and identity_hash
        schema.field_descriptions = helper.field_descriptions;
        schema.field_data_classifications = helper.field_data_classifications;
        schema.field_interest_categories = helper.field_interest_categories;
        schema.ref_fields = helper.ref_fields;
        schema.field_types = helper.field_types;
        schema.field_hashes = helper.field_hashes;
        schema.field_declarations = helper.field_declarations;
        schema.field_versions = helper.field_versions;
        schema.identity_hash = helper.identity_hash;
        schema.identity_hash_algo_version = helper.identity_hash_algo_version;
        schema.trust_domain = helper.trust_domain;
        schema.owner_app_id = helper.owner_app_id;
        schema.source = helper.source;

        // Recompute identity_hash if owner_app_id is present and the helper
        // didn't carry an explicit one. owner_app_id participates in
        // identity_hash; a deserialized schema must reflect that.
        if schema.identity_hash.is_none() && schema.owner_app_id.is_some() {
            schema.compute_identity_hash();
        }

        Ok(schema)
    }
}
