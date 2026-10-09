use super::{DeclarativeSchemaDefinition, FieldMapper, SchemaSource};
use crate::schema_type::DeclarativeSchemaType;
use crate::FieldValueType;
use crate::KeyConfig;
use std::collections::HashMap;

impl DeclarativeSchemaDefinition {
    /// Creates a new DeclarativeSchemaDefinition and generates all hash mappings.
    ///
    /// Pure wire form — no `runtime_fields` (those live on fold_db's wrapper).
    pub fn new(
        name: String,
        schema_type: DeclarativeSchemaType,
        key: Option<KeyConfig>,
        fields: Option<Vec<String>>,
        transform_fields: Option<HashMap<String, String>>,
        field_mappers: Option<HashMap<String, FieldMapper>>,
    ) -> Self {
        let mut schema = Self {
            name,
            descriptive_name: None,
            purpose_statement: None,
            schema_type,
            key,
            fields,
            transform_fields,
            field_mappers,
            record_mapper: None,
            molecule_uuid: None,
            hash: None,
            field_molecule_uuids: None,
            field_classifications: HashMap::new(),
            field_descriptions: HashMap::new(),
            field_data_classifications: HashMap::new(),
            field_interest_categories: HashMap::new(),
            ref_fields: HashMap::new(),
            field_types: HashMap::new(),
            field_hashes: HashMap::new(),
            field_declarations: HashMap::new(),
            field_versions: HashMap::new(),
            identity_hash: None,
            identity_hash_algo_version: None,
            superseded_by: None,
            trust_domain: None,
            owner_app_id: None,
            source: SchemaSource::User,
            inputs_schema_fields: Vec::new(),
            source_schemas: Vec::new(),
            field_to_hash_code: HashMap::new(),
            hash_to_code: HashMap::new(),
        };

        schema.regenerate_metadata();
        schema
    }

    /// Get the declared type for a field. Returns `Any` if no type is declared.
    pub fn get_field_type(&self, field_name: &str) -> &FieldValueType {
        static ANY: FieldValueType = FieldValueType::Any;
        self.field_types.get(field_name).unwrap_or(&ANY)
    }

    pub fn field_mappers(&self) -> Option<&HashMap<String, FieldMapper>> {
        self.field_mappers.as_ref()
    }

    pub fn record_mapper(&self) -> Option<&super::RecordMapper> {
        self.record_mapper.as_ref()
    }

    /// Mint/attach `field_hashes` from each field's description, type, and version.
    ///
    /// Uses existing `field_hashes` entries when present; otherwise computes from
    /// `field_descriptions` / `field_types` / `field_versions` (version default 1).
    /// Empty description is allowed (still hashed).
    pub fn ensure_field_hashes(&mut self) {
        let names: Vec<String> = self
            .fields
            .clone()
            .unwrap_or_default()
            .into_iter()
            .chain(self.field_types.keys().cloned())
            .chain(self.field_descriptions.keys().cloned())
            .collect();
        let mut seen = std::collections::HashSet::new();
        for name in names {
            if !seen.insert(name.clone()) {
                continue;
            }
            if self.field_hashes.contains_key(&name) {
                continue;
            }
            let description = self
                .field_descriptions
                .get(&name)
                .cloned()
                .unwrap_or_default();
            let field_type = self.get_field_type(&name).clone();
            let version = *self.field_versions.get(&name).unwrap_or(&1);
            let hash = crate::compute_field_hash(&name, &description, &field_type, version);
            self.field_versions.entry(name.clone()).or_insert(version);
            self.field_hashes.insert(name, hash);
        }
    }
}
