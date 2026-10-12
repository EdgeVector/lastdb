use super::TEMPLATE_OWNER_APP_ID;
use schema_types::DataClassification;
use schema_types::DeclarativeSchemaType as SchemaType;
use schema_types::FieldValueType;
use schema_types::KeyConfig;
use schema_types::{Schema, SchemaSource};

/// Build a `Schema` value with the standard configuration:
/// - Hash schema_type by default (override where needed)
/// - descriptive_name = name (we keep them aligned for readability)
/// - field_types populated from the caller
/// - field_descriptions populated from the caller (required by schema service)
/// - sensitivity defaults to 0 unless specified per field
/// - identity_hash computed before return
pub(super) struct SchemaBuilder {
    name: &'static str,
    schema_type: SchemaType,
    key: KeyConfig,
    source: SchemaSource,
    owner_app_id: Option<&'static str>,
    purpose_statement: Option<&'static str>,
    fields: Vec<(&'static str, FieldValueType, &'static str, u8)>, // (name, type, desc, sensitivity)
}

impl SchemaBuilder {
    pub(super) fn hash_range(
        name: &'static str,
        hash_field: &'static str,
        range_field: &'static str,
    ) -> Self {
        Self {
            name,
            schema_type: SchemaType::HashRange,
            key: KeyConfig::new(Some(hash_field.to_string()), Some(range_field.to_string())),
            source: SchemaSource::SystemSeed,
            owner_app_id: None,
            purpose_statement: None,
            fields: Vec::new(),
        }
    }

    pub(super) fn single_template(name: &'static str, purpose_statement: &'static str) -> Self {
        Self {
            name,
            schema_type: SchemaType::Single,
            key: KeyConfig::new(None, None),
            source: SchemaSource::StarterSeed,
            owner_app_id: Some(TEMPLATE_OWNER_APP_ID),
            purpose_statement: Some(purpose_statement),
            fields: Vec::new(),
        }
    }

    pub(super) fn sensitive_field(
        mut self,
        name: &'static str,
        ty: FieldValueType,
        description: &'static str,
        sensitivity: u8,
    ) -> Self {
        self.fields.push((name, ty, description, sensitivity));
        self
    }

    pub(super) fn build(self) -> Schema {
        let field_names: Vec<String> = self.fields.iter().map(|f| f.0.to_string()).collect();

        let mut schema = Schema::new(
            self.name.to_string(),
            self.schema_type,
            Some(self.key),
            Some(field_names),
            None,
            None,
        );

        schema.descriptive_name = Some(self.name.to_string());
        schema.source = self.source;
        schema.owner_app_id = self.owner_app_id.map(str::to_string);
        schema.purpose_statement = self.purpose_statement.map(str::to_string);

        for (name, ty, description, sensitivity) in self.fields {
            schema.field_types.insert(name.to_string(), ty);
            schema
                .field_descriptions
                .insert(name.to_string(), description.to_string());
            schema.field_data_classifications.insert(
                name.to_string(),
                DataClassification {
                    sensitivity_level: sensitivity,
                    data_domain: "general".to_string(),
                },
            );
            // Default classification so the schema service doesn't reject on
            // the classification-validation path used elsewhere in the codebase.
            let class = if matches!(schema.field_types.get(name), Some(FieldValueType::Integer)) {
                "number"
            } else if matches!(schema.field_types.get(name), Some(FieldValueType::Boolean)) {
                "boolean"
            } else {
                "word"
            };
            schema
                .field_classifications
                .insert(name.to_string(), vec![class.to_string()]);
        }

        schema.compute_identity_hash();
        schema
    }
}
