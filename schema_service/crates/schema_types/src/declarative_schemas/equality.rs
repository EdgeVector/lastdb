use super::DeclarativeSchemaDefinition;

// Manual PartialEq implementation that excludes derived/runtime metadata
impl PartialEq for DeclarativeSchemaDefinition {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.descriptive_name == other.descriptive_name
            && self.purpose_statement == other.purpose_statement
            && self.schema_type == other.schema_type
            && self.key == other.key
            && self.fields == other.fields
            && self.transform_fields == other.transform_fields
            && self.field_mappers == other.field_mappers
            && self.record_mapper == other.record_mapper
            && self.molecule_uuid == other.molecule_uuid
            && self.hash == other.hash
            && self.field_molecule_uuids == other.field_molecule_uuids
            && self.field_classifications == other.field_classifications
            && self.field_descriptions == other.field_descriptions
            && self.field_data_classifications == other.field_data_classifications
            && self.field_interest_categories == other.field_interest_categories
            && self.ref_fields == other.ref_fields
            && self.field_types == other.field_types
            && self.field_hashes == other.field_hashes
            && self.field_declarations == other.field_declarations
            && self.field_versions == other.field_versions
            && self.identity_hash == other.identity_hash
            && self.superseded_by == other.superseded_by
            && self.trust_domain == other.trust_domain
            && self.owner_app_id == other.owner_app_id
            && self.source == other.source
        // Exclude inputs_schema_fields, source_schemas, and hash mappings
        // These are derived/runtime state and don't affect schema identity
    }
}
