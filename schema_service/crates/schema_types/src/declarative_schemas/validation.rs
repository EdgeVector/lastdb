use super::DeclarativeSchemaDefinition;

impl DeclarativeSchemaDefinition {
    /// Shared admission checks for wire and programmatic definitions.
    pub fn validate_definition(&self) -> Result<(), String> {
        self.validate_field_names()?;
        self.validate_key_field_types()?;
        self.validate_alternate_keys()
    }

    /// Reject reserved names and characters that make field identities ambiguous.
    ///
    /// Schema identity separates field names with commas and key parts with
    /// colons. Keep that established hash format while refusing names that
    /// can escape those boundaries. Dotted paths and ordinary Unicode names
    /// remain valid at this shared wire boundary.
    pub fn validate_field_name(name: &str) -> Result<(), String> {
        if matches!(name, "\u{1f}record" | "__record") {
            return Err(format!(
                "field name {name:?} is reserved and is not supported"
            ));
        }
        if name.is_empty()
            || name.chars().any(|character| {
                character.is_whitespace()
                    || character.is_control()
                    || matches!(character, ',' | ':')
            })
        {
            return Err(format!(
                "field name {name:?} must be nonempty and cannot contain whitespace, control characters, commas, or colons"
            ));
        }
        Ok(())
    }

    /// Malformed field names cannot become ordinary field molecules.
    /// Validate programmatic definitions as well as their JSON wire form.
    pub fn validate_field_names(&self) -> Result<(), String> {
        let mut names = self
            .fields
            .iter()
            .flatten()
            .map(String::as_str)
            .chain(
                self.transform_fields
                    .iter()
                    .flat_map(|map| map.keys())
                    .map(String::as_str),
            )
            .chain(
                self.field_molecule_uuids
                    .iter()
                    .flat_map(|map| map.keys())
                    .map(String::as_str),
            )
            .chain(self.field_classifications.keys().map(String::as_str))
            .chain(self.field_descriptions.keys().map(String::as_str))
            .chain(self.field_data_classifications.keys().map(String::as_str))
            .chain(self.field_interest_categories.keys().map(String::as_str))
            .chain(self.ref_fields.keys().map(String::as_str))
            .chain(self.field_types.keys().map(String::as_str))
            .chain(self.field_hashes.keys().map(String::as_str))
            .chain(self.field_declarations.keys().map(String::as_str))
            .chain(self.field_versions.keys().map(String::as_str))
            .chain(self.key.iter().filter_map(|key| key.hash_field.as_deref()))
            .chain(self.key.iter().filter_map(|key| key.range_field.as_deref()));
        names.try_for_each(Self::validate_field_name)
    }
}
