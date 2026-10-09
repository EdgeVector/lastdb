use super::DeclarativeSchemaDefinition;

// The wire parsing (omitted `schema_type`, array-or-map `fields`, identity
// recompute) lives once in `schema_types`. This type only adds runtime state,
// which `From<schema_types::DeclarativeSchemaDefinition>` rebuilds.
impl<'de> serde::Deserialize<'de> for DeclarativeSchemaDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        schema_types::DeclarativeSchemaDefinition::deserialize(deserializer).map(Self::from)
    }
}
