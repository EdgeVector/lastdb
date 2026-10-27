use super::*;
use crate::schema::types::key_comparison::{KeyComparisonContext, KeyOrderVersion};
use crate::schema::SchemaError;

impl AtomStore {
    /// Capture actual molecule bindings and formats, not environment defaults.
    pub async fn key_comparison_context(
        &self,
        schema: &crate::schema::Schema,
    ) -> Result<Arc<KeyComparisonContext>, SchemaError> {
        if let Some(store) = &self.molecule_keys {
            store.load_schema(schema).await?;
        }
        let mut formats = Vec::new();
        let order = if self.typed_ranges {
            KeyOrderVersion::TypedScalarV1
        } else {
            KeyOrderVersion::LegacyUtf8V1
        };
        for name in schema.runtime_field_names() {
            let field = &schema.runtime_fields[&name];
            if let Some(molecule) = field.common().molecule_uuid() {
                let codec = self.key_codec_for_molecule(molecule)?;
                if codec.key_order() != order {
                    return Err(SchemaError::InvalidData(
                        "the molecule order differs from its node contract".into(),
                    ));
                }
                formats.push((name, molecule.clone(), codec.comparison_format()));
            }
        }
        Ok(Arc::new(KeyComparisonContext::for_schema(
            schema, &formats, order,
        )?))
    }
}
