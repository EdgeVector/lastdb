use super::*;
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::SchemaError;

impl RecordStore {
    /// Load the record-space and alternate bundles, then capture key order.
    pub async fn key_comparison_context(
        &self,
        schema: &crate::schema::Schema,
    ) -> Result<Arc<KeyComparisonContext>, SchemaError> {
        if let Some(store) = &self.field_keys {
            store.load_schema(schema).await?;
        }
        Ok(Arc::new(KeyComparisonContext::for_schema(schema)?))
    }
}
