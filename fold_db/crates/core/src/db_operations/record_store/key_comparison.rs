use super::*;
use crate::schema::types::key_comparison::KeyComparisonContext;
use crate::schema::SchemaError;

impl RecordStore {
    /// Capture canonical key order for `schema`.
    pub fn key_comparison_context(
        &self,
        schema: &crate::schema::Schema,
    ) -> Result<Arc<KeyComparisonContext>, SchemaError> {
        Ok(Arc::new(KeyComparisonContext::for_schema(schema)?))
    }
}
