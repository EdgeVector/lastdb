//! Compatibility cleanup for field history already stored in a home.

mod key_counts;
mod records;
mod release;
pub(crate) use crate::storage::format::history::queue;

impl super::RecordStore {
    pub(crate) async fn has_legacy_field_history(
        &self,
    ) -> Result<bool, crate::schema::SchemaError> {
        let setting = self
            .main_store
            .inner()
            .get(crate::storage::format::control::FIELD_HISTORY_KEY.as_bytes())
            .await
            .map_err(|e| crate::schema::SchemaError::InvalidData(e.to_string()))?;
        Ok(setting.as_deref() == Some(crate::storage::format::control::FIELD_HISTORY_ON))
    }
}
