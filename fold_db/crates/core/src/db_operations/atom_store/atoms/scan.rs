use super::*;

impl AtomStore {
    /// Read items under exactly one storage scope; no bare fallback.
    pub(crate) async fn scan_exact_storage_prefix<T: DeserializeOwned + Send + Sync>(
        &self,
        base_prefix: &str,
        storage_prefix: Option<&str>,
        err_label: &str,
    ) -> Result<Vec<(String, T)>, SchemaError> {
        let prefix = build_storage_key(storage_prefix, base_prefix);
        self.main_store
            .scan_items_with_prefix(&prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{err_label}: {e}")))
    }
}
