use super::*;

impl AtomStore {
    /// Return whether the physical v1 plane still has any live row.
    ///
    /// This is a one-row range probe. It does not enumerate the plane.
    pub async fn atom_ref_v1_keys_remain(&self) -> Result<bool, SchemaError> {
        let namespaced = self.namespaced_store.as_ref().ok_or_else(|| {
            SchemaError::InvalidData(
                "legacy atom reverse-edge drain requires the owner namespace store".to_string(),
            )
        })?;
        let v1 = namespaced
            .open_namespace("atom_ref_edges")
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("open legacy atom reverse-edge plane: {error}"))
            })?;
        v1.scan_range_paged(b"", ATOM_REF_V1_DRAIN_END.as_bytes(), 1)
            .await
            .map(|rows| !rows.is_empty())
            .map_err(|error| {
                SchemaError::InvalidData(format!("probe legacy atom reverse-edge rows: {error}"))
            })
    }
}
