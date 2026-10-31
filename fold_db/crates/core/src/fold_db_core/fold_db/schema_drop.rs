use super::FoldDB;
use crate::error::FoldDbResult;

impl FoldDB {
    pub async fn drop_schema_and_bindings(&self, schema_name: &str) -> FoldDbResult<bool> {
        self.delete_catalog_rows_for_drop(schema_name).await?;
        Ok(self.schema_manager().drop_schema(schema_name).await?)
    }

    /// Drop every schema that `owner_app` owns, one at a time. Returns the names
    /// that were installed and are dropped, sorted.
    pub async fn drop_schemas_owned_by_with_bindings(
        &self,
        owner_app: &str,
    ) -> FoldDbResult<Vec<String>> {
        let mut dropped = Vec::new();
        for name in self.schema_manager().schema_names_owned_by(owner_app)? {
            if self.drop_schema_and_bindings(&name).await? {
                dropped.push(name);
            }
        }
        dropped.sort();
        Ok(dropped)
    }
}
