//! Body existence checks for the current dangling-tip repair.

use super::*;

impl AtomStore {
    pub(super) async fn repair_exists(
        &self,
        keys: &[String],
        context: &str,
    ) -> Result<Vec<bool>, SchemaError> {
        self.raw()
            .exists_items(keys)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("{context}: {e}")))
    }

    pub(super) async fn exists_with_authority(
        &self,
        keys: &[String],
        context: &str,
        authority: ExistsAuthority,
    ) -> Result<Vec<bool>, SchemaError> {
        match authority {
            ExistsAuthority::Cached => self.repair_exists(keys, context).await,
            ExistsAuthority::Live => {
                // Preserve the current final check against live body reads.
                // Independent keys share one bounded parallel operation.
                futures::future::try_join_all(keys.iter().map(|key| async move {
                    self.raw()
                        .exists_item(key)
                        .await
                        .map_err(|e| SchemaError::InvalidData(format!("{context}: {e}")))
                }))
                .await
            }
        }
    }
}
