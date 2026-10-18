//! Complete schema state after a retried native purge.

use super::*;

impl MutationManager {
    pub(super) async fn replay_purge_finalize(&self, schema_name: &str) -> Result<(), SchemaError> {
        let started = std::time::Instant::now();
        let result = async {
            let mut schema = self
                .schema_manager
                .get_schema_metadata(schema_name)?
                .ok_or_else(|| {
                    SchemaError::InvalidData(format!(
                        "Schema '{schema_name}' not found for purge finalization replay"
                    ))
                })?;
            schema.sync_molecule_uuids();
            self.db_ops.store_schema(schema_name, &schema).await?;
            self.schema_manager.load_schema_internal(schema).await?;
            self.db_ops.flush().await.map_err(|error| {
                SchemaError::InvalidData(format!(
                    "Flush failed after purge finalization replay: {error}"
                ))
            })
        }
        .await;
        crate::request_phases::add_phase(
            crate::request_phases::RequestPhase::PurgeFinalize,
            started.elapsed(),
        );
        result
    }
}
