//! Bind a store-local revision to the mutation identity shared by both stores.

use super::MutationManager;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;

/// The same record version expressed in local and publisher terms.
pub struct SharedRecordRevision {
    /// Conditional-write token in this store.
    pub local_revision: String,
    /// Mutation ID carried in the publisher's accepted record.
    pub mutation_id: String,
}

impl MutationManager {
    /// Read the native record and its publisher mutation ID from one committed view.
    pub async fn shared_record_revision(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<SharedRecordRevision>, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let preimage = self.replay_preimage(address.clone()).await?;
                let Some(record) = preimage.filter(|row| row.is_live()) else {
                    return Ok(None);
                };
                let local_revision = record.stamp.revision();
                let mutation_id = self
                    .db_ops
                    .metadata()
                    .shared_record_mutation(&address, &local_revision)
                    .await?;
                Ok(mutation_id.map(|mutation_id| SharedRecordRevision {
                    local_revision,
                    mutation_id,
                }))
            })
            .await
    }
}
