//! Read the stamp that won the fields of one record.

use super::MutationManager;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;
use crate::storage::disk_pack::transaction;
use crate::storage::format::metadata::{ReplayStamp, ReplayTarget};

impl MutationManager {
    /// The stamp of the newest write of the fields of the record at `key` in `schema`,
    /// or `None` when no captured or applied write has one. `key` is the primary key: the
    /// hash, and the range in a range schema. An unknown or blocked schema has none.
    pub async fn replay_winner_of(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<ReplayStamp>, SchemaError> {
        transaction::scope(self.db_ops.native_runtime(), async {
            let Some(schema) = self.resolve_replay_schema(schema).await? else {
                return Ok(None);
            };
            let address = self.replay_record_address(&schema, key)?;
            self.db_ops
                .metadata()
                .replay_winner(ReplayTarget::Record(&address))
                .await
        })
        .await
    }
}
