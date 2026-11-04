use super::*;
use crate::db_operations::record_store::RecordStore;

use crate::schema::SchemaError;
use crate::storage::format::history as format;

impl RecordStore {
    pub async fn field_history_policy(
        &self,
        field: &str,
    ) -> Result<FieldHistoryPolicy, SchemaError> {
        let key = format::policy_key(field);
        Ok(self.main_store.get_item(&key).await?.unwrap_or_default())
    }

    pub async fn set_field_history_policy(
        &self,
        field: &str,
        policy: FieldHistoryPolicy,
    ) -> Result<(), SchemaError> {
        let encoded = format::policy_batch(field, policy)?;
        if policy != FieldHistoryPolicy::Off {
            // Wake before the write: cancellation after native acceptance must
            // not strand a committed policy without its expiry worker. The
            // worker's existing format guard waits for visible activation.
            self.history_activation.notify_one();
        }
        self.in_background_scope(async {
            encoded
                .stage(
                    self.main_store.inner().as_ref(),
                    &self.namespaced_store.disk_pack_runtime(),
                )
                .await?;
            Ok(())
        })
        .await
    }
}
