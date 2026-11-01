use super::*;
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;

impl AtomStore {
    pub async fn field_history_policy(
        &self,
        field: &str,
        scope: Option<&str>,
    ) -> Result<FieldHistoryPolicy, SchemaError> {
        let key = build_storage_key(scope, &format!("ahp:v1:{field}"));
        Ok(self.main_store.get_item(&key).await?.unwrap_or_default())
    }

    pub async fn set_field_history_policy(
        &self,
        field: &str,
        policy: FieldHistoryPolicy,
        scope: Option<&str>,
    ) -> Result<(), SchemaError> {
        if let FieldHistoryPolicy::Ttl { seconds } = policy {
            if seconds == 0 || seconds.checked_mul(1_000_000_000).is_none() {
                return Err(SchemaError::InvalidData(
                    "history TTL must be positive and fit nanoseconds".into(),
                ));
            }
        }
        if field.len() != 64 || !field.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(SchemaError::InvalidData(
                "history needs a canonical field identity".into(),
            ));
        }
        if policy != FieldHistoryPolicy::Off {
            // Wake before the write: cancellation after native acceptance must
            // not strand a committed policy without its expiry worker. The
            // worker's existing format guard waits for visible activation.
            self.history_activation.notify_one();
        }
        self.in_background_scope(async {
            let mut writes = vec![crate::storage::KvMutation::put(
                build_storage_key(scope, &format!("ahp:v1:{field}")).into_bytes(),
                serde_json::to_vec(&policy).map_err(|e| SchemaError::InvalidData(e.to_string()))?,
            )];
            if policy != FieldHistoryPolicy::Off {
                writes.push(crate::storage::KvMutation::put(
                    Self::CURRENT_KEY_FORMAT_KEY.as_bytes().to_vec(),
                    crate::atom::molecule_key_codec::HISTORY_KEY_FORMAT.to_vec(),
                ));
            }
            self.main_store.inner().batch_mutate(writes).await?;
            Ok(())
        })
        .await
    }
}
