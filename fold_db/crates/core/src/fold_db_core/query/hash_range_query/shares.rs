use super::HashRangeQueryProcessor;
use crate::schema::SchemaError;
mod receivers;

/// Resolve the existing receiver controls before native query service starts.
/// Failure leaves them unknown, so a later query uses the ordinary read path.
pub(crate) async fn preload_receiver_controls(
    ops: &crate::db_operations::DbOperations,
) -> crate::storage::error::StorageResult<()> {
    use futures::FutureExt;

    let scope = ops.read_scope().await?;
    scope
        .run_result(
            async {
                receivers::namespaces(ops).await.map(|_| ()).map_err(|_| {
                    crate::storage::StorageError::BackendError(
                        "native receiver control preload failed".into(),
                    )
                })
            }
            .boxed(),
        )
        .await
}

impl HashRangeQueryProcessor {
    /// Look up active `ShareSubscription`s and return a deduped list of
    /// `from:{sender_hash}` namespace prefixes to scan. Returns an empty
    /// vector if the store has no subscriptions.
    ///
    /// Subscriptions with an unparseable `share_prefix` are logged and
    /// skipped (never silently). Inactive subscriptions are excluded.
    pub(super) async fn collect_received_from_namespaces(
        &self,
    ) -> Result<Vec<String>, SchemaError> {
        receivers::namespaces(&self.db_ops).await.map_err(|error| {
            SchemaError::InvalidData(format!(
                "the committed inbound receiver namespaces are unavailable: {error}"
            ))
        })
    }

    /// Alternate entries index only records written on this node. A sender
    /// need not grant every key field, so received records have no entries
    /// and an alternate read would omit them. Refuse while any sender is active.
    pub(crate) async fn refuse_received_data(&self) -> Result<(), SchemaError> {
        if self.collect_received_from_namespaces().await?.is_empty() {
            return Ok(());
        }
        Err(SchemaError::InvalidData(
            "alternate keys do not cover data received from other users; \
             read this schema by its primary key"
                .into(),
        ))
    }
}
