use super::HashRangeQueryProcessor;
use crate::schema::types::field::{FieldKind, FieldValue, FieldVariant, HashRangeFilter};
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
mod receivers;

/// Resolve the existing receiver controls before native query service starts.
/// Failure leaves them unknown, so a later query uses the ordinary read path.
pub(crate) async fn preload_receiver_controls(
    ops: &crate::db_operations::DbOperations,
) -> crate::storage::error::StorageResult<()> {
    use crate::storage::disk_pack::PackReadScope;
    use futures::FutureExt;

    let store = ops.namespaced_store();
    if store.disk_pack_runtime().is_none() {
        return Ok(());
    }
    let Some(scope) = PackReadScope::for_store(&store).await? else {
        return Ok(());
    };
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

    /// Resolve a field's values from a specific storage namespace by
    /// temporarily rewriting the field's `storage_prefix`. The field's
    /// structure is cloned with molecule data stripped so the caller's schema
    /// state is not mutated.
    pub(super) async fn resolve_field_from_namespace(
        &self,
        field: &FieldVariant,
        namespace: &str,
        filter: Option<HashRangeFilter>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
            runtime
                .preload_current_field(std::sync::Arc::clone(&self.db_ops), field, Some(namespace))
                .await?;
        }
        let mut cloned = field.cloned_without_molecule();
        cloned
            .common_mut()
            .set_storage_prefix(Some(namespace.to_string()));
        cloned
            .resolve_value(&self.db_ops, filter, None, include_tombstones)
            .await
    }

    pub(super) fn stamp_shared_writer(namespace: &str, shared: &mut HashMap<KeyValue, FieldValue>) {
        // Respect any existing molecule-level writer_pubkey and fall back to
        // the namespace sender hash only when unset.
        let sender_hash = Self::sender_hash_from_namespace(namespace);
        for fv in shared.values_mut() {
            if fv.writer_pubkey.is_none() {
                fv.writer_pubkey = Some(sender_hash.clone());
            }
        }
    }

    pub(super) fn sort_keys_for_field(field: &FieldVariant, keys: &mut [KeyValue]) {
        match field.kind {
            FieldKind::Hash => keys.sort_by(|a, b| {
                a.hash
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.hash.as_deref().unwrap_or(""))
            }),
            FieldKind::Range => keys.sort_by(|a, b| {
                a.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.range.as_deref().unwrap_or(""))
            }),
            FieldKind::HashRange | FieldKind::Single => keys.sort_by(|a, b| {
                a.range
                    .as_deref()
                    .unwrap_or("")
                    .cmp(b.range.as_deref().unwrap_or(""))
                    .then_with(|| {
                        a.hash
                            .as_deref()
                            .unwrap_or("")
                            .cmp(b.hash.as_deref().unwrap_or(""))
                    })
            }),
        }
    }

    /// Extract the sender hash from a `from:{sender_hash}` namespace prefix.
    fn sender_hash_from_namespace(namespace: &str) -> String {
        namespace
            .strip_prefix("from:")
            .unwrap_or(namespace)
            .split(':')
            .next()
            .unwrap_or(namespace)
            .to_string()
    }
}
