//! Restore scopes stay write-blocked until exact counter validation succeeds.

use super::*;
use futures::{stream, StreamExt};
use std::time::Instant;

const RESTORE_NAMESPACE_CONCURRENCY: usize = 4;

impl Snapshot {
    pub async fn restore(&self, store: &dyn NamespacedStore) -> SyncResult<SnapshotRestoreReport> {
        self.restore_checked(store, store).await
    }

    /// Use the logical reader of this same native home for ciphertext snapshots.
    pub async fn restore_checked(
        &self,
        store: &dyn NamespacedStore,
        logical: &dyn NamespacedStore,
    ) -> SyncResult<SnapshotRestoreReport> {
        self.validate_full_format_markers()?;
        restore_guard::run(self, store, logical, self.restore_inner(store)).await
    }

    pub async fn restore_scoped_to_prefixes(
        &self,
        store: &dyn NamespacedStore,
        prefixes: &[String],
    ) -> SyncResult<SnapshotRestoreReport> {
        restore_guard::run(
            self,
            store,
            store,
            self.restore_scoped_inner(store, prefixes),
        )
        .await
    }

    /// Restore a snapshot into a NamespacedStore.
    ///
    /// Personal bootstrap restore replaces **non-org** keys from the snapshot
    /// and leaves local org-scoped rows alone:
    /// - Clear phase deletes only non-org keys (never mass-deletes org rows).
    /// - Write phase still **skips** org-scoped keys from the snapshot payload
    ///   with a loud count — personal snapshots must not rehydrate org-E2E
    ///   ciphertext under the personal single-provider seam (incident
    ///   2026-07-13).
    ///
    /// Org-scoped storage keys: `{storage_prefix}:…`, `emb:{storage_prefix}:…`,
    /// `graveyard:emb:{storage_prefix}:…` (see [`is_org_scoped_key_bytes`]).
    ///
    /// **Encoding:**
    /// - [`SnapshotValueEncoding::Logical`] — pass the **encrypting** store so
    ///   values are re-sealed under local at-rest crypto.
    /// - [`SnapshotValueEncoding::AtRestEnc`] — pass the **raw** store so
    ///   `ENC:…` envelopes are written as-is (no double-encrypt).
    async fn restore_inner(
        &self,
        store: &dyn NamespacedStore,
    ) -> SyncResult<SnapshotRestoreReport> {
        self.validate_current_version()?;
        self.validate_full_format_markers()?;
        let normalized;
        let namespaces = if self.value_encoding == SnapshotValueEncoding::Logical {
            normalized = validate_photograph(self.namespaces.clone())?;
            normalized.as_slice()
        } else {
            self.validate_unique_restore_keys()?;
            self.namespaces.as_slice()
        };
        let materialize_started = Instant::now();
        let mut org_skipped = 0usize;
        let mut entries = 0usize;
        let mut batches = 0usize;
        let results = stream::iter(
            namespaces
                .iter()
                .map(|namespace| restore_namespace(store, namespace, RestoreMode::Personal)),
        )
        .buffer_unordered(RESTORE_NAMESPACE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            let result = result?;
            entries = entries.saturating_add(result.entries);
            batches = batches.saturating_add(result.batches);
            org_skipped = org_skipped.saturating_add(result.org_skipped);
        }

        let materialize_ms = elapsed_ms(materialize_started);
        let flush_started = Instant::now();
        store.restore_durability_barrier().await?;
        let final_flush_ms = elapsed_ms(flush_started);

        if org_skipped > 0 {
            tracing::warn!(
                target: "fold_db::sync::snapshot",
                skipped = org_skipped,
                "snapshot restore SKIPPED {org_skipped} org-scoped storage key(s)                  (consented drop after org-crypto strip)"
            );
        }

        Ok(SnapshotRestoreReport {
            namespaces: namespaces.len(),
            entries,
            batches,
            materialize_ms,
            final_flush_ms,
        })
    }

    /// Restore a scoped target snapshot for multiple local storage prefixes.
    async fn restore_scoped_inner(
        &self,
        store: &dyn NamespacedStore,
        target_prefixes: &[String],
    ) -> SyncResult<SnapshotRestoreReport> {
        self.validate_current_version()?;
        if self.value_encoding != SnapshotValueEncoding::Logical {
            return Err(SyncError::Serialization(
                "scoped restore requires a current logical backup".to_string(),
            ));
        }
        let normalized;
        let namespaces = if self.value_encoding == SnapshotValueEncoding::Logical {
            normalized = validate_photograph(self.namespaces.clone())?;
            normalized.as_slice()
        } else {
            self.validate_unique_restore_keys()?;
            self.namespaces.as_slice()
        };
        validate_scoped_files(namespaces)?;
        let materialize_started = Instant::now();
        let mut entries = 0usize;
        let mut batches = 0usize;
        let results = stream::iter(namespaces.iter().map(|namespace| {
            restore_namespace(store, namespace, RestoreMode::Scoped(target_prefixes))
        }))
        .buffer_unordered(RESTORE_NAMESPACE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            let result = result?;
            entries = entries.saturating_add(result.entries);
            batches = batches.saturating_add(result.batches);
        }

        let materialize_ms = elapsed_ms(materialize_started);
        let flush_started = Instant::now();
        store.restore_durability_barrier().await?;
        let final_flush_ms = elapsed_ms(flush_started);

        Ok(SnapshotRestoreReport {
            namespaces: namespaces.len(),
            entries,
            batches,
            materialize_ms,
            final_flush_ms,
        })
    }
}
