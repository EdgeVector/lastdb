use crate::storage::EncryptingNamespacedStore;

/// Run non-fatal boot-time at-rest migrations for local encrypted namespaces.
pub(super) async fn run_boot_migrations(enc: Option<&EncryptingNamespacedStore>) {
    let Some(enc) = enc else {
        return;
    };

    migrate_laststore_plaintext_policy_namespaces(enc).await;
}

async fn migrate_laststore_plaintext_policy_namespaces(enc: &EncryptingNamespacedStore) {
    use crate::storage::LASTSTORE_PLAINTEXT_NAMESPACES;

    let mut total = 0usize;
    let mut skipped = 0usize;
    for ns in LASTSTORE_PLAINTEXT_NAMESPACES {
        // Drop any durable strict-encrypt marker left by the prior
        // encrypt-at-rest era.
        //
        // Nothing reads these markers any more: the strict flip and its clean
        // walk were removed by
        // `decision-2026-09-14-drop-dual-read-unsealed-is-gone`, so an
        // un-enveloped row in an encrypted namespace now reads as absent rather
        // than being adopted or rejected on the strength of a marker. The
        // clear stays because the rows themselves are still on disk on any home
        // that ran the old code, and leaving dead sentinels in a namespace that
        // still holds live plaintext-sweep sentinels invites a future reader to
        // treat them as meaningful.
        if let Err(e) = enc.clear_strict_marker(ns).await {
            tracing::warn!(
                namespace = ns,
                error = %e,
                "failed to clear stale strict-mode marker for a plaintext-policy namespace; continuing unwrap"
            );
        }
        match enc.plaintext_sweep_completed(ns).await {
            Ok(true) => {
                skipped += 1;
                tracing::debug!(
                    namespace = ns,
                    "plaintext-policy migration already completed; skipping raw namespace scan"
                );
                continue;
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(
                namespace = ns,
                error = %e,
                "failed to read plaintext-policy migration marker; continuing unwrap"
            ),
        }
        match enc.decrypt_legacy_encrypted_namespace(ns).await {
            Ok(n) => {
                total += n;
                if let Err(e) = enc.mark_plaintext_sweep_completed(ns).await {
                    tracing::warn!(
                        namespace = ns,
                        error = %e,
                        "failed to persist plaintext-policy migration marker; the sweep will retry next boot"
                    );
                }
            }
            Err(e) => tracing::warn!(
                namespace = ns,
                error = %e,
                "plaintext-policy migration failed; ENC rows remain readable only by older encrypted policy and the sweep retries next boot"
            ),
        };
    }

    if total > 0 {
        tracing::info!(
            migrated = total,
            "decrypted legacy ENC rows for LastStore plaintext-policy namespaces"
        );
    }
    if skipped > 0 {
        tracing::info!(
            skipped,
            "skipped completed LastStore plaintext-policy migration scans"
        );
    }
}
