use super::*;

impl EncryptingNamespacedStore {
    /// The reason `reap-unsealed` must refuse `collection`, if any.
    ///
    /// Three independent tests, any one of which is enough. They overlap on
    /// purpose: the static list guards against a test-built seam whose
    /// per-instance allowlist is empty, and the instance policy guards against
    /// a namespace that a future allowlist edit makes plaintext without
    /// touching the static list. A plaintext-by-policy namespace holds only
    /// un-enveloped rows, so a reap there would empty the schema catalog.
    pub fn reap_unsealed_refusal(&self, collection: &str) -> Option<String> {
        if collection == STRICT_MARKER_NAMESPACE {
            return Some(format!(
                "`{collection}` is the reserved at-rest marker namespace and is plaintext by policy"
            ));
        }
        if LASTSTORE_PLAINTEXT_NAMESPACES.contains(&collection) {
            return Some(format!(
                "`{collection}` is on LASTSTORE_PLAINTEXT_NAMESPACES; every row there is \
                 legitimately un-enveloped and removing them destroys the schema catalog"
            ));
        }
        if !self.should_encrypt(collection) {
            return Some(format!(
                "`{collection}` is plaintext by this store's policy; there is no envelope to test"
            ));
        }
        None
    }

    pub(super) async fn reseal_at_rest_impl(
        &self,
        options: ResealAtRestOptions,
    ) -> StorageResult<ResealAtRestReport> {
        let collection = options.collection.trim().to_string();
        if collection.is_empty() {
            return Err(StorageError::BackendError(
                "reseal-at-rest: collection name is required".to_string(),
            ));
        }
        if !collection_is_allowed(&collection) {
            let mut report =
                ResealAtRestReport::empty(collection.clone(), options.dry_run, options.target);
            report.skipped_reason = Some(format!(
                "collection `{collection}` is not on the reseal-at-rest allowlist ({})",
                RESEAL_AT_REST_ALLOWLIST.join(", ")
            ));
            return Ok(report);
        }
        if !self.should_encrypt(&collection) {
            let mut report = ResealAtRestReport::empty(collection, options.dry_run, options.target);
            report.skipped_reason = Some(
                "collection is plaintext-by-policy; there is no ENC: envelope to rewrite"
                    .to_string(),
            );
            return Ok(report);
        }

        if options.restart {
            self.clear_reseal_checkpoint(&collection).await?;
        }

        let stored = if options.restart {
            None
        } else {
            self.load_reseal_checkpoint(&collection).await?
        }
        .filter(|checkpoint| {
            checkpoint.version == RESEAL_CHECKPOINT_VERSION
                && checkpoint.target == options.target
                && checkpoint.format_version == RESEAL_TARGET_FORMAT_VERSION
        });
        if options.progress_only {
            let mut report = ResealAtRestReport::empty(collection, options.dry_run, options.target);
            report.checkpoint = stored;
            return Ok(report);
        }

        let resume = stored
            .as_ref()
            .filter(|c| !c.completed)
            .and_then(|c| c.cursor.clone());
        // Open the inner (already-sealed) namespace and wrap it locally so
        // puts write ENB bytes without consulting process write switches.
        let inner = self.inner.open_namespace(&collection).await?;
        let enc_store =
            EncryptingKvStore::new(&collection, Arc::clone(&inner), Arc::clone(&self.crypto));
        let mut report = enc_store
            .reseal_at_rest_pass(&collection, &options, resume)
            .await?;

        // A physical cursor only completes one lap. A row inserted into an
        // earlier handle while that lap is in flight is intentionally found
        // on the next lap. Require one clean verification lap after any
        // rewrite or CAS race before the durable checkpoint claims complete.
        // `cursor = None` restarts from the first current handle without a
        // checkpoint schema change.
        if !options.dry_run
            && !report.more_remaining
            && (report.rows_converted > 0 || report.rows_cas_skipped > 0)
        {
            report.more_remaining = true;
            report.next_cursor = None;
        }

        if options.dry_run {
            report.checkpoint = Some(ResealAtRestCheckpoint {
                version: RESEAL_CHECKPOINT_VERSION,
                target: options.target,
                format_version: RESEAL_TARGET_FORMAT_VERSION,
                collection: collection.clone(),
                cursor: report.next_cursor.clone(),
                rows_scanned_total: report.rows_scanned,
                rows_converted_total: 0,
                bytes_before_total: report.bytes_before,
                bytes_after_total: report.bytes_after,
                completed: !report.more_remaining
                    && report.rows_to_convert == 0
                    && report.rows_unreadable == 0,
                updated_at: chrono::Utc::now(),
            });
            return Ok(report);
        }

        // A checkpoint may skip every row before its cursor. Make all
        // replacements durable before that checkpoint becomes visible.
        inner.flush().await?;

        let mut checkpoint = stored.unwrap_or_else(|| ResealAtRestCheckpoint {
            version: RESEAL_CHECKPOINT_VERSION,
            target: options.target,
            format_version: RESEAL_TARGET_FORMAT_VERSION,
            collection: collection.clone(),
            cursor: None,
            rows_scanned_total: 0,
            rows_converted_total: 0,
            bytes_before_total: 0,
            bytes_after_total: 0,
            completed: false,
            updated_at: chrono::Utc::now(),
        });
        checkpoint.rows_scanned_total = checkpoint
            .rows_scanned_total
            .saturating_add(report.rows_scanned);
        checkpoint.rows_converted_total = checkpoint
            .rows_converted_total
            .saturating_add(report.rows_converted);
        checkpoint.bytes_before_total = checkpoint
            .bytes_before_total
            .saturating_add(report.bytes_before);
        checkpoint.bytes_after_total = checkpoint
            .bytes_after_total
            .saturating_add(report.bytes_after);
        checkpoint.cursor = report.next_cursor.clone();
        checkpoint.completed = !report.more_remaining;
        checkpoint.updated_at = chrono::Utc::now();
        self.store_reseal_checkpoint(&checkpoint).await?;
        report.checkpoint = Some(checkpoint);
        Ok(report)
    }

    pub(super) async fn reap_unsealed_impl(
        &self,
        options: ReapUnsealedOptions,
    ) -> StorageResult<ReapUnsealedReport> {
        let collection = options.collection.trim().to_string();
        if collection.is_empty() {
            return Err(StorageError::BackendError(
                "reap-unsealed: collection name is required".to_string(),
            ));
        }
        // Refusal comes before the allowlist so the operator reads *why* a
        // catalog namespace is out of bounds, not merely that it is unlisted.
        // It is an error, not a skipped report: a destructive verb pointed at
        // the wrong plane must not exit 0.
        if let Some(reason) = self.reap_unsealed_refusal(&collection) {
            return Err(StorageError::BackendError(format!(
                "reap-unsealed: refused: {reason}"
            )));
        }
        if !collection_is_allowed(&collection) {
            let mut report = ReapUnsealedReport::empty(collection.clone(), options.dry_run);
            report.skipped_reason = Some(format!(
                "collection `{collection}` is not on the sealed-plane allowlist ({})",
                RESEAL_AT_REST_ALLOWLIST.join(", ")
            ));
            return Ok(report);
        }

        if options.restart {
            self.clear_reap_checkpoint(&collection).await?;
        }

        let stored = if options.restart {
            None
        } else {
            self.load_reap_checkpoint(&collection).await?
        };
        if options.progress_only {
            let mut report = ReapUnsealedReport::empty(collection, options.dry_run);
            report.checkpoint = stored;
            return Ok(report);
        }

        let resume = stored
            .as_ref()
            .filter(|c| !c.completed)
            .and_then(|c| c.cursor.clone());
        // Open the inner (raw) namespace so the pass sees stored bytes as they
        // are on disk and deletes go straight to the physical store.
        let inner = self.inner.open_namespace(&collection).await?;
        let enc_store = EncryptingKvStore::new(&collection, inner, Arc::clone(&self.crypto));
        let mut report = enc_store
            .reap_unsealed_pass(&collection, &options, resume)
            .await?;

        if options.dry_run {
            report.checkpoint = Some(ReapUnsealedCheckpoint {
                collection: collection.clone(),
                cursor: report.next_cursor.clone(),
                rows_scanned_total: report.rows_scanned,
                rows_removed_total: 0,
                bytes_reclaimed_total: 0,
                completed: !report.more_remaining,
                updated_at: chrono::Utc::now(),
            });
            return Ok(report);
        }

        let mut checkpoint = stored.unwrap_or_else(|| ReapUnsealedCheckpoint {
            collection: collection.clone(),
            cursor: None,
            rows_scanned_total: 0,
            rows_removed_total: 0,
            bytes_reclaimed_total: 0,
            completed: false,
            updated_at: chrono::Utc::now(),
        });
        checkpoint.rows_scanned_total = checkpoint
            .rows_scanned_total
            .saturating_add(report.rows_scanned);
        checkpoint.rows_removed_total = checkpoint
            .rows_removed_total
            .saturating_add(report.rows_removed);
        checkpoint.bytes_reclaimed_total = checkpoint
            .bytes_reclaimed_total
            .saturating_add(report.bytes_reclaimed);
        checkpoint.cursor = report.next_cursor.clone();
        checkpoint.completed = !report.more_remaining;
        checkpoint.updated_at = chrono::Utc::now();
        self.store_reap_checkpoint(&checkpoint).await?;
        report.checkpoint = Some(checkpoint);
        Ok(report)
    }
}
