//! At-rest maintenance passes (reseal and unsealed-row reap) for `EncryptingKvStore`.

use super::*;

impl EncryptingKvStore {
    /// One bounded pass over `self.inner` sealed bytes.
    ///
    /// Reads and writes the inner store so the replace is already-sealed ENB
    /// and does not consult `KvCompressionPolicy`. Each replace is one atomic
    /// [`KvStore::compare_and_swap`] against the raw bytes the scan read, so a
    /// concurrent write is never overwritten. Resume uses `resume`.
    // lint:fn-size-ok moved verbatim from its original module
    pub(crate) async fn reseal_at_rest_pass(
        &self,
        collection: &str,
        options: &ResealAtRestOptions,
        resume: Option<PhysicalScanCursor>,
    ) -> StorageResult<ResealAtRestReport> {
        let mut report =
            ResealAtRestReport::empty(collection.to_string(), options.dry_run, options.target);
        let max_rows = options.max_rows.unwrap_or(RESEAL_DEFAULT_MAX_ROWS).max(1);
        let max_secs = options.max_secs.unwrap_or(RESEAL_DEFAULT_MAX_SECS).max(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
        let skip_checkpoints =
            collection == crate::storage::reseal_at_rest::RESEAL_CHECKPOINT_NAMESPACE;
        let mut scan_cursor = resume.clone();
        let mut durable_cursor = resume;
        let mut blocked = false;
        let crypto = Arc::clone(&self.crypto);

        loop {
            if report.rows_scanned as usize >= max_rows
                || (report.rows_scanned > 0 && std::time::Instant::now() >= deadline)
            {
                report.more_remaining = true;
                break;
            }
            let remaining_rows = max_rows.saturating_sub(report.rows_scanned as usize);
            let page = self
                .inner
                .scan_range_physical_paged(
                    &[],
                    RESEAL_COLLECTION_END,
                    scan_cursor.as_ref(),
                    remaining_rows.min(RESEAL_PAGE_ROWS),
                    RESEAL_PAGE_HANDLES,
                )
                .await?;
            if page.rows.is_empty() {
                report.more_remaining = page.next_cursor.is_some();
                scan_cursor = page.next_cursor.clone();
                if !blocked {
                    durable_cursor = page.next_cursor;
                }
                if !report.more_remaining {
                    break;
                }
                continue;
            }

            for (key, raw) in page.rows {
                if report.rows_scanned as usize >= max_rows
                    || (report.rows_scanned > 0 && std::time::Instant::now() >= deadline)
                {
                    report.more_remaining = true;
                    break;
                }
                let row_cursor = PhysicalScanCursor {
                    after_key: Some(key.clone()),
                    ..page.row_handle.clone().unwrap_or_default()
                };
                if skip_checkpoints && is_checkpoint_key(&key) {
                    if !blocked {
                        durable_cursor = Some(row_cursor);
                    }
                    continue;
                }
                report.rows_scanned += 1;

                let binary_deflated = if raw.starts_with(AT_REST_ENC_BINARY_PREFIX.as_bytes()) {
                    if let Ok(Some((deflated, _))) = decode_binary_ciphertext(&raw) {
                        Some(deflated)
                    } else {
                        report.rows_unreadable += 1;
                        blocked = true;
                        continue;
                    }
                } else {
                    None
                };
                let sealed = binary_deflated.is_some()
                    || raw.starts_with(AT_REST_ENC_PREFIX.as_bytes())
                    || raw.starts_with(AT_REST_ENC_DEFLATE_PREFIX.as_bytes());
                if !sealed {
                    report.rows_plaintext += 1;
                    if !blocked {
                        durable_cursor = Some(row_cursor);
                    }
                    continue;
                }

                let already_target = matches!(
                    (options.target, binary_deflated),
                    (ResealAtRestTarget::Binary, Some(false))
                        | (ResealAtRestTarget::BinaryCompress, Some(true))
                );
                let Ok(Some(plaintext)) =
                    self.open_sealed_value(raw.clone(), crypto.as_ref()).await
                else {
                    // Wrong key, invalid envelope, or corrupt compression.
                    report.rows_unreadable += 1;
                    blocked = true;
                    continue;
                };
                if already_target {
                    report.rows_already_target += 1;
                    if !blocked {
                        durable_cursor = Some(row_cursor);
                    }
                    continue;
                }
                let (new_sealed, used_deflate) = self
                    .seal_explicit_enb(&key, &plaintext, options.target)
                    .await?;
                if options.target == ResealAtRestTarget::BinaryCompress
                    && binary_deflated == Some(false)
                    && !used_deflate
                {
                    report.rows_already_target += 1;
                    if !blocked {
                        durable_cursor = Some(row_cursor);
                    }
                    continue;
                }
                report.rows_to_convert += 1;
                report.bytes_before += raw.len() as u64;
                report.bytes_after += new_sealed.len() as u64;

                if options.dry_run {
                    if !blocked {
                        durable_cursor = Some(row_cursor);
                    }
                    continue;
                }

                // One atomic compare-and-replace. A writer that landed after
                // the scan read `raw` makes the swap refuse, so its value
                // stays; the next pass reseals it.
                let new_len = new_sealed.len() as u64;
                if !self
                    .inner
                    .compare_and_swap(&key, &raw, Some(new_sealed))
                    .await?
                {
                    report.rows_cas_skipped += 1;
                    report.bytes_before = report.bytes_before.saturating_sub(raw.len() as u64);
                    report.bytes_after = report.bytes_after.saturating_sub(new_len);
                    report.rows_to_convert = report.rows_to_convert.saturating_sub(1);
                    blocked = true;
                    continue;
                }
                report.rows_converted += 1;
                if !blocked {
                    durable_cursor = Some(row_cursor);
                }
            }

            if report.more_remaining {
                break;
            }
            if let Some(next) = page.next_cursor {
                scan_cursor = Some(next.clone());
                if !blocked {
                    durable_cursor = Some(next);
                }
            } else {
                report.more_remaining = false;
                break;
            }
        }

        report.more_remaining |= blocked;
        report.next_cursor = durable_cursor.filter(|_| report.more_remaining);
        Ok(report)
    }

    /// One bounded pass that removes un-enveloped rows from `self.inner`.
    ///
    /// Classifies on the envelope prefix alone and never decrypts: a sealed
    /// row is left in place whether or not it opens under this node's key,
    /// because "ours and unopenable" is not "not ours". Deletes go to the
    /// inner store through one atomic [`KvStore::compare_and_swap`] (the raw
    /// bytes must still match what the scan saw), so a row rewritten between
    /// scan and delete is skipped this pass.
    /// Resume uses `resume`.
    pub(crate) async fn reap_unsealed_pass(
        &self,
        collection: &str,
        options: &ReapUnsealedOptions,
        resume: Option<PhysicalScanCursor>,
    ) -> StorageResult<ReapUnsealedReport> {
        let mut report = ReapUnsealedReport::empty(collection.to_string(), options.dry_run);
        let max_rows = options.max_rows.unwrap_or(REAP_DEFAULT_MAX_ROWS).max(1);
        let max_secs = options.max_secs.unwrap_or(REAP_DEFAULT_MAX_SECS).max(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(max_secs);
        // Both admin checkpoints live in `metadata` and are written through
        // the encrypting seam, so they are sealed and would never be reaped.
        // Skipping them keeps `rows_scanned` about the plane, not the pass.
        let skip_checkpoints = collection == REAP_CHECKPOINT_NAMESPACE;
        let mut cursor = resume;

        loop {
            if report.rows_scanned as usize >= max_rows
                || (report.rows_scanned > 0 && std::time::Instant::now() >= deadline)
            {
                report.more_remaining = true;
                break;
            }
            let remaining_rows = max_rows.saturating_sub(report.rows_scanned as usize);
            let page = self
                .inner
                .scan_range_physical_paged(
                    &[],
                    RESEAL_COLLECTION_END,
                    cursor.as_ref(),
                    remaining_rows.min(RESEAL_PAGE_ROWS),
                    RESEAL_PAGE_HANDLES,
                )
                .await?;
            if page.rows.is_empty() {
                report.more_remaining = page.next_cursor.is_some();
                cursor = page.next_cursor;
                if !report.more_remaining {
                    break;
                }
                continue;
            }

            for (key, raw) in page.rows {
                if report.rows_scanned as usize >= max_rows
                    || (report.rows_scanned > 0 && std::time::Instant::now() >= deadline)
                {
                    report.more_remaining = true;
                    break;
                }
                if skip_checkpoints && (is_checkpoint_key(&key) || is_reap_checkpoint_key(&key)) {
                    continue;
                }
                report.rows_scanned += 1;
                cursor = Some(PhysicalScanCursor {
                    after_key: Some(key.clone()),
                    ..page.row_handle.clone().unwrap_or_default()
                });

                if Self::is_encrypted_value(&raw) {
                    report.rows_sealed += 1;
                    continue;
                }
                report.rows_unsealed += 1;
                report.bytes_reclaimed += raw.len() as u64;

                if options.dry_run {
                    continue;
                }

                // One atomic compare-and-delete: a row rewritten after the
                // scan (for example, sealed by a writer) is never deleted.
                if !self.inner.compare_and_swap(&key, &raw, None).await? {
                    report.rows_cas_skipped += 1;
                    report.rows_unsealed = report.rows_unsealed.saturating_sub(1);
                    report.bytes_reclaimed =
                        report.bytes_reclaimed.saturating_sub(raw.len() as u64);
                    continue;
                }
                report.rows_removed += 1;
            }

            if report.more_remaining {
                break;
            }
            if let Some(next) = page.next_cursor {
                cursor = Some(next);
            } else {
                report.more_remaining = false;
                break;
            }
        }

        if !options.dry_run {
            crate::crypto::record_unsealed_reap(report.rows_removed, report.bytes_reclaimed);
        }
        report.next_cursor = cursor.filter(|_| report.more_remaining);
        Ok(report)
    }
}
