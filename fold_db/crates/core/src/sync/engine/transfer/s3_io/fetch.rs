//! Download fetch and unseal helpers.

use super::super::super::*;
use crate::sync::engine::pin_log::PinLogRecord;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::SyncTarget;

/// Outcome of a single fetch+unseal, including ciphertext size for budgets.
pub(crate) struct FetchedEntry {
    pub seq: u64,
    pub entry: Option<LogEntry>,
    /// When the flat `log/{seq}.enc` object is a continuous mutation-log
    /// segment (`PinLogRecord` batch) rather than a classic `LogEntry`, the
    /// unsealed records live here. Download/bootstrap must apply these before
    /// advancing the classic download cursor — never treat recognition alone
    /// as progress.
    pub mutation_log_records: Option<Vec<PinLogRecord>>,
    /// Ciphertext bytes read from object storage (0 when 404 / oversize skip
    /// without a body, or when the object was skipped before buffering).
    pub ciphertext_bytes: u64,
    /// True when the object was refused for exceeding `max_download_entry_bytes`.
    pub skipped_oversize: bool,
}

impl FetchedEntry {
    /// A fetch outcome that carries no entry and no mutation-log records.
    fn empty(seq: u64, ciphertext_bytes: u64, skipped_oversize: bool) -> Self {
        Self {
            seq,
            entry: None,
            mutation_log_records: None,
            ciphertext_bytes,
            skipped_oversize,
        }
    }
}

/// Result of the download stage: either ciphertext to unseal, or a final
/// outcome that skips the unseal stage.
enum DownloadStage {
    Bytes(Vec<u8>),
    Done(FetchedEntry),
}

impl SyncEngine {
    pub(crate) async fn fetch_and_unseal_entry_detailed(
        &self,
        operation: &str,
        target: &SyncTarget,
        seq: u64,
        url: crate::sync::s3::PresignedUrl,
    ) -> SyncResult<FetchedEntry> {
        // Operator-cleared seq (delete or local-skip): never re-apply, even when
        // the cloud object still exists. Corrupt path usually 404s after delete;
        // apply-failed path deliberately leaves the object for peers.
        if self
            .has_replay_quarantine_tombstone(&target.prefix, seq)
            .await?
        {
            tracing::warn!(
                "sync replay skipping quarantined '{}' seq={} (local tombstone; object may still exist in cloud)",
                target.label,
                seq
            );
            return Ok(FetchedEntry::empty(seq, 0, false));
        }

        let bytes = match self
            .download_log_object(operation, target, seq, url)
            .await?
        {
            DownloadStage::Bytes(bytes) => bytes,
            DownloadStage::Done(fetched) => return Ok(fetched),
        };

        let ciphertext_bytes = bytes.len() as u64;
        match LogEntry::unseal(&bytes, &target.crypto, &target.label, seq).await {
            Ok(entry) => {
                // Ciphertext buffer drops with `bytes` at end of scope — keep
                // only the unsealed entry for sequential replay.
                drop(bytes);
                Ok(FetchedEntry {
                    seq,
                    entry: Some(entry),
                    mutation_log_records: None,
                    ciphertext_bytes,
                    skipped_oversize: false,
                })
            }
            Err(SyncError::Serialization(log_entry_error)) => {
                Self::resolve_log_entry_serialization_failure(
                    target,
                    seq,
                    &bytes,
                    ciphertext_bytes,
                    &log_entry_error,
                )
                .await
            }
            Err(e) if Self::should_quarantine_unsealed_entry(&e) => {
                tracing::warn!(
                    target: "fold_db::sync::replay",
                    target_label = %target.label,
                    seq,
                    poison = true,
                    "sync replay: SKIPPING unsupported-envelope log entry and advancing cursor: {e}"
                );
                Ok(FetchedEntry::empty(seq, ciphertext_bytes, false))
            }
            Err(e) => Err(self.unseal_failure_to_corrupt_entry(target, seq, &e).await),
        }
    }

    /// Download one flat log object, handling the oversize skip and the 404
    /// case (a 404 is only acceptable when a quarantine tombstone exists).
    async fn download_log_object(
        &self,
        operation: &str,
        target: &SyncTarget,
        seq: u64,
        url: crate::sync::s3::PresignedUrl,
    ) -> SyncResult<DownloadStage> {
        let max_entry = self.config.max_download_entry_bytes;
        let max_opt = (max_entry > 0).then_some(max_entry);
        let downloaded = self
            .retry_s3(
                &format!("{operation} '{}' seq {}", target.label, seq),
                || {
                    let url = url.clone();
                    async move { self.s3.download_limited(&url, max_opt).await }
                },
            )
            .await;
        let downloaded = match downloaded {
            Ok(v) => v,
            Err(e) if super::is_oversize_s3_error(&e) => {
                tracing::warn!(
                    target: "fold_db::sync::memory",
                    target_label = %target.label,
                    seq,
                    max_entry_bytes = max_entry,
                    error = %e,
                    "sync download: SKIPPING oversize log object and advancing cursor"
                );
                return Ok(DownloadStage::Done(FetchedEntry::empty(seq, 0, true)));
            }
            Err(e) => return Err(e),
        };
        let Some(bytes) = downloaded else {
            if !self
                .has_replay_quarantine_tombstone(&target.prefix, seq)
                .await?
            {
                return Err(SyncError::S3(format!(
                    "{operation} '{}' seq {seq} returned 404 without a quarantine tombstone; leaving cursor pinned for retry",
                    target.label
                )));
            }
            tracing::warn!(
                "sync replay skipping quarantined '{}' seq={} (server listed this seq but object storage returned 404)",
                target.label,
                seq
            );
            return Ok(DownloadStage::Done(FetchedEntry::empty(seq, 0, false)));
        };
        Ok(DownloadStage::Bytes(bytes))
    }

    /// Resolve a `LogEntry` JSON decode failure on an otherwise valid
    /// envelope: recognize a mutation-log segment, skip a personal third-type
    /// object, or report a corrupt entry.
    async fn resolve_log_entry_serialization_failure(
        target: &SyncTarget,
        seq: u64,
        bytes: &[u8],
        ciphertext_bytes: u64,
        log_entry_error: &str,
    ) -> SyncResult<FetchedEntry> {
        // Mutation-log segments currently share the legacy flat
        // `log/{seq}.enc` namespace with replayable `LogEntry`
        // objects. They use the same authenticated envelope + hash
        // shape, but their plaintext is a `PinLogRecord`, so opening
        // one as a `LogEntry` succeeds cryptographically and then
        // fails JSON decoding. This is the replay-side twin of the
        // decrypt-proof compatibility path in `proof.rs`.
        //
        // Do not treat every serialization failure as a mutation-log
        // segment: that would hide a genuinely malformed encrypted
        // LogEntry. Re-open as the mutation-log type and require its
        // frontier to match the flat key. The caller must APPLY the
        // records before advancing the download cursor — recognition
        // alone is not progress (teardown-sync-mutation-log-download-
        // skips-advances-cursor).
        match crate::sync::engine::pin_log::unseal_mutation_log_segment(bytes, &target.crypto)
            .await
        {
            Ok(records)
                if records
                    .last()
                    .is_some_and(|record| record.frontier_after == seq) =>
            {
                tracing::info!(
                    target: "fold_db::sync::replay",
                    target_label = %target.label,
                    seq,
                    records = records.len(),
                    "sync replay: recognized mutation-log segment on legacy flat log key; apply before cursor advance"
                );
                Ok(FetchedEntry {
                    seq,
                    entry: None,
                    mutation_log_records: Some(records),
                    ciphertext_bytes,
                    skipped_oversize: false,
                })
            }
            Ok(records) => Err(SyncError::CorruptEntry {
                target: target.label.clone(),
                seq,
                reason: format!(
                    "encrypted object decoded as a mutation-log segment with mismatched frontier {}; LogEntry decode failed: {log_entry_error}",
                    records
                        .last()
                        .map_or_else(|| "empty".to_string(), |record| {
                            record.frontier_after.to_string()
                        })
                ),
            }),
            Err(_) if target.prefix.is_empty() => {
                // Personal empty-prefix: decrypt + hash already
                // succeeded (this arm is LogEntry Serialization). The
                // plaintext is neither LogEntry nor PinLogRecord.
                // Skip and advance so one historical third-type object
                // cannot pin the cycle or block later personal upload.
                // Keep the cloud object. Crypto/auth failures never
                // reach this arm.
                tracing::warn!(
                    target: "fold_db::sync::replay",
                    target_label = %target.label,
                    seq,
                    poison = true,
                    "sync replay: SKIPPING personal flat-key object that is neither LogEntry nor PinLogRecord and advancing cursor: {log_entry_error}"
                );
                Ok(FetchedEntry::empty(seq, ciphertext_bytes, false))
            }
            Err(_) => Err(SyncError::CorruptEntry {
                target: target.label.clone(),
                seq,
                reason: format!("unseal failed: {log_entry_error}"),
            }),
        }
    }

    /// Log (or suppress) a hard unseal failure and build the corrupt-entry error.
    async fn unseal_failure_to_corrupt_entry(
        &self,
        target: &SyncTarget,
        seq: u64,
        e: &SyncError,
    ) -> SyncError {
        if self.should_log_unseal_failure(&target.label, seq, e).await {
            tracing::error!(
                "sync replay aborted: failed to unseal entry in '{}' seq={}: {}",
                target.label,
                seq,
                e
            );
        } else {
            tracing::debug!(
                target: "fold_db::sync::replay",
                target_label = %target.label,
                seq,
                "sync replay: repeated unseal failure suppressed: {e}"
            );
        }
        SyncError::CorruptEntry {
            target: target.label.clone(),
            seq,
            reason: format!("unseal failed: {e}"),
        }
    }
}
