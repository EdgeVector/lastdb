//! S3 upload path.

use super::super::super::*;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::SyncTarget;
use futures::stream::StreamExt;

impl SyncEngine {
    /// Upload entries to a single sync target.
    ///
    /// Personal targets (empty prefix) upload under each entry's own
    /// client-assigned nanosecond `entry.seq`. Share targets let the server
    /// atomically allocate a contiguous block of seqs via
    /// `presign_upload_alloc`; each entry's `seq` is rewritten to its
    /// server-assigned value before sealing so the S3 key, the sealed
    /// payload, and the downloader's parsed seq all agree.
    ///
    /// Chunked at `MAX_PRESIGN_BATCH` to respect storage_service's per-request
    /// cap on `seq_numbers`. Earlier successful chunks are counted toward the
    /// returned [`UploadOutcome`] even when a later chunk fails — `do_sync` uses
    /// that count to decide whether to drain pending or keep everything for
    /// retry, and reporting fewer than expected is the correct signal there. Each
    /// chunk's presign + seal + S3 PUTs happen as one logical step;
    /// retries across chunks are safe because S3 PUTs at the same seq key
    /// overwrite idempotently.
    pub(crate) async fn upload_entries(
        &self,
        target: &SyncTarget,
        entries: &[LogEntry],
    ) -> SyncResult<UploadOutcome> {
        if entries.is_empty() {
            return Ok(UploadOutcome::default());
        }
        let mut total = UploadOutcome::default();
        for chunk in entries.chunks(MAX_PRESIGN_BATCH) {
            match self.upload_entries_chunk(target, chunk).await {
                Ok(outcome) => {
                    total.entries_uploaded += outcome.entries_uploaded;
                    total.max_seq_uploaded = total.max_seq_uploaded.max(outcome.max_seq_uploaded);
                    total.transfer_bytes_uploaded += outcome.transfer_bytes_uploaded;
                    total.transfer_elapsed_secs += outcome.transfer_elapsed_secs;
                }
                Err(e) => {
                    if total.entries_uploaded > 0 {
                        tracing::warn!(
                            "upload to '{}' partial: {}/{} entries uploaded across complete chunks before {}",
                            target.label,
                            total.entries_uploaded,
                            entries.len(),
                            e
                        );
                        // Best-effort personal index merge when applicable;
                        // success and failure both surface the same partial
                        // outcome (do not drop already-uploaded chunk progress).
                        if Self::supports_personal_log_index(target) {
                            if let Err(index_err) = self
                                .merge_personal_log_index(
                                    target,
                                    entries
                                        .iter()
                                        .take(total.entries_uploaded)
                                        .map(|entry| entry.seq),
                                )
                                .await
                            {
                                tracing::warn!(
                                    "upload to '{}' partial: personal log index merge failed after {}/{} uploaded entries: {}",
                                    target.label,
                                    total.entries_uploaded,
                                    entries.len(),
                                    index_err
                                );
                            }
                        }
                        total.partial_error = Some(e);
                        return Ok(total);
                    }
                    return Err(e);
                }
            }
        }
        if Self::supports_personal_log_index(target) {
            if let Err(e) = self
                .merge_personal_log_index(
                    target,
                    entries
                        .iter()
                        .take(total.entries_uploaded)
                        .map(|entry| entry.seq),
                )
                .await
            {
                if total.entries_uploaded > 0 {
                    tracing::warn!(
                        "upload to '{}' index merge failed after {}/{} uploaded entries: {}",
                        target.label,
                        total.entries_uploaded,
                        entries.len(),
                        e
                    );
                    total.partial_error = Some(e);
                    return Ok(total);
                }
                return Err(e);
            }
        }
        // Feed adaptive upload policy EWMA from S3 PUT transfer time only.
        // Best-effort metering confirm can be slow and must not make a healthy
        // network look slow enough to clamp future upload cycles.
        if total.entries_uploaded > 0 && total.transfer_bytes_uploaded > 0 {
            self.upload_policy
                .record_upload_sample(total.transfer_bytes_uploaded, total.transfer_elapsed_secs);
        }
        Ok(total)
    }

    /// Upload a single chunk of entries (≤ `MAX_PRESIGN_BATCH`) to a target.
    /// Caller is responsible for chunking; this is the original single-batch
    /// presign + seal + S3 PUT path, factored out so `upload_entries` can loop.
    pub(crate) async fn upload_entries_chunk(
        &self,
        target: &SyncTarget,
        entries: &[LogEntry],
    ) -> SyncResult<UploadOutcome> {
        if entries.is_empty() {
            return Ok(UploadOutcome::default());
        }
        debug_assert!(
            entries.len() <= MAX_PRESIGN_BATCH,
            "upload_entries_chunk called with {} entries (cap {})",
            entries.len(),
            MAX_PRESIGN_BATCH
        );

        let is_scoped = !target.prefix.is_empty();

        let (sealed, urls): (
            Vec<(u64, crate::sync::log::SealedLogEntry)>,
            Vec<crate::sync::s3::PresignedUrl>,
        ) = if is_scoped {
            let pairs = self
                .auth
                .presign_upload_alloc(target, entries.len() as u32)
                .await?;
            if pairs.len() != entries.len() {
                return Err(SyncError::Auth(format!(
                    "expected {} server-assigned seqs for '{}', got {}",
                    entries.len(),
                    target.label,
                    pairs.len(),
                )));
            }
            let mut sealed = Vec::with_capacity(entries.len());
            let mut urls = Vec::with_capacity(entries.len());
            for (entry, (server_seq, url)) in entries.iter().zip(pairs) {
                let mut rewritten = entry.clone();
                rewritten.seq = server_seq;
                let s = rewritten.seal(&target.crypto).await?;
                sealed.push((server_seq, s));
                urls.push(url);
            }
            (sealed, urls)
        } else {
            let mut sealed = Vec::with_capacity(entries.len());
            for entry in entries {
                let s = entry.seal(&target.crypto).await?;
                sealed.push((entry.seq, s));
            }
            let seq_numbers: Vec<u64> = sealed.iter().map(|(seq, _)| *seq).collect();
            // Per-entry ciphertext size for the server quota pre-check.
            // Without this the storage service assumes 1 MiB/entry, so a
            // 1000-entry chunk is billed as ~1 GiB and free-tier (1 GiB)
            // accounts falsely fail with "quota exceeded" while only
            // holding tens of MiB of real objects.
            let max_sealed = sealed
                .iter()
                .map(|(_, s)| s.bytes.len() as u64)
                .max()
                .unwrap_or(1024)
                .max(1);
            let urls = self
                .auth
                .presign_upload(target, &seq_numbers, Some(max_sealed))
                .await?;
            if urls.len() != sealed.len() {
                return Err(SyncError::Auth(format!(
                    "expected {} presigned URLs for '{}', got {}",
                    sealed.len(),
                    target.label,
                    urls.len()
                )));
            }
            (sealed, urls)
        };

        let max_seq_uploaded = sealed.iter().map(|(seq, _)| *seq).max();
        let all_seqs: Vec<u64> = sealed.iter().map(|(seq, _)| *seq).collect();
        let transfer_bytes_uploaded = sealed
            .iter()
            .map(|(_, s)| s.bytes.len() as u64)
            .sum::<u64>();

        // Fan the S3 PUTs out with a bounded concurrency cap. Each PUT targets
        // a distinct `{prefix}/log/{seq}.enc` key, so ordering between them is
        // irrelevant for personal targets (client-assigned seqs overwrite
        // idempotently). Concurrency comes from the adaptive upload policy.
        //
        // Scoped (org/share) targets are different: `presign_upload_alloc`
        // permanently advances a DynamoDB counter. A concurrent partial PUT
        // success leaves objects at some of the allocated seqs; the next cycle
        // re-allocates a *new* block while the orphans remain, so org download
        // hard-fails on a non-contiguous listing forever. Track per-seq success
        // and, on any scoped failure, best-effort delete the orphans from this
        // allocation before surfacing the error so retry re-allocs into a
        // contiguous object set (gaps between counter ranges never appear as
        // listed objects).
        let cap = self.active_upload_caps().await.concurrency.max(1);
        // Iterate OWNED `((seq, sealed), url)` items — `zip(urls)` consumes the
        // Vec and `sealed.into_iter()` yields by value, so the map closure takes
        // no borrowed iterator item. (Borrowing the iterator item here would
        // trip the higher-ranked-lifetime bound when this future is later
        // spawned by the sync coordinator.)
        let transfer_started = std::time::Instant::now();
        let mut put_stream = futures::stream::iter(sealed.into_iter().zip(urls).map(
            |((seq, s), url)| async move {
                let bytes = s.bytes;
                let put_result = self
                    .retry_s3(&format!("upload seq {seq}"), || {
                        let url = url.clone();
                        let bytes = bytes.clone();
                        async move { self.s3.upload(&url, bytes).await }
                    })
                    .await;
                (seq, put_result)
            },
        ))
        .buffer_unordered(cap);

        let mut uploaded_count = 0usize;
        let mut succeeded_seqs: Vec<u64> = Vec::new();
        let mut first_err: Option<SyncError> = None;
        while let Some((seq, res)) = put_stream.next().await {
            match res {
                Ok(()) => {
                    uploaded_count += 1;
                    succeeded_seqs.push(seq);
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_err {
            if is_scoped && !succeeded_seqs.is_empty() {
                // Best-effort orphan cleanup so a re-alloc retry cannot leave
                // permanent non-contiguous holes. Failures here are logged only
                // — the original PUT error is what the caller retries on.
                self.rollback_scoped_partial_puts(target, &succeeded_seqs)
                    .await;
            }
            return Err(e);
        }
        let transfer_elapsed_secs = transfer_started.elapsed().as_secs_f64().max(0.001);

        // Metering P0: credit actual sizes after PUT (HEAD on server). Best-effort —
        // failures do not roll back the upload; absolute reconcile heals drift.
        if uploaded_count == all_seqs.len() && !all_seqs.is_empty() {
            if let Err(e) = self
                .auth
                .confirm_log_upload_for_target(target, &all_seqs)
                .await
            {
                tracing::warn!(
                    target: "fold_db::sync",
                    error = %e,
                    count = all_seqs.len(),
                    "confirm_log_upload metering failed (non-fatal; reconcile will heal)"
                );
            }
        }

        Ok(UploadOutcome {
            entries_uploaded: uploaded_count,
            max_seq_uploaded,
            partial_error: None,
            transfer_bytes_uploaded,
            transfer_elapsed_secs,
        })
    }

    /// [`Self::upload_mutation_log_segments`] with an explicit PUT fan-out.
    ///
    /// `None` keeps the adaptive upload-policy concurrency. The continuous
    /// cycle passes `Some(n)` when it carries a backlog: the policy pins
    /// concurrency to 1 under `interactive_busy`, which on the primary is
    /// effectively always set, and a backlog of one small segment per record
    /// then drains at one serial PUT per record. Every PUT in the call must
    /// still land before the call returns Ok, so published F ordering does not
    /// depend on the fan-out.
    pub(crate) async fn upload_mutation_log_segments_with_put_concurrency(
        &self,
        target: &SyncTarget,
        segments: &[crate::sync::engine::MutationLogSegment],
        put_concurrency: Option<usize>,
    ) -> SyncResult<u64> {
        if segments.is_empty() {
            return Ok(0);
        }

        // The empty target prefix is the internal selector for the personal
        // database. It is safe only when AuthClient will attach a concrete
        // db_hash to every request. Never fall back to the principal root for
        // new mutation-log objects because that recreates one mixed cloud head
        // for every database owned by the account.
        if target.prefix.trim().is_empty()
            && self
                .auth
                .db_hash_scope()
                .is_none_or(|db_hash| db_hash.trim().is_empty())
        {
            return Err(SyncError::Storage(
                "personal mutation-log upload requires a non-empty db_hash; refusing principal-root upload"
                    .to_string(),
            ));
        }

        // The continuous mutation-log plane runs before the ordinary
        // download/upload partition in `do_sync`. Prove the prefix here, at
        // the last common point before any raw segment can be presigned or
        // PUT. On a genuinely empty prefix this plants the bounded
        // `keycheck.enc` proof object first; later cycles can therefore prove
        // the key without opening an arbitrarily large first segment.
        //
        // PR #1436 added keycheck but only reached this proof after the
        // mutation-log publisher had already uploaded its first object. That
        // ordering reproduced `BackupBootstrapBlocked` on a real-backlog CoW
        // rehearsal. Keeping the gate adjacent to the actual write also
        // covers callers outside `do_sync` and preserves fail-closed behavior
        // for established or indeterminate prefixes.
        self.prove_prefix_decryptable(target).await?;

        let mut bytes_uploaded = 0u64;
        for chunk in segments.chunks(MAX_PRESIGN_BATCH) {
            let max_sealed = chunk
                .iter()
                .map(|s| s.payload.len() as u64)
                .max()
                .unwrap_or(1024)
                .max(1);
            let urls = self
                .auth
                .presign_upload_segments(
                    target,
                    &chunk
                        .iter()
                        .map(|segment| segment.segment.clone())
                        .collect::<Vec<_>>(),
                    Some(max_sealed),
                )
                .await?;
            if urls.len() != chunk.len() {
                return Err(SyncError::Auth(format!(
                    "expected {} presigned log-segment URLs for '{}', got {}",
                    chunk.len(),
                    target.label,
                    urls.len()
                )));
            }
            let cap = match put_concurrency {
                Some(explicit) => explicit.max(1),
                None => self.active_upload_caps().await.concurrency.max(1),
            };
            // Iterate OWNED `((seq, bytes), url)` items. Borrowing the iterator
            // item here trips the higher-ranked-lifetime bound when this future
            // is later spawned by the sync coordinator — the same trap called
            // out in `upload_entries_chunk`.
            // clippy::needless_collect is wrong here: the collect is what makes
            // the stream items OWNED. Feeding `chunk.iter()` straight into the
            // closure yields `&MutationLogSegment` and trips the
            // higher-ranked-lifetime bound when the sync coordinator spawns
            // this future ("implementation of `FnOnce` is not general enough").
            #[allow(clippy::needless_collect)]
            let owned: Vec<(u64, Vec<u8>)> = chunk
                .iter()
                .map(|s| (s.segment.through_id, s.payload.clone()))
                .collect();
            let mut put_stream = futures::stream::iter(owned.into_iter().zip(urls).map(
                |((seq, bytes), url)| async move {
                    self.retry_s3(&format!("upload mutation-log segment {seq}"), || {
                        let url = url.clone();
                        let bytes = bytes.clone();
                        async move { self.s3.upload(&url, bytes).await }
                    })
                    .await
                },
            ))
            .buffer_unordered(cap);

            let mut first_err: Option<SyncError> = None;
            let mut ok = 0usize;
            while let Some(res) = put_stream.next().await {
                match res {
                    Ok(()) => ok += 1,
                    Err(e) => {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            if let Some(e) = first_err {
                return Err(e);
            }
            // Only confirm a fully-landed chunk. Metering confirm is
            // best-effort (absolute reconcile heals drift) and must never fail
            // the publish that already succeeded.
            if ok == chunk.len() {
                bytes_uploaded = bytes_uploaded
                    .saturating_add(chunk.iter().map(|s| s.payload.len() as u64).sum::<u64>());
                if let Err(e) = self
                    .auth
                    .confirm_log_upload_segments_for_target(
                        target,
                        &chunk
                            .iter()
                            .map(|segment| segment.segment.clone())
                            .collect::<Vec<_>>(),
                    )
                    .await
                {
                    tracing::warn!(
                        target: "fold_db::sync::mutation_log",
                        error = %e,
                        count = chunk.len(),
                        "confirm_log_upload for mutation-log segments failed (non-fatal; reconcile heals)"
                    );
                }
            }
        }
        Ok(bytes_uploaded)
    }

    /// Best-effort delete of server-allocated seq objects that already landed
    /// before a sibling PUT in the same scoped chunk failed.
    ///
    /// Without this, a re-alloc on the next cycle permanently advances the
    /// scoped counter while the partial objects remain, so org download's
    /// non-contiguous listing check hard-fails forever (manual object cleanup
    /// required). Delete failures are non-fatal: we still surface the original
    /// PUT error and hope a later compact/operator path heals stragglers.
    async fn rollback_scoped_partial_puts(&self, target: &SyncTarget, seqs: &[u64]) {
        if seqs.is_empty() {
            return;
        }
        let mut sorted: Vec<u64> = seqs.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        tracing::warn!(
            target: "fold_db::sync",
            target = %target.label,
            count = sorted.len(),
            first = sorted.first().copied().unwrap_or(0),
            last = sorted.last().copied().unwrap_or(0),
            "scoped upload partial PUT: rolling back orphan seq objects before re-alloc retry"
        );
        for chunk in sorted.chunks(MAX_PRESIGN_BATCH) {
            match self.auth.presign_log_delete_target(target, chunk).await {
                Ok(urls) => {
                    for url in urls {
                        if let Err(e) = self.s3.delete(&url).await {
                            tracing::warn!(
                                target: "fold_db::sync",
                                target = %target.label,
                                error = %e,
                                "scoped partial-PUT rollback delete failed (non-fatal)"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "fold_db::sync",
                        target = %target.label,
                        error = %e,
                        "scoped partial-PUT rollback presign_log_delete failed (non-fatal)"
                    );
                }
            }
        }
    }
}
