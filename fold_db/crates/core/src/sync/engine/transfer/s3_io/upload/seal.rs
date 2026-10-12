//! Seal and PUT helpers for [`SyncEngine::upload_entries`].

use super::*;

impl SyncEngine {
    /// Scoped (org/share) targets: allocate server seqs, rewrite each entry's
    /// seq, then seal.
    pub(super) async fn seal_scoped_entries(
        &self,
        target: &SyncTarget,
        entries: &[LogEntry],
    ) -> SyncResult<(
        Vec<(u64, crate::sync::log::SealedLogEntry)>,
        Vec<crate::sync::s3::PresignedUrl>,
    )> {
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
        Ok((sealed, urls))
    }

    /// Personal targets: seal under client-assigned seqs, then presign.
    pub(super) async fn seal_personal_entries(
        &self,
        target: &SyncTarget,
        entries: &[LogEntry],
    ) -> SyncResult<(
        Vec<(u64, crate::sync::log::SealedLogEntry)>,
        Vec<crate::sync::s3::PresignedUrl>,
    )> {
        let mut sealed = Vec::with_capacity(entries.len());
        for entry in entries {
            let s = entry.seal(&target.crypto).await?;
            sealed.push((entry.seq, s));
        }
        let seq_numbers: Vec<u64> = sealed.iter().map(|(seq, _)| *seq).collect();
        // Per-entry ciphertext size for the server quota pre-check.
        // Without this the storage service assumes 1 MiB/entry, so a
        // 1000-entry chunk is billed as ~1 GiB and free-tier (1 GiB)
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
        Ok((sealed, urls))
    }

    /// PUT sealed entries to their presigned URLs with bounded concurrency.
    /// Returns the uploaded count and the S3 transfer time in seconds.
    pub(super) async fn put_sealed_entries(
        &self,
        target: &SyncTarget,
        sealed: Vec<(u64, crate::sync::log::SealedLogEntry)>,
        urls: Vec<crate::sync::s3::PresignedUrl>,
        is_scoped: bool,
    ) -> SyncResult<(usize, f64)> {
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
        Ok((uploaded_count, transfer_elapsed_secs))
    }
}
