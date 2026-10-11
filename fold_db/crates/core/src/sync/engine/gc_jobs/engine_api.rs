use super::*;

impl SyncEngine {
    pub fn backup_gc_job(&self, id: Option<&str>) -> SyncResult<Option<GcJobReceipt>> {
        self.backup_gc_jobs.get(id)
    }

    pub fn backup_gc_objects(
        &self,
        id: &str,
        after: u64,
        limit: usize,
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        self.backup_gc_jobs.objects(id, after, limit)
    }

    /// Detached daemon work: acceptance never reads a manifest, lists cloud
    /// objects, or waits for the publication/single-executor locks.
    pub fn start_backup_gc(
        self: &Arc<Self>,
        dry_run: bool,
        request_id: Option<&str>,
    ) -> SyncResult<GcJobReceipt> {
        let (job, fresh) = self.backup_gc_jobs.accept("manual", dry_run, request_id)?;
        if fresh {
            let engine = Arc::clone(self);
            let id = job.job_id.clone();
            // lint:spawn-bare-ok daemon-owned durable GC, not request-scoped
            tokio::spawn(async move {
                use futures::FutureExt;
                let result = std::panic::AssertUnwindSafe(async {
                    engine.backup_gc_jobs.phase(&id, "resolve_keep")?;
                    let manifests = engine.resolve_admin_backup_gc_keep_set().await?;
                    engine
                        .gc_orphan_backup_chunks_for_job(&manifests, dry_run, &id)
                        .await
                        .map(Some)
                })
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(io_error("GC task panic; no automatic replay")));
                if let Err(error) = engine.backup_gc_jobs.finish(&id, &result) {
                    tracing::error!(target: "fold_db::sync::backup", job_id = %id, error = %error, "GC terminal receipt write failed; durable intent remains");
                }
            });
        }
        Ok(job)
    }
}
