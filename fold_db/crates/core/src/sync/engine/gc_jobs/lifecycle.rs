use super::*;

impl GcJobManager {
    /// Persist the job and bounded discovery index before returning its ID.
    /// A caller-supplied UUID makes a lost acceptance response safe to retry.
    pub(crate) fn accept(
        &self,
        trigger: &str,
        dry_run: bool,
        id: Option<&str>,
    ) -> SyncResult<(GcJobReceipt, bool)> {
        let mut guard = self.index()?;
        let index = guard.as_mut().expect("index initialized");
        let id = id.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
        let path = self.job_path(&id)?;
        if path.exists() {
            let mut job: GcJobReceipt = read(&path)?;
            if job.trigger != trigger || job.dry_run != dry_run {
                return Err(io_error("request ID reused with different GC parameters"));
            }
            if !job.state.terminal() && !index.active.contains(&id) {
                job.state = GcJobState::Interrupted;
                job.phase = "terminal".into();
                job.stop_reason =
                    Some("acceptance interrupted before index commit; no replay".into());
                job.stop_code = Some(GcStopCode::AcceptanceInterrupted);
                job.progress_unix_ms = unix_millis();
                put_job(&path, &mut job)?;
            }
            return Ok((job, false));
        }
        if index.active.len() >= MAX_ACTIVE {
            return Err(io_error("GC job queue full; attach to an existing job"));
        }
        if index.retained_jobs >= MAX_RETAINED_JOBS {
            return Err(io_error("GC_METADATA_CAPACITY_REQUIRED: terminal job archive is full; export and explicit migration are required; no evidence was pruned"));
        }
        let mut job = GcJobReceipt {
            version: GC_RECEIPT_MIN_VERSION,
            job_id: id.clone(),
            trigger: trigger.into(),
            dry_run,
            state: GcJobState::Queued,
            phase: "queued".into(),
            created_unix_ms: unix_millis(),
            progress_unix_ms: unix_millis(),
            stop_reason: None,
            stop_code: None,
            report: None,
            selection: None,
            objects_reconciled: 0,
            receipt_capacity: 0,
            delete_acknowledged: 0,
            acknowledged_bytes: 0,
            failed_before_dispatch: 0,
            failed_bytes: 0,
            unknown: 0,
            unknown_bytes: 0,
            dispositions: None,
            pending: None,
            pending_batch: Vec::new(),
        };
        // Reserve first: a failed admission may leave an unindexed summary,
        // and that evidence must consume bounded archive capacity too.
        let mut reserved = index.clone();
        reserved.retained_jobs += 1;
        self.commit_index(&mut guard, reserved)?;
        // Summary before active index. An unindexed summary cannot run.
        // A retry of that exact ID becomes Interrupted, never a silent replay.
        put_job(&path, &mut job)?;
        let mut committed = guard.as_ref().expect("reserved index").clone();
        committed.active.push(id.clone());
        committed.latest = Some(id);
        self.commit_index(&mut guard, committed)?;
        self.live
            .lock()
            .map_err(|_| io_error("liveness lock poisoned"))?
            .insert(job.job_id.clone());
        Ok((job, true))
    }

    pub(crate) fn get(&self, id: Option<&str>) -> SyncResult<Option<GcJobReceipt>> {
        let guard = self.index()?;
        let id = id.or_else(|| guard.as_ref().and_then(|index| index.latest.as_deref()));
        id.map(|id| {
            let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
            if !job.state.terminal()
                && !guard
                    .as_ref()
                    .expect("index initialized")
                    .active
                    .iter()
                    .any(|active| active == id)
            {
                job.state = GcJobState::Interrupted;
                job.phase = "terminal".into();
                job.stop_reason = Some("unindexed acceptance; no automatic replay".into());
                job.stop_code = Some(GcStopCode::UnindexedAcceptance);
                put_job(&self.job_path(id)?, &mut job)?;
            }
            Ok(job)
        })
        .transpose()
    }

    pub(crate) fn objects(
        &self,
        id: &str,
        after: u64,
        limit: usize,
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        let job = self.get(Some(id))?.ok_or_else(|| io_error("job missing"))?;
        let end = job.objects_reconciled + job.in_flight_len();
        (after.saturating_add(1)..=end.min(after.saturating_add(limit.min(256) as u64)))
            .map(|sequence| read(&self.object_path(id, sequence)?))
            .collect()
    }

    pub(crate) fn update(
        &self,
        id: &str,
        change: impl FnOnce(&mut GcJobReceipt),
    ) -> SyncResult<()> {
        let mut guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
        if job.state.terminal() {
            return Err(io_error("job is terminal"));
        }
        change(&mut job);
        job.progress_unix_ms = unix_millis();
        put_job(&self.job_path(id)?, &mut job)?;
        if job.state.terminal() {
            let index = guard.as_mut().expect("index initialized");
            let mut committed = index.clone();
            committed.active.retain(|active| active != id);
            self.live
                .lock()
                .map_err(|_| io_error("liveness lock poisoned"))?
                .remove(id);
            self.commit_index(&mut guard, committed)?;
        }
        Ok(())
    }

    pub(crate) fn phase(&self, id: &str, phase: &str) -> SyncResult<()> {
        self.update(id, |job| {
            job.state = GcJobState::Active;
            job.phase = phase.into();
        })
    }
}
