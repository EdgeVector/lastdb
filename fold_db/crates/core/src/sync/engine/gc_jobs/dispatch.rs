use super::*;

impl GcJobManager {
    pub(super) fn apply_outcome(
        job: &mut GcJobReceipt,
        object: &GcObjectReceipt,
    ) -> SyncResult<()> {
        let pending = job
            .in_flight_objects()
            .into_iter()
            .next()
            .ok_or_else(|| io_error("no dispatch intent"))?;
        if pending.sequence != object.sequence
            || pending.key != object.key
            || object.job_id != job.job_id
            || object.sequence != job.objects_reconciled + 1
            || pending.listed_bytes != object.listed_bytes
        {
            return Err(io_error("object receipt does not match dispatch intent"));
        }
        let bytes = object.listed_bytes;
        // Version 1 counters keep their meaning. A precise version 2
        // disposition also feeds the version 1 counter it refines:
        // deleted_now and already_absent are both provider acknowledgements;
        // uncertain_write is a DELETE that may still complete remotely.
        match object.outcome {
            GcObjectOutcome::DeleteAcknowledged => {
                job.delete_acknowledged += 1;
                job.acknowledged_bytes += bytes;
            }
            GcObjectOutcome::FailedBeforeDispatch => {
                job.failed_before_dispatch += 1;
                job.failed_bytes += bytes;
            }
            GcObjectOutcome::Unknown => {
                job.unknown += 1;
                job.unknown_bytes += bytes;
            }
            GcObjectOutcome::DeletedNow => {
                job.delete_acknowledged += 1;
                job.acknowledged_bytes += bytes;
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.deleted_now += 1;
                counts.deleted_now_bytes += bytes;
            }
            GcObjectOutcome::AlreadyAbsent => {
                job.delete_acknowledged += 1;
                job.acknowledged_bytes += bytes;
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.already_absent += 1;
                counts.already_absent_bytes += bytes;
            }
            GcObjectOutcome::Failed => {
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.failed += 1;
                counts.failed_bytes += bytes;
            }
            GcObjectOutcome::Protected => {
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.protected += 1;
                counts.protected_bytes += bytes;
            }
            GcObjectOutcome::UncertainWrite => {
                job.unknown += 1;
                job.unknown_bytes += bytes;
                let counts = job.dispositions.get_or_insert_with(Default::default);
                counts.uncertain_write += 1;
                counts.uncertain_write_bytes += bytes;
            }
            GcObjectOutcome::Intent => return Err(io_error("intent is not an outcome")),
        }
        let rest: Vec<GcObjectReceipt> = job
            .in_flight_objects()
            .into_iter()
            .filter(|pending| pending.sequence != object.sequence)
            .collect();
        job.set_in_flight(rest);
        job.objects_reconciled = object.sequence;
        if let Some(report) = job.report.as_mut() {
            report.deleted = job.delete_acknowledged as usize;
            let failed_after_dispatch = job.dispositions.map_or(0, |counts| counts.failed);
            report.failed =
                (job.failed_before_dispatch + job.unknown + failed_after_dispatch) as usize;
        }
        Ok(())
    }

    pub(crate) fn outcome(
        &self,
        mut object: GcObjectReceipt,
        outcome: GcObjectOutcome,
    ) -> SyncResult<()> {
        let guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(&object.job_id)?)?;
        object.outcome = outcome;
        Self::apply_outcome(&mut job, &object)?;
        put_object(
            &self.object_path(&object.job_id, object.sequence)?,
            &mut object,
        )?;
        job.progress_unix_ms = unix_millis();
        put_job(&self.job_path(&object.job_id)?, &mut job)?;
        drop(guard);
        Ok(())
    }

    /// Persist N intents, then one directory fsync. No DELETE until this
    /// returns. Sequence order matches `shas`.
    pub(crate) fn intent_batch(
        &self,
        id: &str,
        shas: &[(String, u64)],
    ) -> SyncResult<Vec<GcObjectReceipt>> {
        if shas.is_empty() {
            return Ok(Vec::new());
        }
        if shas
            .iter()
            .any(|(sha, _)| !super::backup_keys::is_sha256_hex(sha))
        {
            return Err(io_error(
                "GC_INTENT_NOT_A_V1_CHUNK_DIGEST: legacy_backup_chunk receipts take a 64-hex sha; no DELETE was dispatched",
            ));
        }
        let mut guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(id)?)?;
        if job.state.terminal() || job.in_flight_len() != 0 {
            return Err(io_error("invalid dispatch transition"));
        }
        let needed = job.objects_reconciled.saturating_add(shas.len() as u64);
        while job.receipt_capacity < needed {
            let index = guard.as_mut().expect("index initialized");
            if index.reserved_object_receipts >= MAX_RESERVED_OBJECT_RECEIPTS {
                return Err(io_error("GC_METADATA_CAPACITY_REQUIRED: object receipt archive is full; no evidence was pruned and no DELETE was dispatched"));
            }
            let mut committed = index.clone();
            committed.reserved_object_receipts += RECEIPT_RESERVATION_BATCH;
            self.commit_index(&mut guard, committed)?;
            job.receipt_capacity += RECEIPT_RESERVATION_BATCH;
        }
        let mut objects = Vec::with_capacity(shas.len());
        for (offset, (sha, bytes)) in shas.iter().enumerate() {
            let object = GcObjectReceipt {
                version: GC_RECEIPT_MIN_VERSION,
                job_id: id.into(),
                sequence: job.objects_reconciled + offset as u64 + 1,
                object_class: "legacy_backup_chunk".into(),
                key: format!("{}{sha}", super::backup_keys::BACKUP_CHUNKS_PREFIX),
                listed_bytes: *bytes,
                outcome: GcObjectOutcome::Intent,
                instance_id: None,
                provider_version: None,
            };
            put_file(&self.object_path(id, object.sequence)?, &object)?;
            objects.push(object);
        }
        let job_path = self.job_path(id)?;
        sync_parent(&self.object_path(id, objects[0].sequence)?)?;
        job.set_in_flight(objects.clone());
        job.phase = "delete".into();
        job.progress_unix_ms = unix_millis();
        put(&job_path, &job)?;
        drop(guard);
        Ok(objects)
    }

    pub(crate) fn outcome_batch(
        &self,
        items: Vec<(GcObjectReceipt, GcObjectOutcome)>,
    ) -> SyncResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let job_id = items[0].0.job_id.clone();
        let guard = self.index()?;
        let mut job: GcJobReceipt = read(&self.job_path(&job_id)?)?;
        for (mut object, outcome) in items {
            object.outcome = outcome;
            Self::apply_outcome(&mut job, &object)?;
            put_file(&self.object_path(&job_id, object.sequence)?, &object)?;
        }
        sync_parent(&self.object_path(&job_id, job.objects_reconciled)?)?;
        job.progress_unix_ms = unix_millis();
        put(&self.job_path(&job_id)?, &job)?;
        drop(guard);
        Ok(())
    }

    pub(crate) fn finish(
        &self,
        id: &str,
        result: &SyncResult<Option<BackupOrphanGcReport>>,
    ) -> SyncResult<()> {
        if let Some(job) = self.get(Some(id))? {
            for pending in job.in_flight_objects() {
                let object: GcObjectReceipt = read(&self.object_path(id, pending.sequence)?)?;
                let outcome = if object.outcome == GcObjectOutcome::Intent {
                    GcObjectOutcome::Unknown
                } else {
                    object.outcome.clone()
                };
                self.outcome(object, outcome)?;
            }
        }
        self.update(id, |job| {
            let (state, reason, code) = match result {
                Ok(Some(report)) => {
                    job.report = Some(report.clone());
                    if report.superseded {
                        (
                            GcJobState::Superseded,
                            Some("publication_state_changed".into()),
                            Some(GcStopCode::PublicationStateChanged),
                        )
                    } else if report.failed > 0 {
                        (
                            GcJobState::PartialFailure,
                            Some("object_failures; inspect receipts".into()),
                            Some(GcStopCode::ObjectFailures),
                        )
                    } else {
                        (GcJobState::Completed, None, None)
                    }
                }
                Ok(None) => (
                    GcJobState::Superseded,
                    Some("newer_generation".into()),
                    Some(GcStopCode::NewerGeneration),
                ),
                Err(error) => {
                    let text = error.to_string();
                    (
                        GcJobState::Failed,
                        Some(crate::sync::error::redact_sync_error_text(&text)),
                        Some(GcStopCode::from_failure_text(&text)),
                    )
                }
            };
            job.state = state;
            job.stop_reason = reason;
            job.stop_code = code;
            job.phase = "terminal".into();
        })
    }
}
