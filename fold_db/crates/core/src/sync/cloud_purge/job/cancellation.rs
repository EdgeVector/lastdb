//! Cancel the exact saved operation without issuing any begin request.
use super::*;

impl CloudPurgeDriver {
    pub async fn cancel(&self, job: &mut CloudPurgeJob) -> SyncResult<()> {
        job.cancel_requested = true;
        job.save()?;
        // A cancel intent also binds an unaccepted operation. Save the capability
        // first so the same cancellation can recover a lost service response.
        if job.capability_locator.is_none() {
            let mut bytes = Zeroizing::new([0u8; 32]);
            rand::thread_rng().fill_bytes(&mut *bytes);
            let capability = Zeroizing::new(crate::hex::hex_lower(&bytes));
            job.capability_locator = Some(
                self.secrets
                    .put(&job.operation_id, &capability)
                    .await
                    .map_err(SyncError::Storage)?,
            );
            job.save()?;
        }
        let capability = self.capability(job).await?;
        let cancelled = self
            .auth
            .cloud_purge_cancel_intent(
                &job.account,
                &job.operation_id,
                job.control.as_ref().map(|c| c.generation),
                &capability,
            )
            .await?;
        cancelled
            .cancellation_receipt
            .validate(&job.account, &job.operation_id, &capability)
            .map_err(SyncError::Storage)?;
        if let Some(control) = cancelled.control {
            if control.phase != CloudPurgePhase::Cancelled
                || cancelled.cancellation_receipt.cancelled_before_accept
                || cancelled.cancellation_receipt.principal_hash
                    != control.selected_writer.principal_hash
            {
                return Err(SyncError::Storage(
                    "cloud refused pre-delete cancellation".into(),
                ));
            }
            job.require_control(&control)?;
            job.cancellation_receipt = Some(cancelled.cancellation_receipt);
            job.update_control(control)
        } else {
            if !cancelled.cancellation_receipt.cancelled_before_accept || job.control.is_some() {
                return Err(SyncError::Storage(
                    "accepted cancellation needs its exact cancelled control".into(),
                ));
            }
            job.cancellation_receipt = Some(cancelled.cancellation_receipt);
            job.phase = CloudPurgeJobPhase::Cancelled;
            job.last_error = None;
            job.save()
        }
    }
}
