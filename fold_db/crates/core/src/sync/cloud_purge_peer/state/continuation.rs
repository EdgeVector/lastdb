//! Cancellation cannot bypass a prior Ready snapshot that this peer missed.

use super::*;

impl CloudPurgePeer {
    pub(in crate::sync::cloud_purge_peer) fn require_continuation_floor(
        &self,
        control: &CloudPurgeControl,
    ) -> SyncResult<()> {
        let Some(prior) = prior_ready_plan(control, &self.source_id)? else {
            return Ok(());
        };
        let key = account_key(&control.account)?;
        let state = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?;
        let receipt = state
            .accounts
            .get(&key)
            .and_then(|entry| entry.adoption.as_ref())
            .filter(|receipt| proves_floor(receipt, &prior, control.generation))
            .ok_or_else(|| {
                held("the peer must adopt the previous ready replacement before it can continue")
            })?;
        super::super::retention::verify_retention_receipt(receipt)
    }

    pub(in crate::sync::cloud_purge_peer) fn continuation_needs_previous_ready(
        &self,
        control: &CloudPurgeControl,
    ) -> SyncResult<bool> {
        let Some(prior) = prior_ready_plan(control, &self.source_id)? else {
            return Ok(false);
        };
        let key = account_key(&control.account)?;
        let state = self
            .inner
            .lock()
            .map_err(|_| held("cloud peer state lock failed"))?;
        Ok(!state
            .accounts
            .get(&key)
            .and_then(|entry| entry.adoption.as_ref())
            .is_some_and(|receipt| proves_floor(receipt, &prior, control.generation)))
    }
}

fn proves_floor(receipt: &AdoptionReceipt, prior: &AdoptionPlan, generation: u64) -> bool {
    receipt.account == prior.account
        && receipt.source_id == prior.source_id
        && receipt.generation <= generation
        && (receipt.generation > prior.generation || receipt.matches(prior))
}

fn prior_ready_plan(
    control: &CloudPurgeControl,
    source_id: &str,
) -> SyncResult<Option<AdoptionPlan>> {
    let Some(continuation) = control.unchanged_continuation() else {
        return Ok(None);
    };
    let floor = continuation["required_adopted_generation"]
        .as_u64()
        .ok_or_else(|| held("the continuation adoption floor is absent"))?;
    if floor == 0 {
        return Ok(None);
    }
    let previous: CloudPurgeControl = serde_json::from_value(
        control
            .previous_ready_control
            .clone()
            .ok_or_else(|| held("the prior ready control is absent"))?,
    )?;
    let plan = AdoptionPlan::from_control(&previous, source_id)?;
    if plan.account != control.account || plan.generation != floor || plan.continuation.is_some() {
        return Err(held(
            "the prior ready receipt does not match the continuation floor",
        ));
    }
    Ok(Some(plan))
}
