//! Memory-only tokens bind the exact local adoption identity.

use super::*;

pub(super) struct AdoptionToken {
    generation: u64,
    operation_id: String,
    manifest_sha256: String,
    token: String,
}

impl CloudPurgePeer {
    pub fn complete_adoption(&self, plan: &AdoptionPlan, token: String) -> SyncResult<()> {
        if token.is_empty() || self.adoption_receipt(plan)?.is_none() {
            return Err(held("cloud adoption ack has no durable local receipt"));
        }
        self.tokens
            .lock()
            .map_err(|_| held("cloud peer token lock failed"))?
            .insert(
                account_key(&plan.account)?,
                AdoptionToken {
                    generation: plan.generation,
                    operation_id: plan.operation_id.clone(),
                    manifest_sha256: plan.replacement_manifest_sha256.clone(),
                    token,
                },
            );
        Ok(())
    }

    pub(in crate::sync::cloud_purge_peer) fn adoption_token(
        &self,
        plan: &AdoptionPlan,
    ) -> SyncResult<Option<String>> {
        if self.adoption_receipt(plan)?.is_none() {
            return Ok(None);
        }
        Ok(self
            .tokens
            .lock()
            .map_err(|_| held("cloud peer token lock failed"))?
            .get(&account_key(&plan.account)?)
            .filter(|token| {
                token.generation == plan.generation
                    && token.operation_id == plan.operation_id
                    && token.manifest_sha256 == plan.replacement_manifest_sha256
            })
            .map(|token| token.token.clone()))
    }
}
