//! A memory-only monotonic floor; no adoption capability or recovery record.

use super::AuthClient;
use crate::sync::cloud_purge::protocol::{CloudAccount, CloudControlGet};
use crate::sync::{SyncError, SyncResult};
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct ObservedAccounts(BTreeMap<CloudAccount, ObservedControl>);

struct ObservedControl {
    generation: u64,
    revision: u64,
    operation: String,
    replacement: Option<String>,
    continuation: Option<String>,
}

impl AuthClient {
    pub(super) fn observe_cloud_control(&self, response: &CloudControlGet) -> SyncResult<()> {
        if response.notification_only {
            return Ok(());
        }
        if response.notification.is_some() {
            return Err(held(
                "a foreign notification cannot replace account authority",
            ));
        }
        let account = response
            .account
            .as_ref()
            .or_else(|| response.control.as_ref().map(|c| &c.account))
            .filter(|account| !account.id.is_empty())
            .ok_or_else(|| held("cloud control did not resolve its account"))?;
        let mut observed = self
            .cloud_generation_state
            .lock()
            .map_err(|_| held("cloud generation guard lock failed"))?;
        let Some(control) = response.control.as_ref() else {
            if response.protocol_required
                || response.generation != 0
                || observed.0.contains_key(account)
            {
                return Err(held("cloud control is required, absent, or moved backward"));
            }
            return Ok(());
        };
        control.validate_identity().map_err(held)?;
        if !response.protocol_required
            || response.generation != control.generation
            || account != &control.account
        {
            return Err(held("cloud control account or generation is inconsistent"));
        }
        let next = ObservedControl {
            generation: control.generation,
            revision: control.revision,
            operation: control.operation_id.clone(),
            replacement: digest(control.replacement_manifest.as_ref())?,
            continuation: digest(control.continuation.as_ref())?,
        };
        if let Some(previous) = observed.0.get(account) {
            if next.generation < previous.generation
                || (next.generation == previous.generation
                    && (next.revision < previous.revision
                        || next.operation != previous.operation
                        || (previous.replacement.is_some()
                            && next.replacement != previous.replacement)
                        || (previous.continuation.is_some()
                            && next.continuation != previous.continuation)))
            {
                return Err(held(
                    "cloud control moved backward or changed its committed identity",
                ));
            }
        } else if observed.0.len() >= 1024 {
            return Err(held("cloud generation guard account capacity reached"));
        }
        observed.0.insert(account.clone(), next);
        Ok(())
    }
}

fn digest(value: Option<&serde_json::Value>) -> SyncResult<Option<String>> {
    value
        .map(|value| Ok(crate::hex::sha256_hex(serde_json::to_vec(value)?)))
        .transpose()
}

fn held(message: impl Into<String>) -> SyncError {
    SyncError::CloudPurgeHeld(message.into())
}
