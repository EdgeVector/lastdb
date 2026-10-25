//! Required read work uses one held call's actual finite limits.

use super::*;

impl ReadContext {
    pub(in crate::storage::disk_pack) fn record_stage(&self, stage: &'static str, items: usize) {
        if !tracing::enabled!(target: "lastdb::native_read_budget", tracing::Level::DEBUG) {
            return;
        }
        let call = crate::read_receipts::lock(&self.call);
        let costs = call.costs();
        let limits = call.budget();
        tracing::debug!(
            target: "lastdb::native_read_budget",
            stage,
            items,
            examined_keys = costs.examined_keys,
            key_limit = limits.keys,
            metadata_pages = costs.metadata_pages,
            payload_blocks = costs.payload_blocks,
            charged_bytes = costs.budget_bytes,
            "native preparation cumulative read costs"
        );
    }

    /// Charge required memory results before their body is copied. A write
    /// preparation call keeps one aggregate budget and never resets on error.
    pub(in crate::storage::disk_pack) fn account_memory_record(
        &self,
        call: &mut PhysicalRead,
        key_bytes: usize,
        body_bytes: usize,
    ) -> StorageResult<()> {
        let result = key_bytes
            .checked_add(body_bytes)
            .ok_or_else(|| StorageError::ReadResourceLimit {
                detail: "native memory record length overflow".into(),
            })
            .and_then(|bytes| {
                self.require_room(call, 1, bytes as u64)?;
                call.account_memory_record(key_bytes, body_bytes)
                    .map_err(|error| call_error(error, call))
            });
        self.remember(&result);
        result
    }

    pub(super) fn require_room(
        &self,
        call: &PhysicalRead,
        keys: u64,
        bytes: u64,
    ) -> StorageResult<()> {
        if let Some(error) = self.error() {
            return Err(error);
        }
        let costs = call.costs();
        let limits = call.budget();
        if costs
            .examined_keys
            .checked_add(keys)
            .is_none_or(|n| n > limits.keys)
            || costs
                .budget_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > limits.bytes)
        {
            return Err(StorageError::ReadResourceLimit {
                detail: format!(
                    "native required work: examined_keys={}+{keys}/{}, charged_bytes={}+{bytes}/{}",
                    costs.examined_keys, limits.keys, costs.budget_bytes, limits.bytes,
                ),
            });
        }
        Ok(())
    }
}

pub(super) fn call_error(error: laststore::Error, call: &PhysicalRead) -> StorageError {
    if !matches!(error, laststore::Error::ReadBudgetExceeded) {
        return map(error);
    }
    let costs = call.costs();
    let limits = call.budget();
    let pages = costs.metadata_pages;
    StorageError::ReadResourceLimit {
        detail: format!(
            "native call: examined_keys={}/{}, metadata_pages={}/{}, payload_blocks={}/{}, charged_bytes={}/{}",
            costs.examined_keys, limits.keys, pages, limits.pages,
            costs.payload_blocks, limits.blocks, costs.budget_bytes, limits.bytes,
        ),
    }
}

impl PackRuntime {
    /// End a successful list page before the next refill can exhaust its held call.
    /// The unused half remains available for membership and response materialization.
    pub(crate) fn list_page_should_yield(&self) -> bool {
        let Some(context) = ReadContext::captured(self) else {
            return false;
        };
        let call = crate::read_receipts::lock(&context.call);
        let costs = call.costs();
        let limits = call.budget();
        costs.examined_keys >= limits.keys / 2
            || costs.budget_bytes >= limits.bytes / 2
            || costs.payload_blocks >= limits.blocks / 2
            || costs.metadata_pages >= limits.pages / 2
    }
}
