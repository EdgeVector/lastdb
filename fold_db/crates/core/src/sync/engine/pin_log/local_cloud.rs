use super::*;

pub(super) fn log_entry_namespace(entry: &LogEntry) -> &str {
    match &entry.op {
        crate::sync::log::LogOp::Put { namespace, .. }
        | crate::sync::log::LogOp::Delete { namespace, .. }
        | crate::sync::log::LogOp::BatchPut { namespace, .. }
        | crate::sync::log::LogOp::BatchDelete { namespace, .. } => namespace,
        crate::sync::log::LogOp::LogicalCommit { .. } => "logical_commit",
        crate::sync::log::LogOp::MutationIntent { .. } => "mutation_intent",
        crate::sync::log::LogOp::PhysicalDigest { .. } => "physical_digest",
        crate::sync::log::LogOp::Unknown { .. } => "unknown",
    }
}

/// In-process catch-up cloud plane (S + log + CAS latest) for pin-mode publish
/// and restore proofs. Production maps this onto laststore/S3 object keys under
/// the target's prefix; tests and the CoW harness use this local plane.
#[derive(Debug, Default, Clone)]
pub struct PinModeLocalCloud {
    pub(super) published: std::collections::HashMap<String, PinModePublishedTarget>,
}

#[derive(Debug, Clone)]
pub(super) struct PinModePublishedTarget {
    pub(super) desc: PinModePublishDescriptor,
    pub(super) sealed_objects: std::collections::HashMap<String, Vec<u8>>,
    pub(super) log: Vec<PinLogRecord>,
}

/// Outcome of restoring a fresh home from a pin-mode published plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PinModeRestoreReport {
    pub target_id: String,
    pub include_log: bool,
    pub base_objects: usize,
    pub log_entries: usize,
    pub app_keys_restored: usize,
    pub latest_counter: u64,
}

impl PinModeLocalCloud {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latest(&self, target_id: &str) -> Option<&PinModePublishDescriptor> {
        self.published.get(target_id).map(|p| &p.desc)
    }

    pub fn log_len(&self, target_id: &str) -> usize {
        self.published.get(target_id).map_or(0, |p| p.log.len())
    }

    pub fn base_object_count(&self, target_id: &str) -> usize {
        self.published
            .get(target_id)
            .map_or(0, |p| p.sealed_objects.len())
    }
}
