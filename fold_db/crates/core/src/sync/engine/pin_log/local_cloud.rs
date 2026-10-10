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
