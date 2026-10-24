//! Preflight the whole entry before any raw write or replay frontier change.

use super::super::types::KeyRewriteOp;
use super::super::SyncEngine;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::{LogEntry, LogOp};
use crate::sync::org_sync::SyncTarget;
use crate::sync::{ReplayCause, ReplayOperation};

impl SyncEngine {
    pub(super) fn require_counter_writer(
        entry: &LogEntry,
        target: Option<&SyncTarget>,
    ) -> SyncResult<()> {
        let check = |namespace: &str, key: &str, deleting: bool| -> SyncResult<()> {
            let op = if deleting {
                KeyRewriteOp::Delete
            } else {
                KeyRewriteOp::Write
            };
            // Invalid framing keeps its existing poison classification.
            let Ok(key) = Self::rewrite_key_if_needed(namespace, key, target, op) else {
                return Ok(());
            };
            if target.is_none() && Self::is_org_scoped_storage_key_bytes(&key) {
                return Ok(());
            }
            if crate::storage::current_keys::counter_owned(namespace, &key)? {
                return Err(SyncError::replay_refused(
                    namespace,
                    entry.seq,
                    ReplayOperation::from(&entry.op),
                    ReplayCause::CounterWriterRequired,
                ));
            }
            Ok(())
        };
        match &entry.op {
            LogOp::Put { namespace, key, .. } => check(namespace, key, false)?,
            LogOp::Delete { namespace, key } => check(namespace, key, true)?,
            LogOp::BatchPut { namespace, items } => {
                for (key, _) in items {
                    check(namespace, key, false)?;
                }
            }
            LogOp::BatchDelete { namespace, keys } => {
                for key in keys {
                    check(namespace, key, true)?;
                }
            }
            LogOp::LogicalCommit { changes } => {
                for change in changes {
                    check(&change.namespace, &change.key, change.value.is_none())?;
                }
            }
            // Complete mutations use FoldDB's normal counted writer.
            LogOp::MutationIntent { .. } | LogOp::PhysicalDigest { .. } | LogOp::Unknown { .. } => {
            }
        }
        Ok(())
    }
}
