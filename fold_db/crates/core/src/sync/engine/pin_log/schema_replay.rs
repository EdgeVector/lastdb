//! Refresh replayed schema authority before a dependent mutation can use it.

use super::SyncEngine;
use crate::sync::error::SyncResult;
use crate::sync::log::{LogEntry, LogOp};
use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct SchemaReplayBarrier {
    dirty: bool,
}

impl SchemaReplayBarrier {
    pub(super) fn requires_refresh(&self, entry: &LogEntry) -> bool {
        self.dirty && matches!(&entry.op, LogOp::MutationIntent { .. })
    }

    /// Called only after the entry and any required refresh succeed.
    pub(super) fn applied(&mut self, entry: &LogEntry, touched: &mut BTreeSet<String>) {
        if self.requires_refresh(entry) {
            self.dirty = false;
        }
        self.dirty |= changes_schema_authority(&entry.op);
        if let LogOp::LogicalCommit { changes } = &entry.op {
            touched.extend(
                changes
                    .iter()
                    .filter(|change| is_authority(&change.namespace, &change.key))
                    .map(|change| change.namespace.clone()),
            );
        }
    }

    pub(super) async fn finish(&mut self, engine: &SyncEngine) -> SyncResult<()> {
        if self.dirty {
            engine.refresh_replayed_schemas().await?;
            self.dirty = false;
        }
        Ok(())
    }
}

fn changes_schema_authority(op: &LogOp) -> bool {
    match op {
        LogOp::Put { namespace, key, .. } | LogOp::Delete { namespace, key } => {
            is_authority(namespace, key)
        }
        LogOp::BatchPut { namespace, items } => {
            items.iter().any(|(key, _)| is_authority(namespace, key))
        }
        LogOp::BatchDelete { namespace, keys } => {
            keys.iter().any(|key| is_authority(namespace, key))
        }
        LogOp::LogicalCommit { changes } => changes
            .iter()
            .any(|change| is_authority(&change.namespace, &change.key)),
        // Digests have no replay body. Name claims share schema_states.
        _ => false,
    }
}

fn is_authority(namespace: &str, key: &str) -> bool {
    LogOp::decode_bytes(key).map_or(true, |key| {
        crate::db_operations::schema_store::SchemaStore::is_replay_authority_key(namespace, &key)
    })
}
