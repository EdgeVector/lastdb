//! Replay quarantine tombstones.

use super::super::*;
use crate::sync::error::{SyncError, SyncResult};

impl SyncEngine {
    pub(crate) fn replay_quarantine_tombstone_key(prefix: &str, seq: u64) -> String {
        format!("log:{prefix}:{seq:020}")
    }

    pub(crate) async fn save_replay_quarantine_tombstone(
        &self,
        prefix: &str,
        seq: u64,
    ) -> SyncResult<()> {
        let kv = self
            .cursor_store
            .open_namespace("sync_replay_quarantine")
            .await
            .map_err(|e| {
                SyncError::Storage(format!(
                    "failed to open sync_replay_quarantine namespace for '{prefix}' seq {seq}: {e}"
                ))
            })?;
        kv.put(
            Self::replay_quarantine_tombstone_key(prefix, seq).as_bytes(),
            b"1".to_vec(),
        )
        .await
        .map_err(|e| {
            SyncError::Storage(format!(
                "failed to save sync replay quarantine tombstone for '{prefix}' seq {seq}: {e}"
            ))
        })
    }

    pub(crate) async fn has_replay_quarantine_tombstone(
        &self,
        prefix: &str,
        seq: u64,
    ) -> SyncResult<bool> {
        let kv = self
            .cursor_store
            .open_namespace("sync_replay_quarantine")
            .await
            .map_err(|e| {
                SyncError::Storage(format!(
                    "failed to open sync_replay_quarantine namespace for '{prefix}' seq {seq}: {e}"
                ))
            })?;
        kv.get(Self::replay_quarantine_tombstone_key(prefix, seq).as_bytes())
            .await
            .map(|value| value.is_some())
            .map_err(|e| {
                SyncError::Storage(format!(
                    "failed to read sync replay quarantine tombstone for '{prefix}' seq {seq}: {e}"
                ))
            })
    }
}

/// Upload-side quarantine: durable tombstones for records that were **dropped
/// from the outgoing mutation-log stream and never published**.
///
/// The replay tombstones above stop a poison record being re-applied. These
/// answer a different question, and a worse one: *which mutations does the
/// cloud copy not have?*
///
/// An unsealable `MutationIntent` (its atom is gone locally, so it can never be
/// encrypted for upload) is skipped so one bad row does not fail-close the
/// cycle, and its durable pin-log row is then deleted so the plane does not
/// grow forever. That deletion is permanent and no peer can recover the record.
/// Before 2026-08-22 nothing recorded it: the frontier list was a local `Vec`,
/// the count lived in a process-lifetime field that a restart zeroed, and the
/// row itself was gone. On the primary that was 569 records across 293 distinct
/// missing atoms in 42 hours, with `log_lag`, `upload_backlog` and the RPO
/// headline all reading clean.
///
/// A tombstone per dropped frontier makes the holes enumerable after the fact
/// and survives a restart. Write it **before** the delete — see
/// `SyncEngine::drop_unsealable_pin_log_records`.
impl SyncEngine {
    pub(crate) fn upload_quarantine_tombstone_key(prefix: &str, frontier: u64) -> String {
        format!("log:{prefix}:{frontier:020}")
    }

    /// Record that `frontier` was dropped from the outgoing stream, and why.
    ///
    /// The value is the seal error, which carries the missing atom id and the
    /// field name — the only surviving description of what the hole contains.
    pub(crate) async fn save_upload_quarantine_tombstone(
        &self,
        prefix: &str,
        frontier: u64,
        reason: &str,
    ) -> SyncResult<()> {
        let kv = self
            .cursor_store
            .open_namespace("sync_upload_quarantine")
            .await
            .map_err(|e| {
                SyncError::Storage(format!(
                    "failed to open sync_upload_quarantine namespace for '{prefix}' frontier {frontier}: {e}"
                ))
            })?;
        kv.put(
            Self::upload_quarantine_tombstone_key(prefix, frontier).as_bytes(),
            reason.as_bytes().to_vec(),
        )
        .await
        .map_err(|e| {
            SyncError::Storage(format!(
                "failed to save sync upload quarantine tombstone for '{prefix}' frontier {frontier}: {e}"
            ))
        })
    }
}
