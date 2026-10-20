//! Peer-apply interlock: has this node ever applied a mutation that a peer wrote?
//!
//! When two devices write the same key, LastDB keeps each conflicting write.
//! The replay path that modern peers use records neither side of such a
//! conflict (see `docs/lastdb-atom-refcount-grace-window-gc.md`), so the loser
//! is an ordinary zero-count atom and automatic garbage collection would
//! delete it. Until replay records those writes, deleting unreferenced atoms is
//! allowed only on a node that has NEVER applied a peer mutation.
//!
//! One durable row, `peer-apply:v1`, holds the answer:
//!
//! - `never`: written once, by the boot that creates a brand-new home.
//! - `seen`: written, and flushed, BEFORE the first peer data is applied.
//! - absent: `unknown`. Every home created by an older build is unknown, and so
//!   is a home whose row a copy or a restore dropped. Unknown counts as seen.
//!
//! `never -> seen` is the only transition. The row is node-local: it is on the
//! capture skip list, replay drops it, and a snapshot neither carries nor
//! restores it, so no other node can set or clear it.
//!
//! Every writer and reader is listed in the PR text and in
//! `docs/lastdb-atom-refcount-grace-window-gc.md`.

use super::AtomStore;
use crate::schema::SchemaError;
use crate::storage::KvMutation;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;

/// Main key of the node-local peer-apply row.
pub const PEER_APPLY_KEY: &str = "peer-apply:v1";

const NEVER: &[u8] = b"never";
const SEEN: &[u8] = b"seen";

/// Operator override. Off unless set to `1`, `true`, `yes` or `on`. With it set,
/// automatic delete runs on a node that has applied a peer mutation or whose
/// history is unknown, so the atoms of a LOST multi-device write can be deleted.
pub const ALLOW_AFTER_PEER_APPLY_ENV: &str = "LASTDB_GC_ATOMS_ALLOW_AFTER_PEER_APPLY";

/// Whether the operator override is set in this process.
#[must_use]
pub fn allow_after_peer_apply_from_env() -> bool {
    matches!(
        std::env::var(ALLOW_AFTER_PEER_APPLY_ENV)
            .ok()
            .as_deref()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// What this node knows about peer data in its store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PeerApplyState {
    /// Created by this build; no peer mutation has been applied.
    Never,
    /// A peer mutation was applied, or may have been.
    Seen,
    /// No durable answer. Treated as `Seen`.
    Unknown,
}

impl PeerApplyState {
    /// Stable text for the status and the logs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::Seen => "seen",
            Self::Unknown => "unknown",
        }
    }
}

impl AtomStore {
    /// Read the durable state. A decoded `seen` is cached for the process: the
    /// state never moves back.
    pub async fn peer_apply_state(&self) -> Result<PeerApplyState, SchemaError> {
        if self.peer_apply_seen.load(Ordering::Acquire) {
            return Ok(PeerApplyState::Seen);
        }
        let row = self
            .main_store
            .inner()
            .get(PEER_APPLY_KEY.as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read peer-apply state: {error}")))?;
        Ok(match row.as_deref() {
            Some(SEEN) => {
                self.peer_apply_seen.store(true, Ordering::Release);
                PeerApplyState::Seen
            }
            Some(NEVER) => PeerApplyState::Never,
            // Absent or unreadable: no proof either way.
            _ => PeerApplyState::Unknown,
        })
    }

    /// Record, durably, that this node is about to apply peer data.
    ///
    /// The row is written and flushed BEFORE the caller applies anything. A
    /// crash after the write leaves `seen` with nothing applied (safe); no
    /// order leaves peer data applied with the row unset. Idempotent. Free
    /// after the first SUCCESS in a process.
    ///
    /// "Present" and "durable" are different facts. After a failed flush the
    /// row may read `seen` from memory while the disk still says `never`, so a
    /// retry reads the row, writes it when it is absent, and ALWAYS flushes
    /// before it reports success. Only a flush that returned `Ok` sets the
    /// durable flag that lets later calls return at once.
    pub async fn mark_peer_apply_seen(&self) -> Result<(), SchemaError> {
        if self.peer_apply_durable.load(Ordering::Acquire) {
            return Ok(());
        }
        // One writer at a time, and never beside the `never` stamp.
        let _guard = self.peer_apply_lock.lock().await;
        self.write_and_flush_peer_apply_seen().await
    }

    /// Write `seen` again even when this process already made it durable. A
    /// snapshot restore clears the row from the store, so the flag cannot
    /// vouch for it. The flag is cleared under the lock, so a `seen` write that
    /// is already running cannot leave it set over the cleared row. The `seen`
    /// cache stays set: it only refuses deletes.
    pub async fn rewrite_peer_apply_seen(&self) -> Result<(), SchemaError> {
        let _guard = self.peer_apply_lock.lock().await;
        self.peer_apply_durable.store(false, Ordering::Release);
        self.write_and_flush_peer_apply_seen().await
    }

    /// The body of the `seen` write. The caller holds `peer_apply_lock`.
    async fn write_and_flush_peer_apply_seen(&self) -> Result<(), SchemaError> {
        if self.peer_apply_durable.load(Ordering::Acquire) {
            return Ok(());
        }
        let current = self
            .main_store
            .inner()
            .get(PEER_APPLY_KEY.as_bytes())
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read peer-apply state: {error}")))?;
        let wrote = current.as_deref() != Some(SEEN);
        if wrote {
            self.main_store
                .inner()
                .batch_mutate(vec![KvMutation::put(
                    PEER_APPLY_KEY.as_bytes().to_vec(),
                    SEEN,
                )])
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("write peer-apply state: {error}"))
                })?;
        }
        // The row is visible now; deleters must refuse even if the flush fails.
        self.peer_apply_seen.store(true, Ordering::Release);
        self.main_store.inner().flush().await.map_err(|error| {
            SchemaError::InvalidData(format!("flush peer-apply state: {error}"))
        })?;
        self.peer_apply_durable.store(true, Ordering::Release);
        if wrote {
            tracing::info!(
                target: "fold_db::gc_atoms",
                "peer data is about to be applied; automatic atom delete stays off on this node"
            );
        }
        Ok(())
    }

    /// The interlock for every deleter that finds atoms by the absence of a
    /// reference. `Ok` only when this node has never applied a peer mutation, or
    /// the caller (or the operator override env) allows it explicitly. A read
    /// error refuses: not knowing is not a pass.
    pub async fn require_no_peer_apply_for_atom_delete(
        &self,
        explicit_override: bool,
        verb: &str,
    ) -> Result<(), SchemaError> {
        if explicit_override || allow_after_peer_apply_from_env() {
            return Ok(());
        }
        let why = match self.peer_apply_state().await {
            Ok(PeerApplyState::Never) => return Ok(()),
            Ok(PeerApplyState::Seen) => "this node has applied a peer mutation",
            Ok(PeerApplyState::Unknown) => {
                "this node cannot prove it never applied a peer mutation"
            }
            Err(_) => "the peer-apply state could not be read",
        };
        Err(SchemaError::InvalidData(format!(
            "{verb} refused: {why}, and the atom of a lost multi-device write is an ordinary unreferenced atom. \
             Pass the explicit allow-after-peer-apply option (or set {ALLOW_AFTER_PEER_APPLY_ENV}=1 for the daemon) \
             to accept that those atoms can be deleted"
        )))
    }
}
