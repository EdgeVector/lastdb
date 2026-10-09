//! The [`HostNode`] trait — the framework-agnostic host surface the shared
//! owner-socket [`handlers`](crate::handlers) drive against.
//!
//! Both binaries serve the identical owner-socket wire surface, but they carry
//! their core `fold_db` state on different concrete types:
//! - `lastdb_node::Host` — the minimal daemon's booted core (`Arc<FoldDB>` +
//!   identity keypair), nothing else.
//! - `fold_db_node::FoldNode` — the full desktop node, with the app-identity /
//!   ingestion / discovery subsystems layered on.
//!
//! The **owner-socket** query / mutation / native-search / history / atom
//! handlers do NOT touch the app-identity axis: they receive a pre-built
//! [`AccessContext`] and execute directly against `fold_db` core
//! (`query_with_access` / `write_mutations_with_access` enforce the I3c
//! read-seam / I2 write-guard *inside* core). So this trait abstracts only the
//! small `fold_db`-core surface both hosts share, plus ONE genuine extension
//! point: the QoS admission permit (the host backs it with a
//! [`QosGate`](crate::qos::QosGate); see [`HostNode::acquire_op_permit`]).
//!
//! Keeping this trait — and the [`handlers`](crate::handlers) generic over it —
//! in `lastdb_host` is the point of the convergence: the query/mutation/search
//! EXECUTION logic lives in exactly one place, so the two socket surfaces cannot
//! drift below the app-identity axis.

use std::sync::Arc;
use std::time::Duration;

use fold_db::fold_db_core::FoldDB;

use crate::qos::Lane;

/// A held read-concurrency slot. Dropping it releases the slot. The full node
/// backs this with its process-global read gate; the minimal daemon's permit
/// carries no cost (an empty guard). Held for the duration of a DB-touching
/// read so a burst of scans queues on the semaphore rather than thrashing the
/// sled lock.
pub trait ReadPermit: Send {}

/// Blanket impl so a host may return any `Send` guard type (its own permit, or
/// `()` for the no-op case) as a boxed [`ReadPermit`].
impl<T: Send> ReadPermit for T {}

/// Outcome of failing to acquire a read slot before the host's timeout: the
/// node is saturated and the caller should retry shortly. The shared query
/// handler maps this to a `503` with the suggested retry delay.
#[derive(Debug, Clone, Copy)]
pub struct ReadBusy {
    /// Suggested retry delay (seconds).
    pub retry_after_secs: u64,
}

/// The framework-agnostic host surface the shared owner-socket handlers drive.
///
/// Implemented by `lastdb_node::Host` (minimal daemon) and
/// `fold_db_node::FoldNode` (full node). Every method is cheap: the two hosts
/// both hold their core state behind an `Arc<FoldDB>`, and the only stateful
/// extension point is the read-concurrency gate.
#[async_trait::async_trait]
pub trait HostNode: Send + Sync {
    /// The live core database — query executor, mutation manager, schema
    /// manager, native index, db-ops. The single handle every shared handler
    /// executes against.
    fn fold_db(&self) -> &Arc<FoldDB>;

    /// The node's public key (base64). Used only by the auto-identity route;
    /// never echoed into a caller-visible error.
    fn public_key(&self) -> String;

    /// The node owner's derived user hash (`sha256(pubkey)[..16]` hex). Echoed
    /// back in every success envelope as `user_hash` — both hosts must derive
    /// it identically or socket clients would resolve a different owner per
    /// binary.
    fn owner_user_hash(&self) -> String;

    /// Acquire a QoS admission slot for `lane`, queueing up to the host's
    /// per-lane deadline.
    ///
    /// The host backs this with a [`QosGate`](crate::qos::QosGate): the
    /// [`Lane::Bulk`] lane is capped strictly below the global budget so heavy
    /// blob traffic can never starve the [`Lane::Interactive`] reservation, and
    /// on saturation the acquisition queues up to the lane deadline before
    /// shedding with [`ReadBusy`] (→ `503`) rather than hard-rejecting. The
    /// returned guard is held for the duration of the DB operation and released
    /// on drop. (A gate-free host may still return an empty permit immediately.)
    async fn acquire_op_permit(&self, lane: Lane) -> Result<Box<dyn ReadPermit>, ReadBusy>;

    /// Wait for write-triggered background side effects that can affect
    /// immediate follow-up reads, bounded by `timeout`.
    async fn wait_for_background_tasks(&self, timeout: Duration) -> bool;

    /// Account E2E content key used to open field-level `ENC:` atom content
    /// on index search. Default `None` leaves sealed strings as-is.
    fn atom_content_key(&self) -> Option<[u8; 32]> {
        None
    }
}
