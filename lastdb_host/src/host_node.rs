//! Host services used by the generic owner-socket handlers.
//!
//! Mini's `lastdb_node::Host` supplies core database access, identity, and QoS
//! admission. Handlers receive an access context and use core's guarded query
//! and mutation paths. The host's concrete admission guard stays alive for the
//! operation and releases its slot on drop.

use std::sync::Arc;
use std::time::Duration;

use fold_db::fold_db_core::FoldDB;

use crate::qos::Lane;

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
/// Mini holds its core behind `Arc<FoldDB>` and supplies the concrete QoS guard.
#[async_trait::async_trait]
pub trait HostNode: Send + Sync {
    /// Host-owned admission guard. Held through the operation and released on drop.
    /// Generic handlers retain the concrete guard without a boxed marker trait.
    type OpPermit: Send;

    /// The live core database — query executor, mutation manager, schema
    /// manager, native index, db-ops. The single handle every shared handler
    /// executes against.
    fn fold_db(&self) -> &Arc<FoldDB>;

    /// The node's public key (base64). Used only by the auto-identity route;
    /// never echoed into a caller-visible error.
    fn public_key(&self) -> String;

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
    async fn acquire_op_permit(&self, lane: Lane) -> Result<Self::OpPermit, ReadBusy>;

    /// Wait for write-triggered background side effects that can affect
    /// immediate follow-up reads, bounded by `timeout`.
    async fn wait_for_background_tasks(&self, timeout: Duration) -> bool;
}
