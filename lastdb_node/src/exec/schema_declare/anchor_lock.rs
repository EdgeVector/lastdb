//! One lock per catalog schema for the length of a declare.
//!
//! The retention guards read the node (who is bound to the catalog schema, who
//! holds a TTL) before they bind a schema, and a TTL is written after the bind.
//! Two declares of the same catalog schema must not interleave: a plain declare
//! could pass its check, a declare with a TTL could then write its policy, and
//! the plain declare would bind afterwards to a schema that already carries the
//! TTL. The declare takes the lock before its pre-check and keeps it until it
//! returns, so the pre-check, the bind and the retention step are one step.
//!
//! The lock lives in this process. One home has one `lastdbd` (the second
//! process is refused at the socket), so the lock covers every writer of a
//! home. The CLI and the routes all reach the node through that one process.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, PoisonError};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type Locks = Mutex<HashMap<String, std::sync::Arc<AsyncMutex<()>>>>;

static ANCHOR_LOCKS: OnceLock<Locks> = OnceLock::new();

/// The guard of one catalog schema's lock. Dropping it releases the lock.
#[derive(Debug)]
pub(crate) struct AnchorGuard {
    anchor: String,
    _guard: OwnedMutexGuard<()>,
}

/// Wait for the lock of `anchor`.
pub(crate) async fn lock_anchor(anchor: &str) -> AnchorGuard {
    let lock = {
        let mut locks = ANCHOR_LOCKS
            .get_or_init(Locks::default)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        locks.entry(anchor.to_string()).or_default().clone()
    };
    AnchorGuard {
        anchor: anchor.to_string(),
        _guard: lock.lock_owned().await,
    }
}

/// The lock a declare holds. It is taken once, by the first bind of the
/// declare, and released when the declare is dropped.
#[derive(Debug, Default)]
pub(crate) struct DeclareLock(Mutex<Option<AnchorGuard>>);

impl DeclareLock {
    /// Hold the lock of `anchor` until this value is dropped. A second call for
    /// the same anchor keeps the lock already held.
    pub(crate) async fn hold(&self, anchor: &str) {
        let held = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|guard| guard.anchor == anchor);
        if held {
            return;
        }
        // Release a lock for another anchor before waiting for this one.
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        let guard = lock_anchor(anchor).await;
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(guard);
    }
}
