//! Background dwell pass: delete the local file blobs whose count is zero and
//! whose dwell has passed.
//!
//! The blob counter (`fold_db::db_operations::AtomStore::blob_dwell_scan`)
//! keeps a time-ordered queue of the blobs at zero. Each sampler tick starts at
//! most one pass. A pass reads the head page of the queue, which is one range
//! read under one hash, and deletes the due blobs in chunks. A chunk takes the
//! native write gate for its own reads and its one batch, and the gate is free
//! between chunks. The pass takes the local mutation admission per chunk, as the
//! TTL sweep does, so a backup cut never starts inside a chunk.
//!
//! The dwell is `LASTDB_BLOB_DWELL_SECS` (one hour by default). Kill switch for
//! the pass: `LASTDB_BLOB_DWELL_PASS=0`. Default **on**. With the pass off no
//! blob leaves the store, and the queue grows until the pass runs again.

use fold_db::clock::unix_millis;
use fold_db::db_operations::DWELL_CHUNK;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::Instrument;

use crate::host::Host;

/// Default-on kill switch. Off only for the explicit falsey values.
pub const BLOB_DWELL_PASS_ENV: &str = "LASTDB_BLOB_DWELL_PASS";

/// Scans of one pass. Each scan reads one page of the queue, so a pass handles
/// at most this many pages. A longer queue continues on the next tick.
const MAX_SCANS_PER_PASS: usize = 16;

#[must_use]
pub fn blob_dwell_pass_enabled() -> bool {
    !matches!(
        std::env::var(BLOB_DWELL_PASS_ENV)
            .ok()
            .as_deref()
            .map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

/// Spawn one dwell pass off the sampler tick, at most one at a time. The pass
/// stops between chunks once `shutdown` is set.
pub fn maybe_spawn_blob_dwell_pass(
    host: &std::sync::Arc<Host>,
    shutdown: &std::sync::Arc<AtomicBool>,
) {
    if !blob_dwell_pass_enabled() {
        return;
    }
    let Some(guard) = host.db.db_ops().atoms().begin_blob_dwell_pass() else {
        return;
    };
    let host = std::sync::Arc::clone(host);
    let shutdown = std::sync::Arc::clone(shutdown);
    tokio::spawn(
        async move {
            let _guard = guard;
            run_pass(&host, &shutdown).await;
        }
        .in_current_span(),
    );
}

/// Run one bounded dwell pass.
pub async fn run_pass(host: &Host, shutdown: &AtomicBool) {
    let atoms = host.db.db_ops().atoms();
    for _ in 0..MAX_SCANS_PER_PASS {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let scan = match atoms.blob_dwell_scan(unix_millis()).await {
            Ok(scan) => scan,
            Err(error) => {
                tracing::warn!(target: "lastdbd::blob_dwell", error = %error, "blob dwell scan failed");
                return;
            }
        };
        if scan.due.is_empty() {
            return;
        }
        for chunk in scan.due.chunks(DWELL_CHUNK) {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            let delete = atoms.blob_dwell_delete(chunk, unix_millis());
            match crate::maintenance_write_gate::run(host, delete).await {
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    tracing::warn!(target: "lastdbd::blob_dwell", error = %error, "blob dwell delete failed");
                    return;
                }
                // Local writes are paused for a retained cloud source.
                None => return,
            }
            tokio::task::yield_now().await;
        }
        if !scan.capped {
            return;
        }
    }
}
