//! Library surface of the minimal `lastdbd` daemon.
//!
//! The binary ([`main.rs`](../main.rs)) is a thin wrapper; the host boot
//! ([`host::Host`]) and the owner-socket data-route executor ([`exec`]) live
//! here so integration tests (notably the dual-boot cross-binary wire-shape
//! test) can boot a real minimal host and drive the SAME shared owner-socket
//! handlers the full node runs, asserting the two socket surfaces agree.

pub mod allocator;
pub mod app_publish;
pub mod app_registry_index;
pub mod app_release_host;
pub mod app_storage;
pub mod atom_gc_reap;
pub mod attribution_epoch;
mod change_feed_queue;
pub mod cloud;
pub mod deliver;
pub mod distribution;
pub mod ephemeral;
pub mod exec;
pub mod home_storage;
pub mod host;
pub mod local_outbox;
pub mod log_rotation;
pub mod offline_home;
pub mod ops;
mod primary_resume_job;
pub mod schema_resolver_host;
pub mod schema_sync_audit;
pub mod seal;
pub mod service_home;
pub mod shared_surface;
pub mod volume_isolation;
pub mod watch_gate;

pub use host::Host;
pub use local_outbox::{LocalOutbox, LocalWatchEvent, LocalWatchPoll};
pub use ops::{
    atom_ref_backfill, crash_attribution, crash_telemetry, footprint, gauge, health_alert,
    log_filter, request_telemetry, self_metrics, session_ledger, status_gauge_contract,
    status_gauge_gate, ttl_sweep,
};
pub use watch_gate::{WatchGate, WatchGateSnapshot, WatchSlot};
