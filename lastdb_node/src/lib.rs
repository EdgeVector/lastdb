//! Library surface of the minimal `lastdbd` daemon.

pub mod allocator;
pub mod app_publish;
pub mod app_registry_index;
pub mod app_release_host;
pub mod app_storage;
mod change_feed_queue;
pub mod cloud;
mod cloud_activation;
mod cloud_clients;
pub mod deliver;
pub mod distribution;
pub mod ephemeral;
pub mod exec;
pub mod home_storage;
pub mod host;
pub mod local_outbox;
pub mod log_rotation;
pub mod maintenance_write_gate;
pub mod offline_home;
pub mod offline_recount;
pub mod ops;
mod primary_resume_job;
pub mod proof_replay;
pub mod retention_audit;
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
    blob_dwell, crash_attribution, crash_telemetry, footprint, gauge, health_alert, log_filter,
    request_telemetry, self_metrics, session_ledger, status_gauge_contract, ttl_sweep,
};
pub use watch_gate::{WatchGate, WatchGateSnapshot, WatchSlot};
