//! Library surface of the Mini daemon and its owner-socket operations.
//!
//! The `lastdbd` binary starts [`host::Host`]. The shared [`exec`] module
//! serves the ordinary socket operations.

pub mod allocator;
pub mod app_publish;
pub mod app_registry_index;
pub mod app_release_host;
pub mod app_storage;
pub mod cloud;
mod cloud_activation;
pub mod cloud_purge_adoption;
pub(crate) mod cloud_purge_adoption_org;
pub(crate) mod cloud_purge_job;
pub mod cloud_purge_runtime;
pub(crate) mod cloud_purge_secrets;
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
    atom_ref_health, crash_attribution, crash_telemetry, footprint, gauge, health_alert,
    log_filter, request_telemetry, self_metrics, session_ledger, status_gauge_contract, ttl_sweep,
};
pub use watch_gate::{WatchGate, WatchGateSnapshot, WatchSlot};
