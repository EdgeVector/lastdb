//! Library surface of the minimal `lastdbd` daemon.
//!
//! The binary delegates host boot and owner-socket data routes to this library.
//! Administrative modules share those product types without a second daemon.

pub mod allocator;
pub mod app_publish;
pub mod app_registry_index;
pub mod app_release_host;
pub mod app_storage;
pub mod backup_access;
pub mod backup_runtime;
pub mod byte_cloud_backup;
pub mod byte_cloud_restore;
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
pub mod maintenance_write_gate;
pub mod native_cloud_backup;
pub mod native_cloud_restore;
pub mod native_transfer_target;
pub mod offline_home;
pub mod offline_range_conversion;
pub mod offline_range_inventory;
pub mod offline_recount;
pub mod ops;
pub mod retention_audit;
pub mod schema_resolver_host;
pub mod schema_sync_audit;
pub mod seal;
pub mod service_home;
pub mod shared_surface;
pub mod version;
pub mod volume_isolation;
pub mod watch_gate;

pub use host::Host;
pub use local_outbox::{LocalOutbox, LocalWatchEvent, LocalWatchPoll};
pub use ops::{
    blob_dwell, crash_attribution, crash_telemetry, footprint, gauge, health_alert, log_filter,
    request_telemetry, self_metrics, session_ledger, status_gauge_contract, ttl_sweep,
};
pub use watch_gate::{WatchGate, WatchGateSnapshot, WatchSlot};
