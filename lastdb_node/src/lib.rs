//! Library surface of the minimal `lastdbd` daemon.
//!
//! The binary delegates host boot and owner-socket data routes to this library.
//! Administrative modules share those product types without a second daemon.

pub mod app_publish;
pub mod app_registry_index;
pub mod backup_runtime;
pub mod byte_cloud_backup;
pub mod byte_cloud_log;
pub mod byte_cloud_restore;
pub mod cloud;
pub use crate::cloud as backup_access;
pub mod cloud_backup_client;
pub mod exec;
pub mod host;
pub mod identity;
pub mod jobs;
pub mod lastdb_uds;
pub mod legacy_cutover;
pub mod log_envelope;
pub mod log_rotation;
pub mod log_sync;
pub mod named_grant;
pub mod named_snapshot_restore;
pub mod node_read;
pub mod offline_home;
pub mod profile;
pub mod registered_share;
pub mod saved_query;
pub mod saved_query_run;
pub mod seal;
pub mod service_home;
pub mod share_cache;
pub mod share_contribution;
pub mod share_file;
pub mod share_invitation;
pub mod share_publication;
pub mod share_spec;
pub mod shutdown_receipt;
pub mod version;

pub use host::Host;
pub use jobs::ttl_sweep;
