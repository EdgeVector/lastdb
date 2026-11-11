//! Contract version constants for app-LastDB compatibility.

/// Current HTTP API version (`GET /api/version`).
pub const CURRENT_API_VERSION: u32 = crate::lastdb_uds::uds_router::API_VERSION;

/// Oldest HTTP API version this node still supports.
pub const MIN_SUPPORTED_API_VERSION: u32 = 29;

/// Current outbox format version written to `{LASTDB_HOME}/apps/search/inbox/`.
pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = 1;

/// Oldest outbox format version this node still understands.
pub const MIN_SUPPORTED_OUTBOX_FORMAT_VERSION: u32 = 1;

/// Compile-time build stamp (`FOLDDB_BUILD_VERSION`) set by `build.rs`.
pub fn build_version() -> &'static str {
    env!("FOLDDB_BUILD_VERSION")
}
