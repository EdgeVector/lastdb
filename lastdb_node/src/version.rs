//! Contract version constants for app-LastDB compatibility.

pub const CURRENT_API_VERSION: u32 = crate::lastdb_uds::uds_router::API_VERSION;
pub const MIN_SUPPORTED_API_VERSION: u32 = 29;
pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = 1;
pub const MIN_SUPPORTED_OUTBOX_FORMAT_VERSION: u32 = 1;

pub fn build_version() -> &'static str {
    env!("FOLDDB_BUILD_VERSION")
}
