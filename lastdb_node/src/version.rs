//! Contract version constants for app-LastDB compatibility.

pub const MIN_SUPPORTED_API_VERSION: u32 = 38;

/// Registry listings are filed under this version, not under `API_VERSION`.
///
/// `APP_API_VERSION` and [`MIN_SUPPORTED_APP_API_VERSION`] change only when
/// `/v1/read`, `/v1/write`, or the extension mechanism changes in a way that
/// breaks an installed app. Adding an extension does not change them. An
/// owner-side socket change does not change them. `API_VERSION` stays the
/// socket version for the CLI and the SDK.
pub const APP_API_VERSION: u32 = 1;
/// Lowest app API version whose registry lists this node still reads.
///
/// Same change rule as [`APP_API_VERSION`].
pub const MIN_SUPPORTED_APP_API_VERSION: u32 = 1;

pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = 1;
pub const MIN_SUPPORTED_OUTBOX_FORMAT_VERSION: u32 = 1;

pub fn build_version() -> &'static str {
    env!("FOLDDB_BUILD_VERSION")
}
