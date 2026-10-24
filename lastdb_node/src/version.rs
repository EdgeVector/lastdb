//! Contract version constants for app-LastDB compatibility.
//!
//! LastDB has two contract surfaces:
//! 1. **HTTP API** (`POST /api/mutation`, `POST /api/query`, etc.)
//! 2. **Outbox format** (IndexChangeBatch JSON files for vector indexing apps)
//!
//! Each surface has its own version identifier, allowing independent evolution.

/// The current HTTP API version this node speaks.
///
/// Re-exported from `lastdb_uds::uds_router::API_VERSION`. This is the
/// request-grammar version reported by `GET /api/version` and checked during
/// app install resolution.
pub const CURRENT_API_VERSION: u32 = lastdb_uds::uds_router::API_VERSION;

/// The oldest HTTP API version this node still supports.
///
/// Apps declaring `api_version` less than this are incompatible and will be
/// rejected at install time. When a breaking HTTP API change lands, bump
/// `CURRENT_API_VERSION` but leave this unchanged for at least two releases
/// (e.g., 6 weeks) to provide a deprecation window.
pub const MIN_SUPPORTED_API_VERSION: u32 = 1;

/// The current outbox format version this node writes.
///
/// Governs the IndexChangeBatch JSON schema written to the search inbox
/// (`{LASTDB_HOME}/apps/search/inbox/`). Consumed by lastseek, Search, and
/// other vector indexing apps that process the outbox.
pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = 1;

/// The oldest outbox format version this node still understands.
///
/// Apps declaring `outbox_format_version` less than this are incompatible and
/// will be rejected at install time. When a breaking outbox format change
/// lands, bump `CURRENT_OUTBOX_FORMAT_VERSION` but leave this unchanged for at
/// least two releases to provide a deprecation window.
pub const MIN_SUPPORTED_OUTBOX_FORMAT_VERSION: u32 = 1;
