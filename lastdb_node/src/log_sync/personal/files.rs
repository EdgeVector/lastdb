//! File bytes of a record, as they enter and leave a sealed blob.
//!
//! Re-exports the shared helpers in [`crate::log_sync::record_files`]. Personal
//! and shared databases use that one path.

pub(super) use super::super::record_files::{is_file_of, sha256_hex, ENVELOPE_OVERHEAD};
