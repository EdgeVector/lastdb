//! Clock stamps for atoms.
//!
//! The model types take each time as an argument and read no clock. This
//! extension trait keeps the old call shape for core callers: it reads the
//! system clock and passes the value to the model. Import the trait at the
//! call site.

use chrono::Utc;
use serde_json::Value;

use lastdb_model::atom::Atom;

/// Returns the current time in nanoseconds since the Unix epoch.
#[must_use]
pub fn now_nanos() -> u64 {
    crate::clock::unix_nanos()
}

/// Stamp an [`Atom`] with the current time.
pub trait AtomClock {
    /// A new atom with a content-based UUID and the current time.
    fn new(source_schema_name: String, content: Value) -> Self;
}

impl AtomClock for Atom {
    fn new(source_schema_name: String, content: Value) -> Self {
        Self::new_at(source_schema_name, content, Utc::now())
    }
}
