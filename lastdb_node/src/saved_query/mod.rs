//! Saved keyed-query publication.
//!
//! The app names the records. This module checks that the app owns those
//! schemas, then asks `database_policy` which records, fields, and files may
//! leave `personal`. It plans the writes that make one share database match
//! that selection. It does not open a socket, derive a content key, or read
//! a database file. The daemon applies each database's own content key when
//! it serves the ordinary routes.

mod definition;
mod plan;
mod policy;
mod record;

pub use definition::{label_ok, Definition, Link, RangeSelect, Read};
pub use plan::{
    plan, FileMark, LedgerEntry, LiveRow, Plan, Projected, Receipt, RecordKey, Remove, Step, Write,
    MAX_RECORDS,
};
pub use policy::{authorize, destination_binding, DeclareSchema, RawSchema};
pub use record::{saved_query_declare, SavedQuery, SAVED_QUERY_SCHEMA};

/// Definition and ledger texts each stay under this so the saved-query
/// record stays inside the 64 KiB record cap.
pub const MAX_STORED_TEXT: usize = 24_000;

/// Refusal that names the rule and not a field value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishError {
    detail: String,
}

impl PublishError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.detail
    }
}

impl From<database_policy::PolicyError> for PublishError {
    fn from(error: database_policy::PolicyError) -> Self {
        Self::new(error.as_str())
    }
}
