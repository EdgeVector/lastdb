//! `folddb_profile` — the cross-environment endpoint registry and the
//! `FOLDDB_HOME` / tilde path helpers.
//!
//! This is a small leaf crate. The production node and the observability
//! crate resolve service endpoints and the node home from here. They do not
//! take a heavier dependency to do that.
#![warn(missing_docs)]

pub mod endpoints;
pub mod paths;
