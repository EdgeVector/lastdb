//! In-memory [`MutationLog`] adapter for the unified mutation log.
//!
//! There is no flag. Call [`InMemoryMutationLog::new`]. The daemon does not
//! construct this adapter yet, so its behavior is unchanged. This crate does
//! not read the process environment, open a socket, or call Exemem.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

mod memory;
mod port;

pub use memory::InMemoryMutationLog;
pub use port::MutationLog;
