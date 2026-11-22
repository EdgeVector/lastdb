//! Relay client and restore-from-start for signed history.
//!
//! Relays store and forward only. This crate pages each device stream from
//! `writer_seq = 1`, runs local [`mutation_log::VerifiedHistory::accept_entry`],
//! and returns the rebuilt history. No snapshots or shortcuts.

mod relay;
mod restore;

pub use relay::RelayClient;
pub use restore::{restore_from_relay, RestoreReport, RestoreRequest, StreamRef};
