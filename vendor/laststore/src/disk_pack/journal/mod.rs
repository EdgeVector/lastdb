//! Complete immutable recovery batches for separate disk-pack artifacts.
//!
//! This module does not enable a home writer or change a memory ack.
//! Explicit publication checkpoints the directory before a fresh stream admits work.

mod admission;
mod batch;
mod marker;
mod quote;
mod reader;
mod reclaim;
mod resume;
mod stream;
mod tail;
mod writer;

pub use admission::{JournalPressure, JournalReservation};
use batch::BatchPlan;
pub use batch::{JournalMutation, RecoveredBatch, RecoveredMutation};
pub use quote::{JournalMutationShape, JournalQuote};
pub use reader::JournalRecovery;
pub use reclaim::ReclaimPlan;
pub use tail::{JournalTail, TailPredecessor};
pub use writer::{DurableBatch, JournalWriter};

const MAX_BATCH_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SUFFIX_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SUFFIX_BATCHES: u64 = 4096;

fn refuse(reason: &'static str) -> crate::Error {
    crate::Error::Config(format!("disk-pack journal: {reason}"))
}

fn capacity(reason: &'static str) -> crate::Error {
    crate::Error::WriteCapacity(reason.into())
}
