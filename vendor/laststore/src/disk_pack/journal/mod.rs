//! Complete immutable recovery batches for separate disk-pack artifacts.
//!
//! This module does not enable a home writer, change a memory ack, or publish
//! a locator root. Recovery yields verified mutations for the artifact publisher.

mod batch;
mod marker;
mod reader;
mod stream;
mod tail;
mod writer;

pub use batch::{JournalMutation, RecoveredBatch, RecoveredMutation};
pub use reader::JournalRecovery;
pub use tail::JournalTail;
pub use writer::{DurableBatch, JournalWriter};

const MAX_BATCH_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SUFFIX_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SUFFIX_BATCHES: u64 = 4096;
const MAX_OPERATIONS: usize = 4096;

fn refuse(reason: &'static str) -> crate::Error {
    crate::Error::Config(format!("disk-pack journal: {reason}"))
}
