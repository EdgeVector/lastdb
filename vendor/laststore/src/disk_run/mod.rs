//! Disk format v2 run files: one immutable, sorted run of records.
//!
//! A run holds strictly ascending keys. Each record is a key and either a
//! value or a tombstone. The file is:
//!
//! ```text
//! data block 0 .. data block n-1   front-coded records, one sealed frame each
//! index block 0 .. index block m-1 first key and place of each data block,
//!                                  plus a bloom filter of the keys they cover
//! top block                        first key and place of each index block
//! footer (64 B, clear)             magic, run id, place of the top block
//! ```
//!
//! Every block is one [`crate::frame`] frame: zstd when it helps, then
//! AES-256-GCM under a key derived from the home key and the run id. The
//! frame counter names the block, so a block moved to another place or
//! another run fails authentication.
//!
//! A reader keeps nothing after its call: [`RunReader::open`] reads the
//! footer and the top block, and each lookup reads one index block and one
//! data block. Callers open a reader for one call and drop it after.

mod bloom;
mod codec;
mod footer;
mod merge;
mod range;
mod reader;
mod writer;

pub use merge::{merge_runs, MergeIter};
pub use range::RunRange;
pub use reader::RunReader;
pub use writer::{RunSummary, RunWriter};

/// File name suffix of a complete run.
pub const RUN_SUFFIX: &str = ".ldrun";

/// Largest stored key. Larger keys are refused at write time.
pub const MAX_RUN_KEY_BYTES: usize = 64 * 1024;

/// Largest stored value. A record larger than the block target gets a block
/// of its own; this bound keeps one block inside the frame limit.
pub const MAX_RUN_VALUE_BYTES: usize = crate::frame::MAX_DECOMPRESSED_FRAME_BYTES / 2;

/// Largest decoded top block. A writer refuses a run whose top block would
/// be larger, so every finished run opens.
pub const MAX_TOP_BYTES: usize = crate::frame::MAX_DECOMPRESSED_FRAME_BYTES;

/// Largest entry count of one index or top block. The writer starts a new
/// index block at this count, and a reader refuses a larger claim.
pub const MAX_INDEX_ENTRIES: usize = 1 << 24;

/// One record as a run stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    /// The key holds this value.
    Value(Vec<u8>),
    /// The key was deleted. A newer run's tombstone hides older values.
    Tombstone,
}

impl Found {
    /// The value, or `None` for a tombstone.
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Value(value) => Some(value),
            Self::Tombstone => None,
        }
    }
}

/// Block sizes for a new run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunOptions {
    /// Target decoded size of a data block.
    pub block_bytes: usize,
    /// Target decoded size of the entries in an index block.
    pub index_bytes: usize,
    /// Bloom filter bits for each key. Zero writes no filter.
    pub bloom_bits_per_key: u32,
}

impl Default for RunOptions {
    /// 16 KiB blocks and 10 bloom bits per key (about 1% false positives),
    /// the shape the disk size spike measured (EdgeVector/lastdb#388).
    fn default() -> Self {
        Self {
            block_bytes: 16 * 1024,
            index_bytes: 16 * 1024,
            bloom_bits_per_key: 10,
        }
    }
}

/// Frame counters: data block `n` uses `n`; index block `n` uses
/// `INDEX_COUNTER_BASE + n`; the top block uses `TOP_COUNTER`.
pub(crate) const INDEX_COUNTER_BASE: u64 = 1 << 62;
pub(crate) const TOP_COUNTER: u64 = 1 << 63;
