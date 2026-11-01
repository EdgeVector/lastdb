//! One source for native format sizes and local backup resource policy.
//!
//! Native V1 sizes are format identity, not live environment settings. A new
//! size requires a versioned format and compatible reader/publication changes.
//! Resource sizes bound buffers and metadata, never a user file's total bytes.
#![warn(missing_docs)]

/// Bounds that live code still reads from the removed native V1 format.
pub mod native_v1 {
    /// Native V1 block size, in bytes.
    pub const BLOCK_BYTES: usize = 4_096;
    /// Maximum dependency parts in the removed native V1 format.
    pub const MAX_DEPENDENCY_PARTS: usize = 65_536;
    /// Backup file cap: `MAX_DEPENDENCY_PARTS` plus 16.
    pub const MAX_BACKUP_FILES: usize = MAX_DEPENDENCY_PARTS + 16;
    /// Largest native V1 record body, `u32::MAX` bytes.
    pub const RECORD_BODY_MAX_BYTES: u64 = u32::MAX as u64;
}

/// Change the allocation policy here. File length is deliberately absent.
#[derive(Clone, Copy, Debug)]
pub struct BackupResources {
    /// Working-memory allowance, in bytes. Proof and upload peaks must fit in it.
    pub working_memory_bytes: u64,
    /// Allowance for decoded values, in bytes.
    pub decoded_values_bytes: u64,
    /// Per-stream buffer size, in bytes. Zero is rejected by the crate asserts.
    pub stream_buffer_bytes: u64,
    /// How many streams the backup policy runs at once.
    pub concurrent_streams: usize,
    /// Local durable spool allowance, in bytes.
    pub durable_spool_bytes: u64,
}

/// Allocation policy read by native backup code.
pub const BACKUP_RESOURCES: BackupResources = BackupResources {
    working_memory_bytes: 96 * 1024 * 1024,
    decoded_values_bytes: 64 * 1024 * 1024,
    stream_buffer_bytes: 1024 * 1024,
    concurrent_streams: 4,
    durable_spool_bytes: 1024 * 1024 * 1024,
};

/// Existing total-backup policy; this is not a per-file memory allowance.
pub const BACKUP_TOTAL_STORED_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// Opaque cloud object planning, separate from native physical format identity.
/// Chunk length bounds each transfer; it never caps a complete source file.
pub mod backup_transport {
    /// Maximum bytes in one opaque cloud object chunk. This does not cap a source file.
    pub const OBJECT_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
    /// Maximum bytes in one metadata pack.
    pub const METADATA_PACK_BYTES: u64 = 8 * 1024 * 1024;
    /// Byte size used to classify a native file as small for transport planning.
    pub const SMALL_NATIVE_FILE_BYTES: u64 = 64 * 1024;
    /// Maximum members in one metadata pack.
    pub const MAX_PACK_MEMBERS: usize = 512;
}

/// Native read resources. Streams use a separate exact-record work allowance.
pub mod native_read {
    /// Eager work includes stored frames, decoded blocks, returned records and
    /// their plans. Cumulative work is distinct from retained memory admission.
    pub const EAGER_WORK_BYTES: u64 = 4 * super::native_write::PENDING_MEMORY_BYTES;
    /// One eager read can decode the complete pending-write byte allowance.
    /// An unaligned record can span one additional existing payload block.
    pub const EAGER_BLOCKS: u64 = super::native_write::PENDING_MEMORY_BYTES
        .div_ceil(super::native_v1::BLOCK_BYTES as u64)
        + 1;
}

/// Pending native writes share this memory allowance across complete requests.
pub mod native_write {
    /// Shared memory allowance, in bytes, for pending native writes.
    pub const PENDING_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
    /// Refuse impossible local-file inputs before any file-sized allocation.
    pub const LOCAL_FILE_INPUT_MEMORY_BYTES: u64 =
        if PENDING_MEMORY_BYTES < super::native_v1::RECORD_BODY_MAX_BYTES {
            PENDING_MEMORY_BYTES
        } else {
            super::native_v1::RECORD_BODY_MAX_BYTES
        };
}

/// Small control values and proof identities have their own memory bounds.
pub mod backup_decode {
    /// Complete optional key-head cache, including transient key copies.
    pub const PROOF_KEY_HEAD_BYTES: u64 = 1024 * 1024;
    /// Optional authenticated block cursors retain at most this many file handles.
    /// Other inputs keep the streaming path; this never limits backup file count.
    pub const PROOF_BLOCK_CURSOR_HANDLES: usize = 32;
    /// Maximum record body bytes admitted during backup decode.
    pub const RECORD_BODY_MAX_BYTES: u64 = 4 * 1024 * 1024;
    /// Maximum decoded record JSON tree, half of `decoded_values_bytes`.
    pub const RECORD_JSON_TREE_MAX_BYTES: u64 = super::BACKUP_RESOURCES.decoded_values_bytes / 2;
    /// Maximum directory record size, in bytes, for backup decode.
    pub const DIRECTORY_MAX_BYTES: u64 = 4096;
    /// Read allowance, in bytes, for a directory during backup decode.
    pub const DIRECTORY_READ_BYTES: u64 = 8 * 1024 * 1024;
    /// Read allowance, in bytes, for a record inventory.
    pub const RECORD_INVENTORY_READ_BYTES: u64 = 64 * 1024 * 1024;
    /// Maximum record-inventory pages considered during backup decode.
    pub const RECORD_INVENTORY_PAGES: u64 = 4096;
    /// Read allowance, in bytes, for a native stream lookup.
    pub const NATIVE_STREAM_LOOKUP_READ_BYTES: u64 = 128 * 1024 * 1024;
    /// Maximum native stream blocks considered during a lookup.
    pub const NATIVE_STREAM_BLOCKS: u64 = 16_384;
    /// Memory bound, in bytes, for file-coverage state.
    pub const FILE_COVERAGE_MEMORY_BYTES: usize = 8 * 1024 * 1024;
    /// Reserved bytes for one JSON page during backup decode.
    pub const JSON_PAGE_RESERVE_BYTES: u64 = 4 * 1024 * 1024;
    /// Reserved bytes for the restore spool.
    pub const RESTORE_SPOOL_RESERVE_BYTES: u64 = 16 * 1024 * 1024;
    /// Memory bound, in bytes, for one receipt.
    pub const RECEIPT_MEMORY_BYTES: u64 = 512;
    /// Memory bound, in bytes, for one proof identity.
    pub const IDENTITY_MEMORY_BYTES: u64 = 192;
}

/// Backup phases do not overlap proof trees with upload feeders. This includes
/// the bounded HTTP backend's retained chunks, not only the application queue.
pub mod backup_memory {
    use super::{backup_decode, BACKUP_RESOURCES};
    /// Buffers counted per stream when estimating the upload peak.
    pub const UPLOAD_BUFFERS_PER_STREAM: u64 = 6;
    /// Estimated proof-phase peak: decoded values, key heads, directory and coverage state, and four stream buffers.
    pub const PROOF_PEAK_BYTES: u64 = BACKUP_RESOURCES.decoded_values_bytes
        + backup_decode::PROOF_KEY_HEAD_BYTES
        + backup_decode::DIRECTORY_READ_BYTES
        + backup_decode::FILE_COVERAGE_MEMORY_BYTES as u64
        + backup_decode::JSON_PAGE_RESERVE_BYTES
        + 4 * BACKUP_RESOURCES.stream_buffer_bytes;
    /// Estimated upload-phase peak: stream buffers for every concurrent stream, plus page and coverage reserves.
    pub const UPLOAD_PEAK_BYTES: u64 = UPLOAD_BUFFERS_PER_STREAM
        * BACKUP_RESOURCES.concurrent_streams as u64
        * BACKUP_RESOURCES.stream_buffer_bytes
        + 2 * backup_decode::JSON_PAGE_RESERVE_BYTES
        + backup_decode::FILE_COVERAGE_MEMORY_BYTES as u64;
}

// A zero-sized stream buffer can return EOF without authentication. The crypto
// provider also requires room for its final block within its signed length ABI.
const _: () = assert!(
    BACKUP_RESOURCES.stream_buffer_bytes > 0
        && BACKUP_RESOURCES.stream_buffer_bytes <= (i32::MAX - 16) as u64
        && BACKUP_RESOURCES.stream_buffer_bytes <= (usize::MAX - 16) as u64
);
const _: () = assert!(BACKUP_RESOURCES.concurrent_streams > 0);

const _: () = assert!(
    BACKUP_RESOURCES.working_memory_bytes >= backup_memory::PROOF_PEAK_BYTES
        && BACKUP_RESOURCES.working_memory_bytes >= backup_memory::UPLOAD_PEAK_BYTES
);
