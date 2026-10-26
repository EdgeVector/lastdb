//! One source for native format sizes and local backup resource policy.
//!
//! Native V1 sizes are format identity, not live environment settings. A new
//! size requires a versioned format and compatible reader/publication changes.
//! Resource sizes bound buffers and metadata, never a user file's total bytes.

/// Fixed sizes used to address existing native V1 files.
pub mod native_v1 {
    pub const PAGE_BYTES: usize = 16_384;
    pub const BLOCK_BYTES: usize = 4_096;
    pub const PAGES_PER_PART: u64 = 512;
    pub const PART_BYTES: u64 = PAGE_BYTES as u64 * PAGES_PER_PART;
    pub const MAP_ENTRIES: u64 = 677;
    pub const MAX_DEPENDENCY_PARTS: usize = 65_536;
    pub const MAX_BACKUP_FILES: usize = MAX_DEPENDENCY_PARTS + 16;
    pub const RECORD_BODY_MAX_BYTES: u64 = u32::MAX as u64;
}

/// Change the allocation policy here. File length is deliberately absent.
#[derive(Clone, Copy, Debug)]
pub struct BackupResources {
    pub working_memory_bytes: u64,
    pub decoded_values_bytes: u64,
    pub stream_buffer_bytes: u64,
    pub concurrent_streams: usize,
    pub durable_spool_bytes: u64,
}

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
    pub const OBJECT_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
    pub const METADATA_PACK_BYTES: u64 = 8 * 1024 * 1024;
    pub const SMALL_NATIVE_FILE_BYTES: u64 = 64 * 1024;
    pub const MAX_PACK_MEMBERS: usize = 512;
}

/// Native read resources. Streams use a separate exact-record work allowance.
pub mod native_read {
    pub const STREAM_RUN_BLOCKS: u32 = 16;
    pub const MAX_READ_HANDLES: usize = 32;
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
    pub const ATOM_BODY_MAX_BYTES: u64 = 4 * 1024 * 1024;
    pub const ATOM_JSON_TREE_MAX_BYTES: u64 = super::BACKUP_RESOURCES.decoded_values_bytes / 2;
    pub const DIRECTORY_MAX_BYTES: u64 = 4096;
    pub const DIRECTORY_READ_BYTES: u64 = 8 * 1024 * 1024;
    pub const ATOM_INVENTORY_READ_BYTES: u64 = 64 * 1024 * 1024;
    pub const ATOM_INVENTORY_PAGES: u64 = 4096;
    pub const NATIVE_STREAM_PAGES: u64 = 512;
    pub const NATIVE_STREAM_LOOKUP_READ_BYTES: u64 = 128 * 1024 * 1024;
    pub const NATIVE_STREAM_BLOCKS: u64 = 16_384;
    pub const FILE_COVERAGE_MEMORY_BYTES: usize = 8 * 1024 * 1024;
    pub const JSON_PAGE_RESERVE_BYTES: u64 = 4 * 1024 * 1024;
    pub const RESTORE_SPOOL_RESERVE_BYTES: u64 = 16 * 1024 * 1024;
    pub const RECEIPT_MEMORY_BYTES: u64 = 512;
    pub const IDENTITY_MEMORY_BYTES: u64 = 192;
}

/// Backup phases do not overlap proof trees with upload feeders. This includes
/// the bounded HTTP backend's retained chunks, not only the application queue.
pub mod backup_memory {
    use super::{backup_decode, BACKUP_RESOURCES};
    pub const UPLOAD_BUFFERS_PER_STREAM: u64 = 6;
    pub const PROOF_PEAK_BYTES: u64 = BACKUP_RESOURCES.decoded_values_bytes
        + backup_decode::DIRECTORY_READ_BYTES
        + backup_decode::FILE_COVERAGE_MEMORY_BYTES as u64
        + backup_decode::JSON_PAGE_RESERVE_BYTES
        + 4 * BACKUP_RESOURCES.stream_buffer_bytes;
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
const _: () = assert!(native_read::STREAM_RUN_BLOCKS > 0 && native_read::MAX_READ_HANDLES > 0);

const _: () = assert!(
    BACKUP_RESOURCES.working_memory_bytes >= backup_memory::PROOF_PEAK_BYTES
        && BACKUP_RESOURCES.working_memory_bytes >= backup_memory::UPLOAD_PEAK_BYTES
);
