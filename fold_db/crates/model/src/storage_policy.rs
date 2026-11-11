//! One source for native size bounds and local backup resource policy.
#![warn(missing_docs)]

/// Maximum native files in one backup.
pub const BACKUP_FILES_MAX: usize = 65_552;

/// Resource limits for backup working buffers and local spooling.
#[derive(Clone, Copy, Debug)]
pub struct BackupResources {
    /// Working-memory allowance, in bytes.
    pub working_memory_bytes: u64,
    /// Allowance for decoded values, in bytes.
    pub decoded_values_bytes: u64,
    /// Per-stream buffer size, in bytes.
    pub stream_buffer_bytes: u64,
    /// Concurrent streams run by the backup policy.
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
pub mod backup_transport {
    /// Maximum bytes in one opaque cloud object chunk.
    pub const OBJECT_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
    /// Maximum bytes in one metadata pack.
    pub const METADATA_PACK_BYTES: u64 = 8 * 1024 * 1024;
    /// Byte size used to classify a native file as small for transport planning.
    pub const SMALL_NATIVE_FILE_BYTES: u64 = 64 * 1024;
    /// Maximum members in one metadata pack.
    pub const MAX_PACK_MEMBERS: usize = 512;
}

/// Native read resources.
pub mod native_read {
    /// Eager work allowance across stored frames, decoded blocks, and returned records.
    pub const EAGER_WORK_BYTES: u64 = 4 * super::native_write::PENDING_MEMORY_BYTES;
    /// Maximum 4 KiB blocks decoded by one eager read.
    pub const EAGER_BLOCKS: u64 = super::native_write::PENDING_MEMORY_BYTES.div_ceil(4_096) + 1;
}

/// Pending native writes share this memory allowance across complete requests.
pub mod native_write {
    /// Shared memory allowance, in bytes, for pending native writes.
    pub const PENDING_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
    /// Refuse oversized local-file inputs before any file-sized allocation.
    pub const LOCAL_FILE_INPUT_MEMORY_BYTES: u64 = PENDING_MEMORY_BYTES;
}

/// Small control values and restore decode memory bounds.
pub mod backup_decode {
    /// Maximum record body bytes admitted during backup decode.
    pub const RECORD_BODY_MAX_BYTES: u64 = 4 * 1024 * 1024;
    /// Read allowance, in bytes, for directory reads during restore.
    pub const DIRECTORY_READ_BYTES: u64 = 8 * 1024 * 1024;
    /// Read allowance, in bytes, for a record inventory.
    pub const RECORD_INVENTORY_READ_BYTES: u64 = 64 * 1024 * 1024;
    /// Maximum record-inventory pages considered during backup decode.
    pub const RECORD_INVENTORY_PAGES: u64 = 4096;
    /// Read allowance, in bytes, for a native stream lookup.
    pub const NATIVE_STREAM_LOOKUP_READ_BYTES: u64 = 128 * 1024 * 1024;
    /// Maximum native stream blocks considered during a lookup.
    pub const NATIVE_STREAM_BLOCKS: u64 = 16_384;
    /// Reserved bytes for one JSON page during backup decode.
    pub const JSON_PAGE_RESERVE_BYTES: u64 = 4 * 1024 * 1024;
    /// Reserved bytes for the restore spool.
    pub const RESTORE_SPOOL_RESERVE_BYTES: u64 = 16 * 1024 * 1024;
    /// Memory bound, in bytes, for one receipt.
    pub const RECEIPT_MEMORY_BYTES: u64 = 512;
    /// Memory bound, in bytes, for one proof identity.
    pub const IDENTITY_MEMORY_BYTES: u64 = 192;
}

const _: () = assert!(
    BACKUP_RESOURCES.stream_buffer_bytes > 0
        && BACKUP_RESOURCES.stream_buffer_bytes <= (i32::MAX - 16) as u64
        && BACKUP_RESOURCES.stream_buffer_bytes <= (usize::MAX - 16) as u64
);
const _: () = assert!(BACKUP_RESOURCES.concurrent_streams > 0);
