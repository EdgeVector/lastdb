//! Resource bounds for field/record sizes and backup/native memory admission.

pub mod content_limit {
    pub const DEFAULT_MAX_FIELD_VALUE_BYTES: usize = 256 * 1024;
    pub const ABSOLUTE_MAX_FIELD_VALUE_BYTES: usize = 1024 * 1024;
    pub const MIN_MAX_FIELD_VALUE_BYTES: usize = 1024;
    pub const MAX_FIELD_VALUE_BYTES_ENV: &str = "LASTDB_MAX_FIELD_VALUE_BYTES";
    pub const RECORD_TOO_LARGE_HINT: &str = "A record holds every field of one key, and its \
        encoded value must fit the 64 KiB record cap. Store large bytes as the record's one file \
        (`put-blob`), or write a list that grows as one record per entry in a HashRange schema \
        (hash = partition, range = entry key).";
}

pub mod storage_policy {
    pub const BACKUP_FILES_MAX: usize = 65_552;

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

    pub mod native_read {
        pub const EAGER_WORK_BYTES: u64 = 4 * super::native_write::PENDING_MEMORY_BYTES;
        pub const EAGER_BLOCKS: u64 = super::native_write::PENDING_MEMORY_BYTES.div_ceil(4_096) + 1;
    }

    pub mod native_write {
        pub const PENDING_MEMORY_BYTES: u64 = 512 * 1024 * 1024;
        pub const LOCAL_FILE_INPUT_MEMORY_BYTES: u64 = PENDING_MEMORY_BYTES;
    }

    pub mod backup_decode {
        pub const RECORD_BODY_MAX_BYTES: u64 = 4 * 1024 * 1024;
        pub const DIRECTORY_READ_BYTES: u64 = 8 * 1024 * 1024;
        pub const RECORD_INVENTORY_READ_BYTES: u64 = 64 * 1024 * 1024;
        pub const RECORD_INVENTORY_PAGES: u64 = 4096;
        pub const NATIVE_STREAM_LOOKUP_READ_BYTES: u64 = 128 * 1024 * 1024;
        pub const NATIVE_STREAM_BLOCKS: u64 = 16_384;
        pub const JSON_PAGE_RESERVE_BYTES: u64 = 4 * 1024 * 1024;
        pub const RESTORE_SPOOL_RESERVE_BYTES: u64 = 16 * 1024 * 1024;
        pub const RECEIPT_MEMORY_BYTES: u64 = 512;
        pub const IDENTITY_MEMORY_BYTES: u64 = 192;
    }

    const _: () = assert!(
        BACKUP_RESOURCES.stream_buffer_bytes > 0
            && BACKUP_RESOURCES.stream_buffer_bytes <= (i32::MAX - 16) as u64
            && BACKUP_RESOURCES.stream_buffer_bytes <= (usize::MAX - 16) as u64
    );
    const _: () = assert!(BACKUP_RESOURCES.concurrent_streams > 0);
}
