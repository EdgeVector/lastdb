//! Fixed numbers and hint text for field-value and record size limits.

pub const DEFAULT_MAX_FIELD_VALUE_BYTES: usize = 256 * 1024;
pub const ABSOLUTE_MAX_FIELD_VALUE_BYTES: usize = 1024 * 1024;
pub const MIN_MAX_FIELD_VALUE_BYTES: usize = 1024;
pub const MAX_FIELD_VALUE_BYTES_ENV: &str = "LASTDB_MAX_FIELD_VALUE_BYTES";
pub const RECORD_TOO_LARGE_HINT: &str = "A record holds every field of one key, and its \
    encoded value must fit the 64 KiB record cap. Store large bytes as the record's one file \
    (`put-blob`), or write a list that grows as one record per entry in a HashRange schema \
    (hash = partition, range = entry key).";
