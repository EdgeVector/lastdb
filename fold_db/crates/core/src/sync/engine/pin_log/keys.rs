use super::*;

pub(super) fn pin_log_published_f_key(target_id: &str) -> Vec<u8> {
    format!("{PIN_LOG_PUBLISHED_F_PREFIX}{target_id}").into_bytes()
}

/// Rows read per durable page by the bounded pin-log scan.
///
/// This is the cycle's RAM knob: peak is roughly this many records' worth of
/// raw bytes plus their decoded forms, regardless of how large the plane is.
/// Override with `LASTDB_PIN_LOG_SCAN_PAGE`.
pub(super) fn pin_log_scan_page_size() -> usize {
    env_flag::var_or("LASTDB_PIN_LOG_SCAN_PAGE", 256usize).clamp(1, 4096)
}

/// Exclusive upper bound for a byte-prefix range scan.
///
/// Increments the last byte that is not `0xFF`, dropping the `0xFF` tail.
///
/// Returns `None` for an empty or all-`0xFF` prefix, which have no finite
/// successor. `None` rather than a sentinel because every fallback here is
/// wrong in a way the caller must not silently inherit: returning the prefix
/// unchanged gives `start >= end`, which the range-scan contract answers with
/// an empty result — a scan that reads nothing while reporting success. Pin-log
/// prefixes always end in `:`, so this is unreachable for the real key format;
/// the caller still handles it rather than assuming.
pub(super) fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Smallest key strictly greater than `key`, for keyset pagination.
pub(super) fn key_after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}
