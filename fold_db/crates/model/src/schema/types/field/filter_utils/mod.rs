//! Shared limits and bounds of the record range reads.

/// Page size for an unfiltered field resolve when the caller passes no filter.
/// Matches the historical `SampleN(100)` default cap (now that SampleN is a peek
/// capped at [`crate::schema::types::field::SAMPLE_PEEK_CAP`], the bulk default must be a real `Page`). The
/// node query handler always supplies an explicit `Page`/key filter, so this
/// only governs direct in-core callers.
pub const DEFAULT_UNFILTERED_PAGE_LIMIT: usize = 100;

/// Common filter application utilities
pub struct FilterUtils;

impl FilterUtils {
    /// Return the finite exclusive upper bound for a physical prefix.
    ///
    /// Maximum trailing scalars carry into the preceding scalar. An empty or
    /// all-maximum prefix has no finite upper bound and receives a refusal.
    /// User prefixes should use prefix selection or include the physical
    /// namespace before this conversion.
    pub fn create_prefix_end(prefix: &str) -> Result<String, crate::schema::SchemaError> {
        let mut prefix_end = prefix.to_string();
        while let Some(last_char) = prefix_end.pop() {
            let next_cp = match last_char as u32 + 1 {
                0xD800 => 0xE000,
                n => n,
            };
            if let Some(next_char) = char::from_u32(next_cp) {
                prefix_end.push(next_char);
                return Ok(prefix_end);
            }
        }
        Err(crate::schema::SchemaError::InvalidData(
            "the prefix has no finite upper bound; select by prefix instead".into(),
        ))
    }
}
