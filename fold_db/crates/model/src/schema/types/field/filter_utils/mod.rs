//! Shared limits and bounds of the record range reads.

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
