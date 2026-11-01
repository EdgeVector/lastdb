//! Decode only the selected representation under the exact field key identity.

use super::{FieldKeyCodec, FieldKeyCodecError};

impl FieldKeyCodec {
    /// V2 block encryption supplies authentication. The field key's typed
    /// range profile decodes the stored payload.
    pub fn api_range(&self, stored: &str) -> Result<String, FieldKeyCodecError> {
        self.range_profile
            .decode_value(stored)
            .map_err(FieldKeyCodecError::InvalidRange)
    }
}
