//! The field key contract selects its typed range payloads.

use super::{FieldKeyCodec, FieldKeyCodecError};
use schema_types::field_range_profile::FieldRangeProfile;

impl FieldKeyCodec {
    #[must_use]
    pub fn range_profile(&self) -> FieldRangeProfile {
        self.range_profile
    }

    pub fn require_text_range_prefix(&self) -> Result<(), FieldKeyCodecError> {
        self.range_profile
            .require_text_prefix()
            .map_err(FieldKeyCodecError::InvalidRange)
    }
}
