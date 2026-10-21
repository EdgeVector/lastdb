//! Count encoded bytes without a retained serialized copy of a core object.
use super::*;

pub(super) fn encoded_len(value: &impl Serialize) -> StorageResult<usize> {
    crate::json_size::encoded_len(value).map_err(|error| serialization(&error))
}
