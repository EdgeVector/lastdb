//! Check a typed field key contract from bounded, caller-selected native records.
use super::{
    metadata_key, open_wrap, validate_identity, wrap_key_for, BundleMetadata, BundleWrap,
    StorageError, StorageResult, NODE_DOMAIN,
};
use schema_types::field_range_profile::FieldRangeProfile;
use serde::de::DeserializeOwned;

pub(crate) const NODE_KEY_ROW_MAX_BYTES: u64 = 16 * 1024;
const FIELD_KEY_ID_MAX_BYTES: usize = 1024;

/// These are the normal node-global keys, independent of a record's body scope.
pub(crate) fn node_bundle_storage_keys(field_key_id: &str) -> StorageResult<[String; 2]> {
    validate_field_key(field_key_id)?;
    Ok([
        metadata_key(field_key_id),
        wrap_key_for(field_key_id, NODE_DOMAIN),
    ])
}

/// Verify a typed schema's existing field key contract at a held root.
/// This reads no cache and creates no key.
pub(crate) fn verify_frozen_typed_profile(
    field_key: &str,
    expected: FieldRangeProfile,
    account_key: &[u8; 32],
    metadata: Option<&[u8]>,
    wrap: Option<&[u8]>,
) -> StorageResult<()> {
    validate_field_key(field_key)?;
    let metadata: BundleMetadata =
        decode(metadata.ok_or_else(|| invalid("typed field key metadata is absent"))?)?;
    if metadata.version != super::contracts::TYPED_METADATA_VERSION
        || metadata.field_key_id != field_key
        || metadata.range_profile != expected
    {
        return Err(invalid("typed field key metadata differs from its schema"));
    }
    let wrap: BundleWrap = decode(wrap.ok_or_else(|| invalid("typed field key wrap is absent"))?)?;
    open_wrap(&wrap, field_key, NODE_DOMAIN, account_key).map(drop)
}

/// The native field_keys namespace stores metadata and wrapped keys as
/// plain JSON.
fn decode<T: DeserializeOwned>(stored: &[u8]) -> StorageResult<T> {
    if stored.len() as u64 > NODE_KEY_ROW_MAX_BYTES {
        return Err(invalid("native field key record exceeds its bound"));
    }
    serde_json::from_slice(stored).map_err(|_| invalid("native field key record format"))
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}

fn validate_field_key(field_key_id: &str) -> StorageResult<()> {
    validate_identity(field_key_id, NODE_DOMAIN)?;
    if field_key_id.len() > FIELD_KEY_ID_MAX_BYTES {
        return Err(invalid("native field key identity exceeds its bound"));
    }
    Ok(())
}
