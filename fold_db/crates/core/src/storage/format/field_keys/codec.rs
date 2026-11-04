//! Existing node metadata/wrap grammar and bounded nested bundle encoding.

use super::super::{
    FIELD_KEY_BUNDLE_MAX_BYTES, FIELD_KEY_BUNDLE_VERSION, FIELD_KEY_ID_MAX_BYTES,
    FIELD_KEY_METADATA_PREFIX, FIELD_KEY_METADATA_VERSION, FIELD_KEY_NODE_DOMAIN,
    FIELD_KEY_ROW_MAX_BYTES, FIELD_KEY_WRAP_PREFIX,
};
use super::FieldKeyBundle;
use crate::crypto::{seal_at_rest_utf8, AT_REST_ENC_PREFIX};
use crate::storage::error::StorageResult;
use crate::storage::{KvMutation, StorageError};
use schema_types::field_range_profile::FieldRangeProfile;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BundleMetadata {
    pub(crate) version: u8,
    pub(crate) field_key_id: String,
    pub(crate) range_profile: schema_types::field_range_profile::FieldRangeProfile,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BundleWrap {
    pub(crate) version: u8,
    pub(crate) field_key_id: String,
    pub(crate) domain: String,
    pub(crate) sealed_bundle: String,
}

fn make_wrap(
    field_key: &str,
    bundle: &FieldKeyBundle,
    key: &[u8; 32],
) -> StorageResult<BundleWrap> {
    let plaintext = zeroize::Zeroizing::new(
        serde_json::to_vec(bundle)
            .map_err(|error| StorageError::SerializationError(error.to_string()))?,
    );
    if plaintext.len() as u64 > FIELD_KEY_BUNDLE_MAX_BYTES {
        return Err(invalid("field key plaintext exceeds its bound"));
    }
    let sealed = seal_at_rest_utf8(key, &plaintext)
        .map_err(|_| StorageError::InvalidOperation("cannot wrap the field key bundle".into()))?;
    let sealed_bundle = String::from_utf8(sealed)
        .map_err(|_| StorageError::InvalidOperation("the field key wrap is not UTF-8".into()))?;
    if sealed_bundle.len() as u64 > FIELD_KEY_BUNDLE_MAX_BYTES {
        return Err(invalid("field key wrap exceeds its bound"));
    }
    Ok(BundleWrap {
        version: FIELD_KEY_BUNDLE_VERSION,
        field_key_id: field_key.into(),
        domain: FIELD_KEY_NODE_DOMAIN.into(),
        sealed_bundle,
    })
}

pub(super) fn open_wrap(
    wrap: &BundleWrap,
    field_key_id: &str,
    wrapping_key: &[u8; 32],
) -> StorageResult<FieldKeyBundle> {
    if wrap.version != FIELD_KEY_BUNDLE_VERSION
        || wrap.field_key_id != field_key_id
        || wrap.domain != FIELD_KEY_NODE_DOMAIN
    {
        return Err(StorageError::InvalidOperation(format!(
            "field key wrap metadata mismatch for {field_key_id} in {FIELD_KEY_NODE_DOMAIN}"
        )));
    }
    // `make_wrap` writes only the `ENC:` form of `seal_at_rest_utf8`.
    if !wrap.sealed_bundle.starts_with(AT_REST_ENC_PREFIX) {
        return Err(StorageError::InvalidOperation(
            "field key bundle wrap is not encrypted".into(),
        ));
    }
    // The generated bundle contains only three fixed-size keys, so a wrap
    // above this bound is not one that `make_wrap` wrote.
    let plaintext = zeroize::Zeroizing::new(
        crate::crypto::at_rest::open_at_rest_bounded(
            wrapping_key,
            wrap.sealed_bundle.as_bytes(),
            FIELD_KEY_BUNDLE_MAX_BYTES,
        )
        .map_err(|error| {
            StorageError::InvalidOperation(format!("unwrap field key bundle: {error}"))
        })?,
    );
    serde_json::from_slice(&plaintext).map_err(|error| {
        StorageError::SerializationError(format!("decode field key bundle: {error}"))
    })
}

pub(crate) fn validate_identity(field_key_id: &str) -> StorageResult<()> {
    if field_key_id.trim().is_empty() {
        return Err(StorageError::InvalidOperation(
            "field key bundle requires non-empty field key and domain".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn metadata_key(field_key_id: &str) -> String {
    format!("{FIELD_KEY_METADATA_PREFIX}{field_key_id}")
}

pub(crate) fn wrap_key_for(field_key_id: &str) -> String {
    let domain_hash = Sha256::digest(FIELD_KEY_NODE_DOMAIN.as_bytes());
    format!("{FIELD_KEY_WRAP_PREFIX}{field_key_id}:{domain_hash:x}")
}

pub(super) fn prepare(
    fields: Vec<(String, FieldRangeProfile)>,
    key: &[u8; 32],
) -> StorageResult<(super::EncodedFieldKeys, Vec<super::CreatedContract>)> {
    let mut rows = Vec::with_capacity(fields.len().saturating_mul(2));
    let mut contracts = Vec::with_capacity(fields.len());
    for (field_key, profile) in fields {
        validate_field_key(&field_key)?;
        let bundle = FieldKeyBundle::generate();
        let wrap = make_wrap(&field_key, &bundle, key)?;
        let metadata = BundleMetadata {
            version: FIELD_KEY_METADATA_VERSION,
            field_key_id: field_key.clone(),
            range_profile: profile,
        };
        rows.push(KvMutation::put(wrap_key_for(&field_key), encode(&wrap)?));
        rows.push(KvMutation::put(
            metadata_key(&field_key),
            encode(&metadata)?,
        ));
        contracts.push(super::CreatedContract {
            field_key,
            profile,
            bundle,
        });
    }
    Ok((super::EncodedFieldKeys { rows }, contracts))
}

fn encode(value: &impl Serialize) -> StorageResult<Vec<u8>> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    if bytes.len() as u64 > FIELD_KEY_ROW_MAX_BYTES {
        return Err(invalid("native field key record exceeds its bound"));
    }
    Ok(bytes)
}

pub(super) fn validate_field_key(field_key_id: &str) -> StorageResult<()> {
    validate_identity(field_key_id)?;
    if field_key_id.len() > FIELD_KEY_ID_MAX_BYTES {
        return Err(invalid("native field key identity exceeds its bound"));
    }
    Ok(())
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
