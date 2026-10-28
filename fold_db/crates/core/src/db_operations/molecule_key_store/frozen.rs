//! Check a typed molecule contract from bounded, caller-selected native records.
use super::{
    metadata_key, open_wrap, validate_identity, wrap_key_for, BundleMetadata, BundleWrap,
    StorageError, StorageResult, NODE_DOMAIN,
};
use schema_types::molecule_range_profile::MoleculeRangeProfile;
use serde::de::DeserializeOwned;

pub(crate) const NODE_KEY_ROW_MAX_BYTES: u64 = 16 * 1024;
const MOLECULE_ID_MAX_BYTES: usize = 1024;

/// These are the normal node-global keys, independent of an atom's body scope.
pub(crate) fn node_bundle_storage_keys(molecule_uuid: &str) -> StorageResult<[String; 2]> {
    validate_molecule(molecule_uuid)?;
    Ok([
        metadata_key(molecule_uuid),
        wrap_key_for(molecule_uuid, NODE_DOMAIN),
    ])
}

/// Verify a typed schema's existing molecule contract at a held root.
/// This reads no cache, creates no key, and never turns node keys into a bundle.
pub(crate) fn verify_frozen_typed_profile(
    molecule: &str,
    expected: MoleculeRangeProfile,
    account_key: &[u8; 32],
    metadata: Option<&[u8]>,
    wrap: Option<&[u8]>,
) -> StorageResult<()> {
    validate_molecule(molecule)?;
    let metadata: BundleMetadata =
        decode(metadata.ok_or_else(|| invalid("typed molecule metadata is absent"))?)?;
    if metadata.version != super::contracts::TYPED_METADATA_VERSION
        || metadata.molecule_uuid != molecule
        || metadata.range_profile != Some(expected)
        || metadata.conflict_identity.is_none()
    {
        return Err(invalid("typed molecule metadata differs from its schema"));
    }
    match (metadata.key_authority, wrap) {
        (Some(super::KeyAuthority::Node), None) => Ok(()),
        (Some(super::KeyAuthority::Bundle), Some(bytes)) => {
            let wrap: BundleWrap = decode(bytes)?;
            open_wrap(&wrap, molecule, NODE_DOMAIN, account_key).map(drop)
        }
        _ => Err(invalid(
            "typed molecule key authority differs from its node wrap",
        )),
    }
}

/// The native molecule_keys namespace stores metadata and wrapped keys as
/// plain JSON.
fn decode<T: DeserializeOwned>(stored: &[u8]) -> StorageResult<T> {
    if stored.len() as u64 > NODE_KEY_ROW_MAX_BYTES {
        return Err(invalid("native molecule key record exceeds its bound"));
    }
    serde_json::from_slice(stored).map_err(|_| invalid("native molecule key record format"))
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}

fn validate_molecule(molecule_uuid: &str) -> StorageResult<()> {
    validate_identity(molecule_uuid, NODE_DOMAIN)?;
    if molecule_uuid.len() > MOLECULE_ID_MAX_BYTES {
        return Err(invalid("native molecule key identity exceeds its bound"));
    }
    Ok(())
}
