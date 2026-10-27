//! Decode the existing node bundle from bounded, caller-selected native records.
use super::{
    metadata_key, open_wrap, validate_identity, wrap_key_for, BundleMetadata, BundleWrap,
    MoleculeKeyBundle, StorageError, StorageResult, NODE_DOMAIN,
};
use crate::atom::molecule_key_codec::ConflictKeyIdentity;
use schema_types::molecule_range_profile::MoleculeRangeProfile;
use serde::de::DeserializeOwned;

pub(crate) const NODE_KEY_ROW_MAX_BYTES: u64 = 16 * 1024;
const MOLECULE_ID_MAX_BYTES: usize = 1024;

/// One metadata row replaces its legacy form during offline publication.
/// Account-derived keys remain node authority; no key material is copied.
/// The metadata row is plain JSON, so the converter's account key is unused.
pub(crate) fn typed_frozen_metadata(
    molecule: &str,
    profile: MoleculeRangeProfile,
    identity: ConflictKeyIdentity,
    _account_key: &[u8; 32],
    source: Option<&[u8]>,
) -> StorageResult<Vec<u8>> {
    validate_molecule(molecule)?;
    profile.validate().map_err(StorageError::InvalidOperation)?;
    if let Some(source) = source {
        let metadata: BundleMetadata = decode(source)?;
        if !metadata.is_legacy_for(molecule) {
            return Err(invalid("source molecule metadata is not legacy"));
        }
    }
    let metadata = BundleMetadata {
        version: super::contracts::TYPED_METADATA_VERSION,
        molecule_uuid: molecule.to_owned(),
        range_profile: Some(profile),
        key_authority: Some(if source.is_some() {
            super::KeyAuthority::Bundle
        } else {
            super::KeyAuthority::Node
        }),
        conflict_identity: Some(identity),
    };
    serde_json::to_vec(&metadata).map_err(|_| invalid("cannot encode typed molecule metadata"))
}

/// These are the normal node-global keys, independent of an atom's body scope.
pub(crate) fn node_bundle_storage_keys(molecule_uuid: &str) -> StorageResult<[String; 2]> {
    validate_molecule(molecule_uuid)?;
    Ok([
        metadata_key(molecule_uuid),
        wrap_key_for(molecule_uuid, NODE_DOMAIN),
    ])
}

/// Read no active cache and create no key. Both rows must come from one held root.
pub(crate) fn open_frozen_node_bundle(
    molecule_uuid: &str,
    account_key: &[u8; 32],
    metadata_stored: &[u8],
    wrap_stored: &[u8],
) -> StorageResult<MoleculeKeyBundle> {
    validate_molecule(molecule_uuid)?;
    let metadata: BundleMetadata = decode(metadata_stored)?;
    if !metadata.is_legacy_for(molecule_uuid) {
        return Err(invalid("native molecule key metadata differs"));
    }
    let wrap: BundleWrap = decode(wrap_stored)?;
    open_wrap(&wrap, molecule_uuid, NODE_DOMAIN, account_key)
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
    expected
        .validate()
        .map_err(StorageError::InvalidOperation)?;
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
