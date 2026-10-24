//! Decode the existing node bundle from bounded, caller-selected native records.
use super::{
    metadata_key, open_wrap, validate_identity, wrap_key_for, BundleMetadata, BundleWrap,
    MoleculeKeyBundle, StorageError, StorageResult, BUNDLE_VERSION, NODE_DOMAIN,
};
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

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

/// Read no active cache and create no key. Both rows must come from one held root.
pub(crate) fn open_frozen_node_bundle(
    molecule_uuid: &str,
    account_key: &[u8; 32],
    metadata_stored: &[u8],
    wrap_stored: &[u8],
) -> StorageResult<MoleculeKeyBundle> {
    validate_molecule(molecule_uuid)?;
    let metadata: BundleMetadata = decode(account_key, metadata_stored)?;
    if metadata.version != BUNDLE_VERSION || metadata.molecule_uuid != molecule_uuid {
        return Err(invalid("native molecule key metadata differs"));
    }
    let wrap: BundleWrap = decode(account_key, wrap_stored)?;
    open_wrap(&wrap, molecule_uuid, NODE_DOMAIN, account_key)
}

fn decode<T: DeserializeOwned>(key: &[u8; 32], stored: &[u8]) -> StorageResult<T> {
    if stored.len() as u64 > NODE_KEY_ROW_MAX_BYTES {
        return Err(invalid("native molecule key record exceeds its bound"));
    }
    // The native molecule_keys namespace stores metadata and wrapped keys as
    // plain JSON. Historical outer account envelopes remain readable.
    let plain = crate::crypto::at_rest::is_sealed_at_rest(stored)
        .then(|| {
            crate::crypto::at_rest::open_at_rest_bounded(key, stored, NODE_KEY_ROW_MAX_BYTES)
                .map(Zeroizing::new)
                .map_err(|_| invalid("native molecule key record envelope"))
        })
        .transpose()?;
    let bytes = plain.as_deref().map_or(stored, Vec::as_slice);
    serde_json::from_slice(bytes).map_err(|_| invalid("native molecule key record format"))
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
