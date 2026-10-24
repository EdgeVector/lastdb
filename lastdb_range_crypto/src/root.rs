use crate::{Error, RangeOpeKey, Result, FORMAT_ID};
use hkdf::Hkdf;
use sha2::Sha256;
use std::fmt;
use zeroize::Zeroizing;

const ROOT_SALT: &[u8] = b"lastdb:range-ope:private-seed:v1";
const ROOT_INFO: &[u8] = b"lastdb:range-ope:root-key:v1";
const MOLECULE_INFO: &[u8] = b"lastdb:range-ope:molecule-key:v1";
const HASH_INFO: &[u8] = b"lastdb:range-ope:hash-key:v1";
const MAX_SCOPE_COMPONENT_BYTES: usize = 65_535;

/// A private-key-derived range root. It is not serializable or clonable.
pub struct RangeOpeRoot(Zeroizing<[u8; 32]>);

/// One ordered stream. All components are stable public context, not secrets.
#[derive(Clone, Copy)]
pub struct RangeScope<'a> {
    /// Exact molecule identity, without an alternate UUID spelling.
    pub molecule: &'a str,
    /// The storage-form blinded hash, not its original API value.
    pub blinded_hash: &'a str,
    /// A changed epoch creates a different, non-comparable ordered stream.
    pub epoch: u64,
}

impl fmt::Debug for RangeOpeRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RangeOpeRoot(<redacted>)")
    }
}

impl RangeOpeRoot {
    /// Derive a separate OPE root from the user's 32-byte private identity seed.
    ///
    /// A public key, signature, or textual key-file representation is not an
    /// input. The caller owns and protects the borrowed private seed.
    pub fn from_private_key_seed(seed: &[u8; 32]) -> Result<Self> {
        let mut key = Zeroizing::new([0; 32]);
        Hkdf::<Sha256>::new(Some(ROOT_SALT), seed)
            .expand(ROOT_INFO, key.as_mut())
            .map_err(|_| Error::KeyDerivation)?;
        Ok(Self(key))
    }

    /// Derive a deterministic symmetric key for one molecule, hash, and epoch.
    pub fn for_scope(&self, scope: RangeScope<'_>) -> Result<RangeOpeKey> {
        let info = scope_info(scope)?;
        let mut molecule_key = Zeroizing::new([0; 32]);
        Hkdf::<Sha256>::from_prk(self.0.as_ref())
            .map_err(|_| Error::KeyDerivation)?
            .expand(&info, molecule_key.as_mut())
            .map_err(|_| Error::KeyDerivation)?;
        let hash_info = frame_fields(&[HASH_INFO, scope.blinded_hash.as_bytes()])?;
        let mut key = Zeroizing::new([0; 32]);
        Hkdf::<Sha256>::from_prk(molecule_key.as_ref())
            .map_err(|_| Error::KeyDerivation)?
            .expand(&hash_info, key.as_mut())
            .map_err(|_| Error::KeyDerivation)?;
        Ok(RangeOpeKey::from_derived(key))
    }
}

fn scope_info(scope: RangeScope<'_>) -> Result<Vec<u8>> {
    if scope.molecule.is_empty() {
        return Err(Error::InvalidScope);
    }
    let epoch = scope.epoch.to_be_bytes();
    frame_fields(&[
        MOLECULE_INFO,
        FORMAT_ID.as_bytes(),
        scope.molecule.as_bytes(),
        &epoch,
    ])
}

fn frame_fields(fields: &[&[u8]]) -> Result<Vec<u8>> {
    let capacity = fields.iter().try_fold(0usize, |bytes, field| {
        if field.len() > MAX_SCOPE_COMPONENT_BYTES {
            return Err(Error::InvalidScope);
        }
        bytes
            .checked_add(4)
            .and_then(|bytes| bytes.checked_add(field.len()))
            .ok_or(Error::ByteLimit)
    })?;
    let mut info = Vec::new();
    info.try_reserve_exact(capacity)
        .map_err(|_| Error::Allocation)?;
    for field in fields {
        let length = u32::try_from(field.len()).map_err(|_| Error::InvalidScope)?;
        info.extend_from_slice(&length.to_be_bytes());
        info.extend_from_slice(field);
    }
    Ok(info)
}
