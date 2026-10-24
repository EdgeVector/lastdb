//! A scoped secret that a sharing grant can delegate without the account root.

use crate::{root::frame_fields, Error, RangeOpeKey, Result};
use hkdf::Hkdf;
use sha2::Sha256;
use std::fmt;
use zeroize::Zeroizing;

const HASH_INFO: &[u8] = b"lastdb:range-ope:hash-key:v1";
const VALUE_INFO: &[u8] = b"lastdb:range-ope:original-key:v1";

/// Private material for one exact molecule and epoch. No raw secret accessor.
pub struct RangeMoleculeKey {
    pub(crate) key: Zeroizing<[u8; 32]>,
    pub(crate) molecule: String,
    pub(crate) epoch: u64,
}

impl fmt::Debug for RangeMoleculeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RangeMoleculeKey(<redacted>)")
    }
}

impl RangeMoleculeKey {
    pub(crate) fn new(key: Zeroizing<[u8; 32]>, molecule: &str, epoch: u64) -> Self {
        Self {
            key,
            molecule: molecule.to_owned(),
            epoch,
        }
    }

    /// Exact public molecule identity authenticated by every protected grant.
    pub fn molecule(&self) -> &str {
        &self.molecule
    }

    /// Public epoch; streams from different epochs are never comparable.
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Derive the same hash key as `RangeOpeRoot::for_scope`.
    pub fn for_hash(&self, blinded_hash: &str) -> Result<RangeOpeKey> {
        Ok(RangeOpeKey::from_derived(
            self.derive(HASH_INFO, blinded_hash)?,
        ))
    }

    pub(crate) fn value_key(&self, blinded_hash: &str) -> Result<Zeroizing<[u8; 32]>> {
        self.derive(VALUE_INFO, blinded_hash)
    }

    fn derive(&self, purpose: &[u8], blinded_hash: &str) -> Result<Zeroizing<[u8; 32]>> {
        let info = frame_fields(&[purpose, blinded_hash.as_bytes()])?;
        let mut key = Zeroizing::new([0; 32]);
        Hkdf::<Sha256>::from_prk(self.key.as_ref())
            .map_err(|_| Error::KeyDerivation)?
            .expand(&info, key.as_mut())
            .map_err(|_| Error::KeyDerivation)?;
        Ok(key)
    }
}
