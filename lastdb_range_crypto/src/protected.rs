//! Original ranges use independent authenticated encryption, bound to the term.

use crate::{
    root::frame_fields, term_bytes, wrapping, Error, RangeMoleculeKey, RangeTerm, Result, FORMAT_ID,
};
use sha2::{Digest, Sha256};
use std::fmt;
use zeroize::Zeroizing;

const VALUE_CONTEXT: &[u8] = b"lastdb:range-ope:original-context:v1";

/// Prepared immutable ciphertext. A retry reuses this complete value.
#[derive(Clone)]
pub struct ProtectedRange {
    /// Searchable OPE term; authenticated by `encrypted` in this exact scope.
    pub term: RangeTerm,
    /// Version, random nonce, and AES-256-GCM-SIV ciphertext with its tag.
    pub encrypted: Vec<u8>,
}

impl fmt::Debug for ProtectedRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProtectedRange")
            .field("term_bytes", &self.term.as_ref().len())
            .field("encrypted_bytes", &self.encrypted.len())
            .finish()
    }
}

impl RangeMoleculeKey {
    /// Prepare the complete range field; `maximum` bounds original bytes.
    pub fn protect_range(
        &self,
        hash: &str,
        range: &[u8],
        maximum: usize,
    ) -> Result<ProtectedRange> {
        if range.len() > maximum {
            return Err(Error::ByteLimit);
        }
        let term = self.for_hash(hash)?.encrypt(range, term_bytes(maximum)?)?;
        let aad = self.range_context(hash, &term)?;
        let key = self.value_key(hash)?;
        let encrypted = wrapping::encrypt(&key, range, &aad)?;
        Ok(ProtectedRange { term, encrypted })
    }

    /// Authenticate location and validate the original against the stored term.
    pub fn open_range(
        &self,
        hash: &str,
        prepared: &ProtectedRange,
        maximum: usize,
    ) -> Result<Zeroizing<Vec<u8>>> {
        let term_limit = term_bytes(maximum)?;
        if prepared.term.as_ref().len() > term_limit {
            return Err(Error::ByteLimit);
        }
        let aad = self.range_context(hash, &prepared.term)?;
        let key = self.value_key(hash)?;
        let plaintext = wrapping::decrypt(&key, &prepared.encrypted, &aad, maximum)?;
        if self.for_hash(hash)?.encrypt(&plaintext, term_limit)? != prepared.term {
            return Err(Error::Authentication);
        }
        Ok(plaintext)
    }

    fn range_context(&self, hash: &str, term: &RangeTerm) -> Result<Vec<u8>> {
        let digest = Sha256::digest(term.as_ref());
        frame_fields(&[
            VALUE_CONTEXT,
            FORMAT_ID.as_bytes(),
            self.molecule.as_bytes(),
            &self.epoch.to_be_bytes(),
            hash.as_bytes(),
            digest.as_ref(),
        ])
    }
}
