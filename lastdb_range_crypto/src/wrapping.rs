//! Authenticated scoped grants and bounded AES-GCM-SIV envelopes.

use crate::{
    root::{frame_fields, scope_info},
    Error, RangeMoleculeKey, Result, FORMAT_ID,
};
use aes_gcm_siv::{
    aead::{Aead, KeyInit, Payload},
    Aes256GcmSiv, Nonce,
};
use rand::{rngs::OsRng, RngCore};
use zeroize::Zeroizing;

const GRANT_CONTEXT: &[u8] = b"lastdb:range-ope:grant-context:v1";
pub(crate) const ENVELOPE_OVERHEAD: usize = 29;
const VERSION: u8 = 1;

impl RangeMoleculeKey {
    /// Protect only this molecule secret for an explicitly named access domain.
    pub fn wrap_for_access(&self, wrap_key: &[u8; 32], domain: &str) -> Result<Vec<u8>> {
        let aad = grant_context(&self.molecule, self.epoch, domain)?;
        encrypt(wrap_key, self.key.as_ref(), &aad)
    }

    /// Authenticate scope and domain before a recipient can derive stream keys.
    pub fn from_access_wrap(
        wrap_key: &[u8; 32],
        molecule: &str,
        epoch: u64,
        domain: &str,
        wrapped: &[u8],
    ) -> Result<Self> {
        if wrapped.len() != 32 + ENVELOPE_OVERHEAD {
            return Err(Error::Authentication);
        }
        let aad = grant_context(molecule, epoch, domain)?;
        let plaintext = decrypt(wrap_key, wrapped, &aad, 32)?;
        let mut key = Zeroizing::new([0; 32]);
        key.copy_from_slice(&plaintext);
        Ok(Self::new(key, molecule, epoch))
    }
}

fn grant_context(molecule: &str, epoch: u64, domain: &str) -> Result<Vec<u8>> {
    scope_info(molecule, epoch)?;
    if domain.is_empty() {
        return Err(Error::InvalidScope);
    }
    frame_fields(&[
        GRANT_CONTEXT,
        FORMAT_ID.as_bytes(),
        molecule.as_bytes(),
        &epoch.to_be_bytes(),
        domain.as_bytes(),
    ])
}

pub(crate) fn encrypt(key: &[u8; 32], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
    let capacity = plaintext
        .len()
        .checked_add(ENVELOPE_OVERHEAD)
        .ok_or(Error::ByteLimit)?;
    let cipher = Aes256GcmSiv::new_from_slice(key).map_err(|_| Error::KeyDerivation)?;
    let mut nonce = [0; 12];
    OsRng
        .try_fill_bytes(&mut nonce)
        .map_err(|_| Error::Entropy)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|_| Error::Allocation)?;
    output.push(VERSION);
    output.extend_from_slice(&nonce);
    output.extend_from_slice(
        &cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| Error::Authentication)?,
    );
    Ok(output)
}

pub(crate) fn decrypt(
    key: &[u8; 32],
    wrapped: &[u8],
    aad: &[u8],
    maximum: usize,
) -> Result<Zeroizing<Vec<u8>>> {
    let ceiling = maximum
        .checked_add(ENVELOPE_OVERHEAD)
        .ok_or(Error::ByteLimit)?;
    if wrapped.len() > ceiling {
        return Err(Error::ByteLimit);
    }
    if wrapped.len() < ENVELOPE_OVERHEAD || wrapped[0] != VERSION {
        return Err(Error::Authentication);
    }
    let cipher = Aes256GcmSiv::new_from_slice(key).map_err(|_| Error::KeyDerivation)?;
    cipher
        .decrypt(
            Nonce::from_slice(&wrapped[1..13]),
            Payload {
                msg: &wrapped[13..],
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| Error::Authentication)
}
