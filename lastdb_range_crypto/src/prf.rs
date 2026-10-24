//! A secure prefix PRF with one excluded output and explicit refusal.
//!
//! A random 128-bit draw hits the excluded maximum with probability 2^-128.
//! The bounded sampler returns a uniform non-maximum block or an error. Across
//! Q prefix evaluations, coupling to uniform 128-bit blocks fails with
//! probability at most Q/2^128, plus the HMAC PRF advantage. Sixteen rejected
//! draws cause explicit refusal, never a wrapped or misordered ciphertext.

use crate::{Error, Result, BIT_BLOCK_BYTES};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

const MAX_DRAWS: u32 = 16;

pub(crate) fn nonwrapping_block(prefix: &Hmac<Sha256>) -> Result<Zeroizing<[u8; BIT_BLOCK_BYTES]>> {
    for counter in 0..MAX_DRAWS {
        let mut candidate = prefix.clone();
        // Prior bits are bytes 0/1. This terminator and fixed-width counter
        // make different prefixes and rejection draws distinct PRF inputs.
        candidate.update(&[0xff]);
        candidate.update(&counter.to_be_bytes());
        let mut digest = candidate.finalize().into_bytes();
        let mut block = Zeroizing::new([0; BIT_BLOCK_BYTES]);
        block.copy_from_slice(&digest[..BIT_BLOCK_BYTES]);
        let digest_bytes: &mut [u8] = digest.as_mut();
        digest_bytes.zeroize();
        if block.iter().any(|byte| *byte != u8::MAX) {
            return Ok(block);
        }
    }
    Err(Error::PrfRefused)
}
