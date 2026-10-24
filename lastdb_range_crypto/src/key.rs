use crate::{term_bytes, Error, RangeTerm, Result, FORMAT_ID};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use zeroize::{Zeroize, Zeroizing};

// One fixed marker precedes every value, including an empty range. It is
// input framing, not a length prefix: total input length in the PRF context
// would break the common-prefix property required for ordered comparison.
const INPUT_MARKER: u8 = 1;
const PRF_DOMAIN: &[u8] = b"lastdb:range-ope:prefix-prf:v1\0";

/// A private, symmetric key for exactly one ordered stream.
pub struct RangeOpeKey(Zeroizing<[u8; 32]>);

/// Encrypted half-open bounds for one plaintext byte prefix.
pub struct EncryptedRangeBounds {
    /// Inclusive lower bound, including the empty prefix's real term.
    pub start: RangeTerm,
    /// Exclusive upper bound; absent means the end of the named hash stream.
    pub end: Option<RangeTerm>,
}

impl fmt::Debug for RangeOpeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RangeOpeKey(<redacted>)")
    }
}

impl RangeOpeKey {
    pub(crate) fn from_derived(key: Zeroizing<[u8; 32]>) -> Self {
        Self(key)
    }

    /// Encrypt a complete range without normalization or integer conversion.
    ///
    /// `max_term_bytes` bounds raw ciphertext, before optional hexadecimal
    /// encoding. This is a per-term limit, not a native storage admission.
    pub fn encrypt(&self, range: &[u8], max_term_bytes: usize) -> Result<RangeTerm> {
        let size = term_bytes(range.len())?;
        if size > max_term_bytes {
            return Err(Error::ByteLimit);
        }
        let mut output = Zeroizing::new(Vec::new());
        output
            .try_reserve_exact(size)
            .map_err(|_| Error::Allocation)?;
        output.resize(size, 0);
        let mut prefix =
            Hmac::<Sha256>::new_from_slice(self.0.as_ref()).map_err(|_| Error::KeyDerivation)?;
        prefix.update(PRF_DOMAIN);
        prefix.update(FORMAT_ID.as_bytes());
        for (position, bit) in input_bits(range).enumerate() {
            let mut digest = prefix.clone().finalize().into_bytes();
            output[position + 1] = digest[0];
            let digest_bytes: &mut [u8] = digest.as_mut();
            digest_bytes.zeroize();
            prefix.update(&[bit]);
        }
        // C = P + 128*B in base 256, with one byte per input bit. Keeping
        // the backward carry makes byte order exact; +1 is insufficient.
        let mut carry = 0u16;
        for (slot, bit) in output[1..].iter_mut().rev().zip(input_bits(range).rev()) {
            let sum = u16::from(*slot) + (u16::from(bit) << 7) + carry;
            *slot = (sum & 0xff) as u8;
            carry = sum >> 8;
        }
        output[0] = carry as u8;
        Ok(RangeTerm::from_encryption(std::mem::take(&mut *output)))
    }

    /// Encode a plaintext prefix as an interval, not an encrypted byte prefix.
    /// Each returned term fits `max_term_bytes`; both terms may be allocated.
    pub fn encrypt_prefix(
        &self,
        prefix: &[u8],
        max_term_bytes: usize,
    ) -> Result<EncryptedRangeBounds> {
        let start = self.encrypt(prefix, max_term_bytes)?;
        let Some(position) = prefix.iter().rposition(|byte| *byte < u8::MAX) else {
            return Ok(EncryptedRangeBounds { start, end: None });
        };
        let mut upper = Zeroizing::new(Vec::new());
        upper
            .try_reserve_exact(position + 1)
            .map_err(|_| Error::Allocation)?;
        upper.extend_from_slice(&prefix[..=position]);
        upper[position] += 1;
        let end = Some(self.encrypt(&upper, max_term_bytes)?);
        Ok(EncryptedRangeBounds { start, end })
    }
}

fn input_bits(range: &[u8]) -> impl DoubleEndedIterator<Item = u8> + '_ {
    std::iter::once(INPUT_MARKER)
        .chain(range.iter().copied())
        .flat_map(|byte| (0..8).rev().map(move |shift| (byte >> shift) & 1))
}
