use crate::{term_bytes, Error, Result};
use std::fmt;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Byte-sortable ciphertext. Ordering is meaningful only inside one key scope.
/// Parsing validates shape only; it does not authenticate the term or its scope.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeTerm(Vec<u8>);

impl fmt::Debug for RangeTerm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RangeTerm")
            .field("bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl AsRef<[u8]> for RangeTerm {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl RangeTerm {
    pub(crate) fn from_encryption(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Encode as lowercase hexadecimal without changing the ciphertext order.
    /// The byte limit includes the entire returned text, not a native key frame.
    pub fn to_storage_text(&self, max_text_bytes: usize) -> Result<String> {
        let size = self.0.len().checked_mul(2).ok_or(Error::ByteLimit)?;
        if size > max_text_bytes {
            return Err(Error::ByteLimit);
        }
        let mut text = String::new();
        text.try_reserve_exact(size)
            .map_err(|_| Error::Allocation)?;
        for byte in &self.0 {
            text.push(char::from(HEX[usize::from(byte >> 4)]));
            text.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        Ok(text)
    }

    /// Parse canonical lowercase text within a raw ciphertext byte limit.
    /// The caller must separately validate the native key frame and scope.
    pub fn from_storage_text(text: &str, max_term_bytes: usize) -> Result<Self> {
        let size = text.len() / 2;
        if size > max_term_bytes {
            return Err(Error::ByteLimit);
        }
        if !text.len().is_multiple_of(2) || size < term_bytes(0)? || size % 8 != 1 {
            return Err(Error::InvalidTerm);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| Error::Allocation)?;
        for pair in text.as_bytes().chunks_exact(2) {
            let high = hex_digit(pair[0]).ok_or(Error::InvalidTerm)?;
            let low = hex_digit(pair[1]).ok_or(Error::InvalidTerm)?;
            bytes.push((high << 4) | low);
        }
        if bytes[0] > 1 {
            return Err(Error::InvalidTerm);
        }
        Ok(Self(bytes))
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
