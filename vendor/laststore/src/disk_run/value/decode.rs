//! The decoder of tag 1 values: it writes the compact JSON text again.

use super::{
    time, ARRAY, END, FALSE, HEX, KEYS, MAX_DEPTH, NEGATIVE, NULL, NUMBER, OBJECT, STRING, TIME,
    TRUE, UINT, UUID,
};
use crate::disk_run::codec::corrupt;
use crate::Result;

const DIGITS: &[u8; 16] = b"0123456789abcdef";

/// The text of the tag 1 elements in `input`. Text longer than `limit` is
/// an error.
pub(super) fn expand(input: &[u8], limit: usize) -> Result<Vec<u8>> {
    let mut decoder = Decoder {
        input,
        at: 0,
        out: Vec::with_capacity(input.len().saturating_mul(2).min(limit)),
        limit,
    };
    decoder.element(0)?;
    if decoder.at != input.len() {
        return Err(corrupt("compact value has bytes after its element"));
    }
    Ok(decoder.out)
}

struct Decoder<'a> {
    input: &'a [u8],
    at: usize,
    out: Vec<u8>,
    limit: usize,
}

impl<'a> Decoder<'a> {
    fn byte(&mut self) -> Result<u8> {
        let byte = *self
            .input
            .get(self.at)
            .ok_or_else(|| corrupt("compact value ends early"))?;
        self.at += 1;
        Ok(byte)
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                break;
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(corrupt("compact value varint is too long"))
    }

    fn take(&mut self, len: u64) -> Result<&'a [u8]> {
        let input = self.input;
        let end = usize::try_from(len)
            .ok()
            .and_then(|len| self.at.checked_add(len))
            .filter(|&end| end <= input.len())
            .ok_or_else(|| corrupt("compact value ends early"))?;
        let bytes = &input[self.at..end];
        self.at = end;
        Ok(bytes)
    }

    fn room(&self, len: usize) -> Result<()> {
        if self.out.len().saturating_add(len) > self.limit {
            return Err(corrupt("compact value exceeds its limit"));
        }
        Ok(())
    }

    fn put(&mut self, bytes: &[u8]) -> Result<()> {
        self.room(bytes.len())?;
        self.out.extend_from_slice(bytes);
        Ok(())
    }

    fn quoted(&mut self, bytes: &[u8]) -> Result<()> {
        self.room(bytes.len().saturating_add(2))?;
        self.out.push(b'"');
        self.out.extend_from_slice(bytes);
        self.out.push(b'"');
        Ok(())
    }

    fn element(&mut self, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(corrupt("compact value nests too deep"));
        }
        match self.byte()? {
            NULL => self.put(b"null"),
            FALSE => self.put(b"false"),
            TRUE => self.put(b"true"),
            UINT => {
                let value = self.varint()?;
                self.decimal(u128::from(value), false)
            }
            NEGATIVE => {
                let value = self.varint()?;
                self.decimal(u128::from(value) + 1, true)
            }
            NUMBER => {
                let len = self.varint()?;
                let text = self.take(len)?;
                self.put(text)
            }
            STRING => {
                let len = self.varint()?;
                let text = self.take(len)?;
                self.quoted(text)
            }
            HEX => {
                let len = self.varint()?;
                let bytes = self.take(len)?;
                self.hex(bytes, &[])
            }
            UUID => {
                let bytes = self.take(16)?;
                self.hex(bytes, &[4, 6, 8, 10])
            }
            TIME => self.time(),
            ARRAY => self.array(depth),
            OBJECT => self.object(depth),
            _ => Err(corrupt("unknown compact value element")),
        }
    }

    fn decimal(&mut self, value: u128, negative: bool) -> Result<()> {
        let mut text = [0u8; 40];
        let mut at = text.len();
        let mut rest = value;
        loop {
            at -= 1;
            text[at] = b'0' + (rest % 10) as u8;
            rest /= 10;
            if rest == 0 {
                break;
            }
        }
        if negative {
            at -= 1;
            text[at] = b'-';
        }
        self.put(&text[at..])
    }

    /// Quoted lowercase hex of `bytes`, with a hyphen before each byte
    /// index in `hyphens`.
    fn hex(&mut self, bytes: &[u8], hyphens: &[usize]) -> Result<()> {
        self.room(bytes.len().saturating_mul(2).saturating_add(hyphens.len() + 2))?;
        self.out.push(b'"');
        for (at, &byte) in bytes.iter().enumerate() {
            if hyphens.contains(&at) {
                self.out.push(b'-');
            }
            self.out.push(DIGITS[usize::from(byte >> 4)]);
            self.out.push(DIGITS[usize::from(byte & 15)]);
        }
        self.out.push(b'"');
        Ok(())
    }

    fn time(&mut self) -> Result<()> {
        let zigzag = self.varint()?;
        let seconds = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
        let digits = self.byte()?;
        let fraction = self.varint()?;
        let text = time::format(seconds, digits, fraction)
            .ok_or_else(|| corrupt("compact value time is out of range"))?;
        self.quoted(&text)
    }

    fn array(&mut self, depth: usize) -> Result<()> {
        self.put(b"[")?;
        let mut first = true;
        while self.input.get(self.at) != Some(&END) {
            if !first {
                self.put(b",")?;
            }
            first = false;
            self.element(depth + 1)?;
        }
        self.at += 1;
        self.put(b"]")
    }

    fn object(&mut self, depth: usize) -> Result<()> {
        self.put(b"{")?;
        let mut first = true;
        loop {
            let code = self.varint()?;
            if code == 0 {
                break;
            }
            if !first {
                self.put(b",")?;
            }
            first = false;
            if code % 2 == 0 {
                let key = usize::try_from(code / 2 - 1)
                    .ok()
                    .and_then(|index| KEYS.get(index))
                    .ok_or_else(|| corrupt("unknown compact value key"))?;
                self.quoted(key.as_bytes())?;
            } else {
                let key = self.take(code / 2)?;
                self.quoted(key)?;
            }
            self.put(b":")?;
            self.element(depth + 1)?;
        }
        self.put(b"}")
    }
}
