//! The stored form of a value in a disk format v2 run home (phase V1b).
//!
//! Every stored value starts with one tag byte:
//!
//! ```text
//! 0  the value as given
//! 1  compact JSON: the elements of a compact JSON text, typed
//! ```
//!
//! Tag 1 applies only to compact JSON text as `serde_json::to_vec` writes
//! it: no white space between tokens. Object keys from [`KEYS`] become an
//! index, lowercase hex and UUID strings become their bytes, plain integers
//! become varints, and UTC timestamps become numbers. Every other string and
//! number keeps its text as written, escapes included, so a decode gives the
//! exact original bytes. The encoder decodes its own output and keeps tag 0
//! when that output differs, when the text is not compact JSON, or when tag 1
//! is not smaller.
//!
//! Element forms after tag 1 (`n` is a LEB128 varint):
//!
//! ```text
//! 0 null | 1 false | 2 true
//! 3 n: integer n             4 n: integer -(n + 1)
//! 5 n, n bytes: number text  6 n, n bytes: string text between the quotes
//! 7 n, n bytes: lowercase hex string of 2n characters
//! 8 16 bytes: lowercase UUID string
//! 9 zigzag n, 1 byte d, n f: "YYYY-MM-DDTHH:MM:SS[.f]Z", d fraction digits
//! 10 elements, 12: array     11 members, n = 0: object
//! member: n = 2(k + 1), element: key KEYS[k] | n = 2m + 1, m bytes, element
//! ```

mod decode;
mod time;

use crate::disk_run::codec::corrupt;
use crate::Result;

/// Tag of a value stored as given.
pub const RAW: u8 = 0;
/// Tag of a compact JSON value.
pub const COMPACT_JSON: u8 = 1;

/// Object keys that tag 1 stores as an index. The list is part of the stored
/// format: do not change or reorder it. A different list needs a new tag.
pub const KEYS: [&str; 63] = [
    "atom_uuid",
    "written_at",
    "logical_counter",
    "mutation_uuid",
    "writer_pubkey",
    "signature",
    "signature_version",
    "provenance",
    "device_id",
    "origin_device",
    "prev_tip_id",
    "content",
    "created_at",
    "metadata",
    "source_file_name",
    "source_schema_name",
    "uuid",
    "molecule_key_bundle",
    "User",
    "pubkey",
    "Derived",
    "wasm_hash",
    "input_snapshot_hash",
    "sources_merkle_root",
    "v",
    "refs",
    "blobs",
    "sz",
    "src",
    "suspect",
    "seq",
    "mutation_id",
    "schema",
    "operation",
    "hash",
    "range",
    "committed_at_ms",
    "background_tasks_drained",
    "convergence_pending",
    "molecule_uuid",
    "timestamp",
    "field_key",
    "old_atom_uuid",
    "new_atom_uuid",
    "kind",
    "version",
    "is_conflict",
    "conflict_loser_atom",
    "source_order",
    "suppressed_by_delete",
    "suppressed_by_tip",
    "mk_key",
    "updated_at",
    "generation",
    "previous_generation",
    "activated_at_unix_nanos",
    "shadow",
    "domain",
    "sealed_bundle",
    "after",
    "retired",
    "examined",
    "minimum_written_at_nanos",
];

/// Larger values are stored as given: their encode would cost time for
/// little gain.
const MAX_COMPACT_INPUT: usize = 1 << 20;
/// Deepest nesting that tag 1 accepts.
const MAX_DEPTH: usize = 128;

const NULL: u8 = 0;
const FALSE: u8 = 1;
const TRUE: u8 = 2;
const UINT: u8 = 3;
const NEGATIVE: u8 = 4;
const NUMBER: u8 = 5;
const STRING: u8 = 6;
const HEX: u8 = 7;
const UUID: u8 = 8;
const TIME: u8 = 9;
const ARRAY: u8 = 10;
const OBJECT: u8 = 11;
const END: u8 = 12;

/// The stored form of `value`: tag 1 when the value is compact JSON and that
/// form is smaller, otherwise tag 0 and the value.
pub fn encode(value: &[u8]) -> Vec<u8> {
    if let Some(compact) = compact(value) {
        return compact;
    }
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(RAW);
    out.extend_from_slice(value);
    out
}

/// The value of a stored form. A value longer than `limit` is an error.
pub fn decode(mut stored: Vec<u8>, limit: usize) -> Result<Vec<u8>> {
    match stored.first() {
        Some(&RAW) if stored.len() - 1 <= limit => {
            stored.remove(0);
            Ok(stored)
        }
        Some(&RAW) => Err(corrupt("stored value exceeds its limit")),
        Some(&COMPACT_JSON) => decode::expand(&stored[1..], limit),
        _ => Err(corrupt("unknown stored value tag")),
    }
}

fn compact(value: &[u8]) -> Option<Vec<u8>> {
    if value.len() > MAX_COMPACT_INPUT || !value.first().is_some_and(|b| b"{[".contains(b)) {
        return None;
    }
    let mut out = Vec::with_capacity(value.len() / 2 + 8);
    out.push(COMPACT_JSON);
    let mut parser = Parser { text: value, at: 0 };
    parser.element(&mut out, 0)?;
    if parser.at != value.len() || out.len() > value.len() {
        return None;
    }
    let check = decode::expand(&out[1..], value.len()).ok()?;
    (check == value).then_some(out)
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// The compact JSON parser of the encoder. It reads one element and stops.
struct Parser<'a> {
    text: &'a [u8],
    at: usize,
}

impl<'a> Parser<'a> {
    fn next(&mut self) -> Option<u8> {
        let byte = *self.text.get(self.at)?;
        self.at += 1;
        Some(byte)
    }

    fn element(&mut self, out: &mut Vec<u8>, depth: usize) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        match *self.text.get(self.at)? {
            b'{' => self.object(out, depth),
            b'[' => self.array(out, depth),
            b'"' => {
                let text = self.string()?;
                string_element(text, out);
                Some(())
            }
            b'n' => self.word(b"null", NULL, out),
            b't' => self.word(b"true", TRUE, out),
            b'f' => self.word(b"false", FALSE, out),
            b'-' | b'0'..=b'9' => {
                self.number(out);
                Some(())
            }
            _ => None,
        }
    }

    fn word(&mut self, word: &[u8], kind: u8, out: &mut Vec<u8>) -> Option<()> {
        let end = self.at + word.len();
        (self.text.get(self.at..end)? == word).then_some(())?;
        self.at = end;
        out.push(kind);
        Some(())
    }

    /// The text between the quotes of the string that starts here.
    fn string(&mut self) -> Option<&'a [u8]> {
        let start = self.at + 1;
        let mut at = start;
        loop {
            match *self.text.get(at)? {
                b'"' => break,
                b'\\' => at += 2,
                _ => at += 1,
            }
        }
        self.at = at + 1;
        Some(&self.text[start..at])
    }

    fn number(&mut self, out: &mut Vec<u8>) {
        let start = self.at;
        while self
            .text
            .get(self.at)
            .is_some_and(|b| matches!(b, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
        {
            self.at += 1;
        }
        let text = &self.text[start..self.at];
        if let Some(value) = plain_integer(text) {
            out.push(UINT);
            varint(out, value);
            return;
        }
        if let Some(value) = text.strip_prefix(b"-").and_then(plain_integer) {
            if value > 0 {
                out.push(NEGATIVE);
                varint(out, value - 1);
                return;
            }
        }
        out.push(NUMBER);
        varint(out, text.len() as u64);
        out.extend_from_slice(text);
    }

    fn array(&mut self, out: &mut Vec<u8>, depth: usize) -> Option<()> {
        self.at += 1;
        out.push(ARRAY);
        if self.text.get(self.at) == Some(&b']') {
            self.at += 1;
        } else {
            loop {
                self.element(out, depth + 1)?;
                match self.next()? {
                    b',' => {}
                    b']' => break,
                    _ => return None,
                }
            }
        }
        out.push(END);
        Some(())
    }

    fn object(&mut self, out: &mut Vec<u8>, depth: usize) -> Option<()> {
        self.at += 1;
        out.push(OBJECT);
        if self.text.get(self.at) == Some(&b'}') {
            self.at += 1;
        } else {
            loop {
                if self.text.get(self.at) != Some(&b'"') {
                    return None;
                }
                let key = self.string()?;
                match KEYS.iter().position(|known| known.as_bytes() == key) {
                    Some(index) => varint(out, 2 * (index as u64 + 1)),
                    None => {
                        varint(out, 2 * key.len() as u64 + 1);
                        out.extend_from_slice(key);
                    }
                }
                (self.next()? == b':').then_some(())?;
                self.element(out, depth + 1)?;
                match self.next()? {
                    b',' => {}
                    b'}' => break,
                    _ => return None,
                }
            }
        }
        varint(out, 0);
        Some(())
    }
}

/// Digits with no sign and no leading zero that fit in 64 bits.
fn plain_integer(text: &[u8]) -> Option<u64> {
    if text.is_empty() || (text.len() > 1 && text[0] == b'0') {
        return None;
    }
    text.iter().try_fold(0u64, |value, &digit| {
        let digit = digit.is_ascii_digit().then(|| u64::from(digit - b'0'))?;
        value.checked_mul(10)?.checked_add(digit)
    })
}

fn string_element(text: &[u8], out: &mut Vec<u8>) {
    if let Some(bytes) = uuid_bytes(text) {
        out.push(UUID);
        out.extend_from_slice(&bytes);
    } else if let Some(bytes) = hex_bytes(text) {
        out.push(HEX);
        varint(out, bytes.len() as u64);
        out.extend_from_slice(&bytes);
    } else if let Some((seconds, digits, fraction)) = time::parse(text) {
        out.push(TIME);
        varint(out, ((seconds << 1) ^ (seconds >> 63)) as u64);
        out.push(digits);
        varint(out, fraction);
    } else {
        out.push(STRING);
        varint(out, text.len() as u64);
        out.extend_from_slice(text);
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// The bytes of lowercase hex text with at least one pair.
fn hex_bytes(text: &[u8]) -> Option<Vec<u8>> {
    if text.is_empty() || text.len() % 2 != 0 {
        return None;
    }
    text.chunks(2)
        .map(|pair| Some((hex_value(pair[0])? << 4) | hex_value(pair[1])?))
        .collect()
}

/// The bytes of a lowercase `8-4-4-4-12` UUID text.
fn uuid_bytes(text: &[u8]) -> Option<[u8; 16]> {
    const HYPHENS: [usize; 4] = [8, 13, 18, 23];
    if text.len() != 36 || HYPHENS.iter().any(|&at| text[at] != b'-') {
        return None;
    }
    let digits: Vec<u8> = text
        .iter()
        .enumerate()
        .filter(|(at, _)| !HYPHENS.contains(at))
        .map(|(_, &digit)| digit)
        .collect();
    hex_bytes(&digits)?.try_into().ok()
}
