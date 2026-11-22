//! Sealed payloads: how a history that holds private data reads its own entries.
//!
//! The owner of a personal database signs entries whose content only the owner
//! may read. The payload of such an entry is sealed before it is signed, and the
//! entry names the fixed namespace [`SEALED_NAMESPACE`] instead of the schema.
//! The relay and every other reader see only that namespace and the length of
//! the ciphertext. This crate holds no keys and does no cryptography. A history
//! created with [`crate::VerifiedHistory::with_opener`] asks its
//! [`SealOpener`] for the plaintext, and the plaintext carries the schema name
//! and the record that the entry changes.
//!
//! Plaintext layouts, all big-endian:
//!
//! - a data entry: `u16 name_len` · schema name · the record payload of
//!   [`crate::encode_record_payload`];
//! - a schema entry: `u16 name_len` · schema name · the JSON definition;
//! - a bundle blob: `u32 count`, then per change `u16 name_len` · schema name ·
//!   `u16 key_len` · record key · `u32 value_len` · value, in the order of
//!   [`crate::validate_bundle_changes`].

use std::fmt::Debug;

use crate::bundle::BundleChange;
use crate::entry::EntryHeader;

/// The namespace every sealed data and schema entry carries on the wire.
pub const SEALED_NAMESPACE: &str = "sealed";

/// Opens the sealed payload of an entry.
///
/// The implementation binds the ciphertext to the position of the entry, so a
/// payload that was moved to another position does not open.
pub trait SealOpener: Send + Sync + Debug {
    /// The plaintext of `sealed` for the entry with this header, or `None` when
    /// it does not open.
    fn open(&self, header: &EntryHeader, sealed: &[u8]) -> Option<Vec<u8>>;
}

/// Put `name` in front of `rest`. `None` when the name is empty or over 128 bytes.
#[must_use]
pub fn encode_named(name: &str, rest: &[u8]) -> Option<Vec<u8>> {
    if name.is_empty() || name.len() > 128 {
        return None;
    }
    let mut out = Vec::with_capacity(2 + name.len() + rest.len());
    out.extend_from_slice(&u16::try_from(name.len()).ok()?.to_be_bytes());
    out.extend_from_slice(name.as_bytes());
    out.extend_from_slice(rest);
    Some(out)
}

/// Split the name off a plaintext made by [`encode_named`].
#[must_use]
pub fn decode_named(plain: &[u8]) -> Option<(&str, &[u8])> {
    let (len, rest) = plain.split_first_chunk::<2>()?;
    let (name, rest) = rest.split_at_checked(usize::from(u16::from_be_bytes(*len)))?;
    Some((std::str::from_utf8(name).ok()?, rest))
}

/// The plaintext of a bundle blob. `None` when a name, key or value is too long.
#[must_use]
pub fn encode_bundle_changes(changes: &[BundleChange]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&u32::try_from(changes.len()).ok()?.to_be_bytes());
    for change in changes {
        out.extend_from_slice(&u16::try_from(change.namespace.len()).ok()?.to_be_bytes());
        out.extend_from_slice(change.namespace.as_bytes());
        out.extend_from_slice(&u16::try_from(change.record_key.len()).ok()?.to_be_bytes());
        out.extend_from_slice(change.record_key.as_bytes());
        out.extend_from_slice(&u32::try_from(change.value.len()).ok()?.to_be_bytes());
        out.extend_from_slice(&change.value);
    }
    Some(out)
}

/// The changes of a plaintext made by [`encode_bundle_changes`].
#[must_use]
pub fn decode_bundle_changes(plain: &[u8]) -> Option<Vec<BundleChange>> {
    let mut reader = Reader { rest: plain };
    let count = u32::from_be_bytes(reader.take()?);
    let mut changes = Vec::new();
    for _ in 0..count {
        let name_len = usize::from(u16::from_be_bytes(reader.take()?));
        let namespace = reader.text(name_len)?;
        let key_len = usize::from(u16::from_be_bytes(reader.take()?));
        let record_key = reader.text(key_len)?;
        let value_len = usize::try_from(u32::from_be_bytes(reader.take()?)).ok()?;
        let value = reader.bytes(value_len)?.to_vec();
        changes.push(BundleChange {
            namespace,
            record_key,
            value,
        });
    }
    reader.rest.is_empty().then_some(changes)
}

struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let (head, tail) = self.rest.split_at_checked(len)?;
        self.rest = tail;
        Some(head)
    }

    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.bytes(N)?.try_into().ok()
    }

    fn text(&mut self, len: usize) -> Option<String> {
        String::from_utf8(self.bytes(len)?.to_vec()).ok()
    }
}
