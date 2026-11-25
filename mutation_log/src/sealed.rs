//! Sealed payloads: namespace constant and opener trait for personal databases.
//!
//! The owner of a personal database signs entries whose content only the owner
//! may read. The payload of such an entry is sealed before it is signed, and the
//! entry names the fixed namespace [`SEALED_NAMESPACE`] instead of the schema.
//! The relay and every other reader see only that namespace and the length of
//! the ciphertext. This crate holds no keys and does no cryptography.

use std::fmt::Debug;

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
