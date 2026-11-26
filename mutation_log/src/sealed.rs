//! Sealed payloads: namespace constant and opener trait.
//!
//! The owner of a personal database and every member of a shared database sign
//! entries whose content only holders of the database read key may read. The
//! payload of such an entry is sealed before it is signed, and the entry names
//! the fixed namespace [`SEALED_NAMESPACE`] instead of the schema. The relay and
//! every other reader see only that namespace and the length of the ciphertext.
//! This crate holds no keys and does no cryptography.
//!
//! Shared data and schema entries pack the real namespace in front of the
//! payload bytes before sealing, so the schema name never appears on the wire.

use std::fmt::Debug;

use crate::entry::{DataBody, EntryHeader, SchemaBody};

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

/// Pack a namespace and its bytes so both travel inside the sealed payload.
pub fn pack_hiding_namespace(namespace: &str, bytes: &[u8]) -> Result<Vec<u8>, &'static str> {
    let count = u16::try_from(namespace.len()).map_err(|_| "namespace is too long")?;
    let mut out = Vec::with_capacity(usize::from(count) + bytes.len() + 2);
    out.extend_from_slice(&count.to_be_bytes());
    out.extend_from_slice(namespace.as_bytes());
    out.extend_from_slice(bytes);
    Ok(out)
}

/// Split the packed form produced by [`pack_hiding_namespace`].
pub fn unpack_hiding_namespace(bytes: &[u8]) -> Option<(String, Vec<u8>)> {
    let count = usize::from(u16::from_be_bytes(bytes.get(..2)?.try_into().ok()?));
    let name = std::str::from_utf8(bytes.get(2..2 + count)?)
        .ok()?
        .to_owned();
    Some((name, bytes.get(2 + count..)?.to_vec()))
}

/// Open a data body to `(namespace, payload)`.
///
/// A sealed body needs `opener`. A clear body is returned only when no opener
/// is installed (in-process proof harnesses). A history that holds a database
/// read key refuses clear data: the product authors only sealed entries.
pub fn open_data_body(
    opener: Option<&dyn SealOpener>,
    header: &EntryHeader,
    body: &DataBody,
) -> Option<(String, Vec<u8>)> {
    if body.namespace == SEALED_NAMESPACE {
        return unpack_hiding_namespace(&opener?.open(header, &body.payload)?);
    }
    if opener.is_some() {
        return None;
    }
    Some((body.namespace.clone(), body.payload.clone()))
}

/// Open a schema body to `(namespace, definition)`.
///
/// Same rule as [`open_data_body`]: sealed needs `opener`; clear is only for
/// harness histories that have no opener.
pub fn open_schema_body(
    opener: Option<&dyn SealOpener>,
    header: &EntryHeader,
    body: &SchemaBody,
) -> Option<(String, Vec<u8>)> {
    if body.namespace == SEALED_NAMESPACE {
        return unpack_hiding_namespace(&opener?.open(header, &body.definition)?);
    }
    if opener.is_some() {
        return None;
    }
    Some((body.namespace.clone(), body.definition.clone()))
}
