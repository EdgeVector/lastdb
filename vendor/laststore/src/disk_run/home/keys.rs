//! Run keys: one byte order that keeps each collection contiguous.
//!
//! A run key is the collection length (u16, big-endian), the collection
//! bytes, then the complete storage key. Inside one collection the run key
//! order is the storage key byte order, so a range under one hash is one
//! contiguous run range.

use super::super::codec::corrupt;
use super::super::MAX_RUN_KEY_BYTES;
use crate::{Error, Result};

/// Builds the run key of `key` in `collection`.
pub(super) fn run_key(collection: &str, key: &[u8]) -> Result<Vec<u8>> {
    let len = u16::try_from(collection.len())
        .map_err(|_| Error::Config("disk run home: collection name too long".into()))?;
    let total = 2 + collection.len() + key.len();
    if total > MAX_RUN_KEY_BYTES {
        return Err(Error::Config(
            "disk run home: storage key exceeds the run key limit".into(),
        ));
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(collection.as_bytes());
    out.extend_from_slice(key);
    Ok(out)
}

/// The least byte string above every string that starts with `prefix`,
/// or `None` when no such bound exists (the prefix is all 0xff bytes).
pub(super) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Splits a run key into its storage key, checked against `collection`.
pub(super) fn storage_key(collection: &str, run_key: &[u8]) -> Result<String> {
    let at = 2 + collection.len();
    if run_key.len() < at
        || run_key[..2] != (collection.len() as u16).to_be_bytes()
        || &run_key[2..at] != collection.as_bytes()
    {
        return Err(corrupt("run key outside its collection"));
    }
    String::from_utf8(run_key[at..].to_vec()).map_err(|_| corrupt("storage key is not UTF-8"))
}
