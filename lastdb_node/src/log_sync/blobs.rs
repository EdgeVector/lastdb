//! The sealed blobs of `bundle` entries, kept beside the log.
//!
//! A bundle entry names the blob that holds its changes by digest. The relay
//! keeps the blob for other devices. This node keeps its own copy under
//! `log-sync/<database>/blobs/<digest>`, so a restart rebuilds the history from
//! the files without the relay. A blob is never changed: its name is its digest.

use std::path::{Path, PathBuf};

use mutation_log::{decode_bundle_changes, BundleChange, EntryHeader, Hex32, SealOpener};
use sha2::{Digest, Sha256};

use super::sealed::Sealer;
use crate::profile::private_fs as disk;

/// The protocol cap of a bundle blob: 64 MiB.
const BLOB_LIMIT: usize = 64 * 1024 * 1024;

/// Where the kept blob lives (or will live).
pub(super) fn path(home: &Path, database_id: &str, sha256: &Hex32) -> PathBuf {
    home.join("log-sync")
        .join(database_id)
        .join("blobs")
        .join(sha256.to_hex())
}

/// Keep a blob. Its digest must be the one that names it.
pub(super) fn save(
    home: &Path,
    database_id: &str,
    sha256: &Hex32,
    blob: &[u8],
) -> Result<(), String> {
    if digest(blob) != *sha256 {
        return Err("bundle blob does not match its digest".into());
    }
    disk::write_private(&path(home, database_id, sha256), blob)
        .map_err(|_| "cannot write bundle blob".into())
}

/// The kept blob, or `None` when this node does not hold it.
pub(super) fn load(
    home: &Path,
    database_id: &str,
    sha256: &Hex32,
) -> Result<Option<Vec<u8>>, String> {
    disk::read_limited(&path(home, database_id, sha256), BLOB_LIMIT)
        .map_err(|_| "cannot read bundle blob".into())
}

/// The SHA-256 of a blob, as a bundle entry names it.
pub(super) fn digest(blob: &[u8]) -> Hex32 {
    Hex32::from_bytes(Sha256::digest(blob).into())
}

/// Open `blob` for the bundle entry with this header and list its changes.
pub(super) fn open_changes(
    sealer: &Sealer,
    header: &EntryHeader,
    blob: &[u8],
) -> Result<Vec<BundleChange>, String> {
    let plain = sealer
        .open(header, blob)
        .ok_or("bundle blob does not open")?;
    decode_bundle_changes(&plain).ok_or_else(|| "bundle blob holds no list of changes".to_owned())
}
