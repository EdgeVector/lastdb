//! Check the logical format through read-only snapshots before writer cleanup.

use super::{map, native_routing, run_node, StorageError, StorageResult, NODE_MARKER};
use crate::atom::molecule_key_codec::current_key_format;
use crate::records::key_format::{validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY};
use crate::storage::content_key_proof;
use laststore::disk_run::home::{ReadBudget, RunRead};
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// Format 7: the record model. One record lives at each primary key. Every
/// new home is format 7, and it is the only format this program opens.
pub(super) const RECORD_NODE_BYTES: &[u8] =
    b"format=7\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\nalternate-keys=v2\nrecords=v1\n";
/// The backup profile version of a format 7 home.
pub(super) const RECORD_PROFILE: u16 = 5;
// Formats 4 to 6 store atoms and molecules. They stay recognized so that a
// refusal can name the format it found.
pub(super) const TYPED_NODE_BYTES: &[u8] =
    b"format=6\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\nalternate-keys=v2\n";
pub(super) const FORMAT_FIVE_BYTES: &[u8] =
    b"format=5\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\nalternate-keys=v1\n";
pub(super) const FORMAT_FOUR_BYTES: &[u8] =
    b"format=4\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\n";

/// The backup profile version of a known node marker, for any format from 4
/// to 7. Only format 7 (`RECORD_PROFILE`) is admitted; see [`require_record`].
pub(super) fn profile(bytes: &[u8]) -> Option<u16> {
    match bytes {
        FORMAT_FOUR_BYTES => Some(2),
        FORMAT_FIVE_BYTES => Some(3),
        TYPED_NODE_BYTES => Some(4),
        RECORD_NODE_BYTES => Some(RECORD_PROFILE),
        _ => None,
    }
}

/// Refuse every known profile except format 7. An older home moves to this
/// release by a fresh home and a one-time copy that the older program serves;
/// this program has no reader for it.
pub(super) fn require_record(version: u16) -> StorageResult<u16> {
    if version == RECORD_PROFILE {
        return Ok(version);
    }
    let format = version + 2;
    Err(StorageError::InvalidOperation(format!(
        "node range admission: the home is format {format}; this release opens only format 7. \
         Move an older home by a one-time copy of its records into a new home, read through \
         the program that wrote it."
    )))
}

/// Admit the home and return its backup profile version.
pub(super) fn admit(root: &Path, key: &[u8; 32]) -> StorageResult<u16> {
    let marker = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(invalid("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    let version = profile(&bytes).ok_or_else(|| invalid("the node range format is unsupported"))?;
    let version = require_record(version)?;
    // Creation commits both proofs before its node marker appears. Even with
    // a creation intent, no missing or partial proof is admitted.
    verify(root, key)?;
    Ok(version)
}

fn verify(root: &Path, key: &[u8; 32]) -> StorageResult<()> {
    let mut main = RunRead::open_existing(
        &root.join(run_node::RUN_DIR),
        &run_node::home_key(key),
        budget(),
    )
    .map_err(map)?;
    let _cas = RunRead::open_existing(
        &root.join(run_node::CAS_DIR),
        &run_node::cas_key(key),
        budget(),
    )
    .map_err(map)?;
    verify_proofs(&mut main, key)
}

pub(super) fn verify_proofs(main: &mut RunRead, key: &[u8; 32]) -> StorageResult<()> {
    let marker = CURRENT_KEY_FORMAT_KEY;
    let cutoff = "layout:v3:delete-replay-cutoff";
    let coordinates = vec![
        (
            native_routing::collection_for_key(marker.as_bytes()).into(),
            marker.into(),
        ),
        (
            native_routing::collection_for_key(cutoff.as_bytes()).into(),
            cutoff.into(),
        ),
        (
            content_key_proof::NAMESPACE.into(),
            native_routing::encode(content_key_proof::KEY),
        ),
    ];
    let values = main
        .get_many_bounded(&coordinates, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    if values.len() != coordinates.len() {
        return Err(invalid("the account/format proof batch is incomplete"));
    }
    let marker = values[0]
        .as_deref()
        .ok_or_else(|| invalid("the logical format is absent"))?;
    if !current_key_format(marker) {
        return Err(invalid("the logical format differs from the node marker"));
    }
    validate_frozen_replay_floor(Some(marker), values[1].as_deref())?;
    let proof = values[2]
        .as_deref()
        .ok_or_else(|| invalid("the account proof is absent"))?;
    content_key_proof::verify(key, proof)
}

fn budget() -> ReadBudget {
    ReadBudget {
        pages: 16384,
        blocks: 65536,
        bytes: 8 * 1024 * 1024,
        keys: 16,
    }
}

fn invalid(text: &str) -> StorageError {
    StorageError::InvalidOperation(format!("node range admission: {text}"))
}
