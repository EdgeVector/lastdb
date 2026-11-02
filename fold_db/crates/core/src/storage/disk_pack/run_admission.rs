//! Check the logical format through read-only snapshots before writer cleanup.

use super::{map, native_routing, run_node, StorageError, StorageResult, NODE_MARKER};
use crate::records::key_format::{
    validate_frozen_replay_floor, CURRENT_KEY_FORMAT_KEY, DELETE_REPLAY_CUTOFF_KEY,
};
use crate::storage::content_key_proof;
use lastdb_model::field_key_codec::current_key_format;
use laststore::disk_run::home::{ReadBudget, RunRead};
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

/// Format 8: the record model with its final disk names. One record lives at
/// each primary key. Every new home is format 8, and it is the only format
/// this program opens. The format number names the whole layout, so the
/// marker holds no feature versions.
pub(in crate::storage) const RECORD_NODE_BYTES: &[u8] = b"format=8\nhome=disk-run\ncas=disk-cas\n";
/// The backup profile version of a format 8 home.
pub(in crate::storage) const RECORD_PROFILE: u16 = 6;

/// The backup profile version of a node marker. Only the format 8 marker has
/// one; every other marker is unsupported.
pub(in crate::storage) fn profile(bytes: &[u8]) -> Option<u16> {
    (bytes == RECORD_NODE_BYTES).then_some(RECORD_PROFILE)
}

/// Admit a format 8 home with complete creation proofs.
pub(super) fn admit(root: &Path, key: &[u8; 32]) -> StorageResult<()> {
    let marker = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(invalid("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    profile(&bytes).ok_or_else(|| invalid("the node range format is unsupported"))?;
    // Creation commits both proofs before its node marker appears. Even with
    // a creation intent, no missing or partial proof is admitted.
    verify(root, key)
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
    let cutoff = DELETE_REPLAY_CUTOFF_KEY;
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
