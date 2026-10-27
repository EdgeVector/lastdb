//! Check the logical format through read-only snapshots before writer cleanup.

use super::{map, native_routing, run_node, PackRuntime, StorageError, StorageResult, NODE_MARKER};
use crate::atom::molecule_key_codec::TYPED_KEY_FORMAT;
use crate::crypto::content_key_proof;
use crate::db_operations::atom_store::{key_format, AtomStore};
use laststore::disk_run::home::{ReadBudget, RunRead};
use schema_types::key_comparison::KeyOrderVersion;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub(super) const TYPED_NODE_BYTES: &[u8] =
    b"format=4\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\n";
pub(super) const CONVERSION_INTENT: &str = "lastdb-range-conversion-v1";

impl PackRuntime {
    pub(crate) fn range_order(&self) -> KeyOrderVersion {
        self.physical.range_order
    }
}

pub(super) fn admit(root: &Path, key: &[u8; 32]) -> StorageResult<KeyOrderVersion> {
    if root.join(CONVERSION_INTENT).try_exists()? {
        return Err(invalid(
            "range conversion requires an explicit offline resume",
        ));
    }
    let marker = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(invalid("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    let order = match bytes.as_slice() {
        run_node::RUN_NODE_BYTES => KeyOrderVersion::LegacyUtf8V1,
        TYPED_NODE_BYTES => KeyOrderVersion::TypedScalarV1,
        _ => return Err(invalid("the node range format is unsupported")),
    };
    require_settings(order)?;
    if run_node::creation_pending(root)? && order == KeyOrderVersion::LegacyUtf8V1 {
        // The legacy creation gate admits only its atomic account/format proof.
        return Ok(order);
    }
    // Typed creation commits both proofs before its node marker appears.
    // Even with a creation intent, no missing or partial typed proof is admitted.
    verify(root, key, order)?;
    Ok(order)
}

fn verify(root: &Path, key: &[u8; 32], order: KeyOrderVersion) -> StorageResult<()> {
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
    let marker = AtomStore::CURRENT_KEY_FORMAT_KEY;
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
    let matches = match order {
        KeyOrderVersion::LegacyUtf8V1 => key_format::is_supported_key_format(marker),
        KeyOrderVersion::TypedScalarV1 => marker == TYPED_KEY_FORMAT,
    };
    if !matches {
        return Err(invalid("the logical format differs from the node marker"));
    }
    AtomStore::validate_frozen_replay_floor(Some(marker), values[1].as_deref())?;
    match values[2].as_deref() {
        Some(proof) => content_key_proof::verify(key, proof)?,
        // Legacy library callers can initialize DbOperations without account
        // keys. Both CURRENT files already authenticate their physical key.
        // Typed publication always requires the account proof as well.
        None if order == KeyOrderVersion::LegacyUtf8V1 => {}
        None => return Err(invalid("the account proof is absent")),
    }
    Ok(())
}

fn require_settings(order: KeyOrderVersion) -> StorageResult<()> {
    if order == KeyOrderVersion::TypedScalarV1 {
        for (name, expected) in [
            ("LASTDB_HASH_KEY_ENCODING", "blind_v1"),
            ("LASTDB_RANGE_KEY_ENCODING", "plain"),
        ] {
            match std::env::var(name) {
                Ok(value) if value.eq_ignore_ascii_case(expected) => {}
                Err(std::env::VarError::NotPresent) => {}
                _ => {
                    return Err(invalid(
                        "typed ranges require blinded hashes and plain ordered payloads",
                    ))
                }
            }
        }
    }
    Ok(())
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
