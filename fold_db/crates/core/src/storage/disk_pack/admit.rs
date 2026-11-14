//! `O_NOFOLLOW` file opening and stopped-home marker/account admission.

use super::{map, native_routing, run_node, StorageError, StorageResult, CAS_DIR, RUN_DIR};
use crate::storage::content_key_proof;
use crate::storage::format::control::{validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY};
use crate::storage::format::metadata::{validate_local_author, LOCAL_AUTHOR_KEY};
use crate::storage::format::{
    CURRENT_KEY_FORMAT_KEY, METADATA_NAMESPACE, NODE_MARKER, RECORDS_COLLECTION, RECORD_NODE_BYTES,
    RECORD_PROFILE,
};
use lastdb_model::field_key_codec::current_key_format;
use laststore::disk_run::home::{ReadBudget, RunRead};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub(in crate::storage) fn open_nofollow_read(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

fn bad_admission(text: &str) -> StorageError {
    StorageError::invalid(format!("node range admission: {text}"))
}

fn read_stopped_node_profile(root: &Path) -> StorageResult<u16> {
    let marker = open_nofollow_read(&root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(bad_admission("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    crate::storage::retired::refuse_shared_home(root, &bytes)?;
    (bytes.as_slice() == RECORD_NODE_BYTES)
        .then_some(RECORD_PROFILE)
        .ok_or_else(|| bad_admission("the node range format is unsupported"))
}

pub(super) fn admit_stopped_home(root: &Path, key: &[u8; 32]) -> StorageResult<u16> {
    let version = read_stopped_node_profile(root)?;
    let budget = ReadBudget::new(8 << 20, 64);
    let mut main = RunRead::open_existing(&root.join(RUN_DIR), &run_node::home_key(key), budget)
        .map_err(map)?;
    RunRead::open_existing(&root.join(CAS_DIR), &run_node::cas_key(key), budget).map_err(map)?;
    let coords = [
        (RECORDS_COLLECTION, CURRENT_KEY_FORMAT_KEY.as_bytes()),
        (RECORDS_COLLECTION, DELETE_REPLAY_CUTOFF_KEY.as_bytes()),
        (METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes()),
        (content_key_proof::NAMESPACE, content_key_proof::KEY),
    ]
    .map(|(collection, raw_key)| (collection.to_owned(), native_routing::encode(raw_key)));
    let rows = main
        .get_many_bounded(&coords, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    let [fmt, floor, author, proof]: [Option<Vec<u8>>; 4] = rows
        .try_into()
        .map_err(|_| bad_admission("the account/format proof batch is incomplete"))?;
    let marker = fmt
        .as_deref()
        .ok_or_else(|| bad_admission("the logical format is absent"))?;
    if version != RECORD_PROFILE || !current_key_format(marker) {
        return Err(bad_admission(
            "the logical format differs from the node marker",
        ));
    }
    validate_frozen_replay_floor(Some(marker), floor.as_deref())?;
    validate_local_author(author.as_deref())?;
    let stored_proof = proof
        .as_deref()
        .ok_or_else(|| bad_admission("the account proof is absent"))?;
    content_key_proof::verify(key, stored_proof)?;
    crate::storage::retired::refuse_journal(&mut main).map(|()| version)
}
