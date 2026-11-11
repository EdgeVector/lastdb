//! Refusals for the retired dedicated shared home, and the retired-key predicate.
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::{
    MAIN_NAMESPACE, METADATA_NAMESPACE, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
};
use std::path::Path;

pub(in crate::storage) const SHARED_RETIRED: &str = "the dedicated shared home format is retired";
pub(in crate::storage) const JOURNAL_RETIRED: &str = "a portable journal key is retired";

const METADATA_PREFIXES: &[&str] = &[
    "portable-entry:",
    "portable-inbox:",
    "portable-inbox-index:",
    "portable-progress:",
    "portable-schema:",
    "portable-schema-versions:",
    "portable-replay-state",
    "portable-restore-owner",
    "portable-last-applied-peer",
    "native-shared-schema-scope",
];
const MAIN_PREFIXES: &[&str] = &["ade:"];

pub(in crate::storage) fn refuse_shared_home(root: &Path, marker: &[u8]) -> StorageResult<()> {
    let private =
        std::fs::symlink_metadata(root.join(RETIRED_SHARED_DIR)).is_ok_and(|meta| meta.is_dir());
    if marker == RETIRED_SHARED_NODE_BYTES || private {
        return Err(StorageError::RetiredFormat(SHARED_RETIRED));
    }
    Ok(())
}

pub(in crate::storage) fn retired_prefixes(namespace: &str) -> &'static [&'static str] {
    match namespace {
        METADATA_NAMESPACE => METADATA_PREFIXES,
        MAIN_NAMESPACE => MAIN_PREFIXES,
        _ => &[],
    }
}

/// True when `key` is leftover portable-journal metadata, not a live personal key.
pub(in crate::storage) fn is_retired_key(namespace: &str, key: &[u8]) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    retired_prefixes(namespace)
        .iter()
        .any(|prefix| key.starts_with(prefix))
}
