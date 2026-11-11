//! Refusals for the retired dedicated shared home and portable journal.
use crate::storage::disk_pack::{map, native_routing};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::{
    MAIN_NAMESPACE, METADATA_NAMESPACE, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
};
use laststore::disk_run::home::RunRead;
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

pub(in crate::storage) fn is_journal_key(namespace: &str, key: &[u8]) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    let prefixes = match namespace {
        METADATA_NAMESPACE => METADATA_PREFIXES,
        MAIN_NAMESPACE => MAIN_PREFIXES,
        _ => return false,
    };
    prefixes.iter().any(|prefix| key.starts_with(prefix))
}

pub(in crate::storage) fn refuse_journal(main: &mut RunRead) -> StorageResult<()> {
    for prefix in METADATA_PREFIXES {
        if prefixed(main, METADATA_NAMESPACE, prefix)? {
            return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
        }
    }
    for prefix in MAIN_PREFIXES {
        if prefixed(main, MAIN_NAMESPACE, prefix)? {
            return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
        }
    }
    Ok(())
}

fn prefixed(main: &mut RunRead, namespace: &str, prefix: &str) -> StorageResult<bool> {
    let collection = native_routing::collection_for_namespace(namespace);
    let end = exclusive_end(prefix);
    let mut range = main
        .administrative_range(collection, prefix.as_bytes(), end.as_bytes())
        .map_err(map)?;
    let page = range.next_page(1).map_err(map)?;
    Ok(!page.records.is_empty())
}

fn exclusive_end(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    let last = bytes.last_mut().expect("retired prefix");
    assert!(prefix.is_ascii() && *last < 0xff, "retired prefix end");
    *last += 1;
    String::from_utf8(bytes).expect("ascii prefix")
}
