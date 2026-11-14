//! Startup refusal checks for retired shared homes and portable journal keys.

use crate::storage::current_keys::{JOURNAL_RETIRED, META_PFX};
use crate::storage::disk_pack::{map, native_routing::collection_for_namespace};
use crate::storage::error::{StorageError, StorageResult};
use crate::storage::format::{
    MAIN_NAMESPACE, METADATA_NAMESPACE, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
};
use laststore::disk_run::home::RunRead;
use std::path::Path;

pub(in crate::storage) fn refuse_shared_home(root: &Path, marker: &[u8]) -> StorageResult<()> {
    let has_shared_dir =
        std::fs::symlink_metadata(root.join(RETIRED_SHARED_DIR)).is_ok_and(|meta| meta.is_dir());
    if marker == RETIRED_SHARED_NODE_BYTES || has_shared_dir {
        return Err(StorageError::RetiredFormat(
            "the dedicated shared home format is retired",
        ));
    }
    Ok(())
}

pub(in crate::storage) fn refuse_journal(main: &mut RunRead) -> StorageResult<()> {
    let prefixes = META_PFX.iter().map(|&prefix| (METADATA_NAMESPACE, prefix));
    for (namespace, prefix) in prefixes.chain([(MAIN_NAMESPACE, "ade:")]) {
        let mut end = prefix.as_bytes().to_vec();
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        let collection = collection_for_namespace(namespace);
        let mut range = main
            .administrative_range(collection, prefix.as_bytes(), &end)
            .map_err(map)?;
        if !range.next_page(1).map_err(map)?.records.is_empty() {
            return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
        }
    }
    Ok(())
}
