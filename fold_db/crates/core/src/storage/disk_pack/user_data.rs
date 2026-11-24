//! Detect pre-existing user data when opening a captured home without spool state.

use super::{map, native_routing, physical, StorageResult};
use crate::storage::content_key_proof;
use crate::storage::format::metadata::{initial_local_author, LOCAL_AUTHOR_KEY};
use crate::storage::format::{
    CURRENT_KEY_FORMAT_KEY, METADATA_NAMESPACE, RECORDS_COLLECTION, SCHEMAS_NAMESPACE,
    SCHEMA_STATES_NAMESPACE,
};
use laststore::disk_run::home::ReadBudget;

pub(super) fn has_user_data(v2: &physical::V2Home, root: &physical::V2Root) -> StorageResult<bool> {
    if root.cas.frontier() > 0 {
        return Ok(true);
    }
    for col in [SCHEMAS_NAMESPACE, SCHEMA_STATES_NAMESPACE] {
        if !first_live_rows(v2, root, col, 1)?.is_empty() {
            return Ok(true);
        }
    }
    if first_live_rows(v2, root, RECORDS_COLLECTION, 2)?
        .iter()
        .any(|(key, _)| key != CURRENT_KEY_FORMAT_KEY)
    {
        return Ok(true);
    }
    let fresh_author = initial_local_author(v2.home.identity())?;
    let proof_key = native_routing::encode(content_key_proof::KEY);
    for (key, value) in first_live_rows(v2, root, METADATA_NAMESPACE, 3)? {
        if key == LOCAL_AUTHOR_KEY {
            if value != fresh_author {
                return Ok(true);
            }
        } else if key != proof_key {
            return Ok(true);
        }
    }
    Ok(false)
}

fn first_live_rows(
    v2: &physical::V2Home,
    root: &physical::V2Root,
    col: &str,
    want: usize,
) -> StorageResult<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::with_capacity(want);
    let mut start = Vec::new();
    while out.len() < want && start.as_slice() < &[0xff][..] {
        let mut main = v2
            .home
            .read_root(&root.main, ReadBudget::new(8 << 20, 4_096));
        let mut range = main
            .administrative_range(col, &start, &[0xff])
            .map_err(map)?;
        let page = range
            .next_page_yielding(want - out.len(), 0, 1)
            .map_err(map)?;
        out.extend(page.records);
        if page.exhausted || out.len() >= want {
            break;
        }
        let Some(last) = page.resume else {
            break;
        };
        start = last.into_bytes();
        start.push(0);
    }
    Ok(out)
}
