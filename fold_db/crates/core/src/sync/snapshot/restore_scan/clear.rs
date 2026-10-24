//! Remove the rows that a restore replaces. No source conversion.

use super::*;
use crate::storage::KvStore;
use std::sync::Arc;

fn selected(key: &[u8], mode: RestoreMode<'_>, is_main: bool) -> bool {
    if is_main && key == crate::storage::disk_pack::restore_safety::PENDING_KEY.as_bytes() {
        return false;
    }
    match mode {
        RestoreMode::Personal => !is_org_scoped_key_bytes(key),
        RestoreMode::Scoped(prefixes) => {
            namespace_key_belongs_to_any_target(key, prefixes, is_main)
        }
    }
}

/// Delete the rows of the restore scope. The node-global file rows stay: a
/// restore never replaces them with an older copy.
pub(super) async fn clear_existing(
    kv: &Arc<dyn KvStore>,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    mode: RestoreMode<'_>,
    is_main: bool,
) -> SyncResult<()> {
    let sources: Vec<Vec<u8>> = rows
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| selected(key, mode, is_main) && !(is_main && files::is_main_file_key(key)))
        .collect();
    for chunk in sources.chunks(128) {
        kv.batch_delete(chunk.to_vec()).await?;
    }
    Ok(())
}
