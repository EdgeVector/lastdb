//! Part of `lastdb_local_maintain`.

use std::path::{Path, PathBuf};

use fold_db::storage::laststore::high_water_path_for_store_root;
use fold_db::storage::LastStoreNamespacedStore;
use lastdb_node::offline_home::refuse_primary;

/// Open a Last Store for the residue commands.
///
/// Values in `indexes` are ciphertext, but neither of these commands decodes a
/// value: the inventory sums stored bytes and the reclaim deletes by key. So a
/// raw open is not merely tolerated here, it is correct — the stored length IS
/// what occupies the plane, and asking for the plaintext length would report a
/// number the disk does not hold.
pub(crate) fn open_laststore(
    home: &Path,
    i_know_this_is_primary: bool,
) -> Result<LastStoreNamespacedStore, String> {
    if !i_know_this_is_primary {
        refuse_primary(home)?;
    }
    let store_root = resolve_laststore_root(home)?;
    if !i_know_this_is_primary {
        refuse_primary(&store_root)?;
    }
    let high_water = high_water_path_for_store_root(&store_root);
    LastStoreNamespacedStore::open_with_options_and_high_water(
        &store_root,
        laststore::LastStoreOptions::hash_group(),
        high_water,
    )
    .map_err(|e| format!("open LastStore {}: {e}", store_root.display()))
}

pub(crate) fn resolve_laststore_root(home: &Path) -> Result<PathBuf, String> {
    if looks_like_laststore_root(home) {
        return Ok(home.to_path_buf());
    }
    let data = home.join("data");
    if looks_like_laststore_root(&data) {
        return Ok(data);
    }
    Err(format!(
        "{} is not a LastStore root or Mini home with data/",
        home.display()
    ))
}

pub(crate) fn looks_like_laststore_root(path: &Path) -> bool {
    path.join("laststore-layout-v1").exists()
        || path.join("data").join("field_tips").is_dir()
        || path.join("data").join("tips").is_dir()
        || path.join("data").join("proteins").is_dir()
}
