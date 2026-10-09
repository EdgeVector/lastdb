//! Part of `lastdb_local_maintain`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fold_db::crypto::{CryptoProvider, E2eKeys, LocalCryptoProvider};
use fold_db::storage::laststore::high_water_path_for_store_root;
use fold_db::storage::traits::NamespacedStore;
use fold_db::storage::{
    EncryptingNamespacedStore, LastStoreNamespacedStore, LASTSTORE_PLAINTEXT_NAMESPACES,
};
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

/// The store an atom-GC command reads, plus how it was opened.
pub(crate) struct HomeStore {
    pub(crate) store: Arc<dyn NamespacedStore>,
    pub(crate) store_root: PathBuf,
    /// `at-rest-seam` when the home's `identity.key` was found and values
    /// decrypt, `raw-no-identity-key` when they do not.
    pub(crate) seam: &'static str,
}

/// Open `home`'s Last Store the way the daemon does: base store, then the
/// at-rest seam when the home's identity key is present.
///
/// **The seam is not optional detail for this command.** Every namespace an
/// atom GC reads — `atoms`, `main`, `tips`, `atom_locators` — is encrypted
/// (`LASTSTORE_PLAINTEXT_NAMESPACES` lists the exceptions, and none of these is
/// on it). Opened raw, tip values do not parse as JSON, so *no* atom uuid is
/// ever collected as referenced and every body looks unreferenced; body values
/// are ciphertext sealed with a per-write nonce, so two identical bodies hash
/// differently and content comparison is meaningless. Both failures are silent
/// and both point the same way — toward deleting something live. So a raw open
/// still reports, but the reaper refuses to execute against one.
pub(crate) fn open_home(home: &Path, i_know_this_is_primary: bool) -> Result<HomeStore, String> {
    if !i_know_this_is_primary {
        refuse_primary(home)?;
    }
    let store_root = resolve_laststore_root(home)?;
    if !i_know_this_is_primary {
        refuse_primary(&store_root)?;
    }

    let high_water = high_water_path_for_store_root(&store_root);
    let base = LastStoreNamespacedStore::open_with_options_and_high_water(
        &store_root,
        laststore::LastStoreOptions::hash_group(),
        high_water,
    )
    .map_err(|e| format!("open LastStore {}: {e}", store_root.display()))?;
    let base: Arc<dyn NamespacedStore> = Arc::new(base);

    match load_home_crypto(home) {
        Some(crypto) => Ok(HomeStore {
            store: Arc::new(EncryptingNamespacedStore::with_plaintext_namespaces(
                base,
                crypto,
                LASTSTORE_PLAINTEXT_NAMESPACES
                    .iter()
                    .map(|ns| (*ns).to_string())
                    .collect(),
            )),
            store_root,
            seam: "at-rest-seam",
        }),
        None => Ok(HomeStore {
            store: base,
            store_root,
            seam: "raw-no-identity-key",
        }),
    }
}

/// The Mini at-rest provider for `home`, from its `identity.key` seed.
///
/// `None` for any home without a readable seed — an offline fixture, or a copy
/// taken without the key. Mini seals with the account content key and no
/// keyring, which is why this can be reconstructed from the seed alone.
pub(crate) fn load_home_crypto(home: &Path) -> Option<Arc<dyn CryptoProvider>> {
    let bytes = std::fs::read(home.join("identity.key")).ok()?;
    if bytes.len() < 32 {
        return None;
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes[..32]);
    let e2e = E2eKeys::from_ed25519_seed(&seed).ok()?;
    Some(Arc::new(LocalCryptoProvider::from_key(
        e2e.encryption_key(),
    )))
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
