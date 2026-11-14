//! Open and remove one owned sibling store. The owner store is not handled here.
//!
//! Each sibling content key is derived from the node identity seed and that
//! database id. The personal store keeps its own key path. The node signing
//! key is the FoldDB signer and is not the content key.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::storage::disk_pack::NODE_MARKER;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use super::Entry;

/// Domain for owned-database content keys. Personal uses the node seed through
/// `E2eKeys::from_ed25519_seed`. Publication homes use a different salt.
const OWNED_CONTENT_DOMAIN: &[u8] = b"lastdb:owned-database:content:v1";

pub(super) async fn open_all(
    home: &Path,
    owned: &[Entry],
    signer: &Arc<Ed25519KeyPair>,
) -> Result<HashMap<String, Arc<FoldDB>>, String> {
    let mut stores = HashMap::new();
    for entry in owned {
        let store = open(home, &entry.id, Arc::clone(signer), false).await?;
        stores.insert(entry.id.clone(), store);
    }
    Ok(stores)
}

pub(super) async fn open(
    home: &Path,
    id: &str,
    signer: Arc<Ed25519KeyPair>,
    create: bool,
) -> Result<Arc<FoldDB>, String> {
    let root = home.join("databases").join(id);
    let data = root.join("data");
    if !create && !data.join(NODE_MARKER).is_file() {
        return Err(format!("owned database {id} store is missing"));
    }
    let node_seed = Zeroizing::new(signer.secret_key_bytes());
    let keys = content_keys(&node_seed, id)?;
    std::fs::create_dir_all(&data)
        .map_err(|error| format!("create owned database dir: {error}"))?;
    let inbox = root.join("search-inbox");
    std::fs::create_dir_all(&inbox)
        .map_err(|error| format!("create owned search inbox: {error}"))?;
    fold_db::db_operations::search_index::queue_policy::prepare(&inbox)
        .map_err(|error| format!("owned search queue preparation failed: {error}"))?;
    fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig::local(data),
        &keys,
        signer,
        fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: false,
            search_outbox_inbox: Some(inbox),
            // Owned siblings are shareable. Capture starts here, before cloud enrollment.
            capture_accepted: true,
            capture_initial_snapshot: false,
        },
    )
    .await
    .map_err(|error| format!("open owned database {id}: {error}"))
}

/// Stable content key for one owned database. The derived seed is not stored
/// and is not used as a signing identity.
fn content_keys(node_seed: &[u8; 32], id: &str) -> Result<E2eKeys, String> {
    let mut seed = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(OWNED_CONTENT_DOMAIN), node_seed)
        .expand(id.as_bytes(), seed.as_mut())
        .map_err(|_| "cannot derive owned database content key")?;
    E2eKeys::from_ed25519_seed(&seed).map_err(|_| "cannot derive owned database content key".into())
}

/// AES key that opens this owned database. Not the node seed and not the
/// intermediate HKDF seed.
pub(super) fn content_encryption_key(
    node_seed: &[u8; 32],
    id: &str,
) -> Result<Zeroizing<[u8; 32]>, String> {
    Ok(Zeroizing::new(
        content_keys(node_seed, id)?.encryption_key(),
    ))
}

pub(super) fn remove(home: &Path, id: &str) -> Result<(), String> {
    let root = home.join("databases").join(id);
    if root.is_dir() {
        std::fs::remove_dir_all(&root)
            .map_err(|error| format!("remove owned database {id}: {error}"))?;
    }
    Ok(())
}
