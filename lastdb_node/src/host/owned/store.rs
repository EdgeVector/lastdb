//! Open and remove one owned sibling store. The owner store is not handled here.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::storage::disk_pack::NODE_MARKER;

use super::Entry;

pub(super) async fn open_all(
    home: &Path,
    owned: &[Entry],
    keys: &E2eKeys,
    signer: &Arc<Ed25519KeyPair>,
) -> Result<HashMap<String, Arc<FoldDB>>, String> {
    let mut stores = HashMap::new();
    for entry in owned {
        let store = open(home, &entry.id, keys, Arc::clone(signer), false).await?;
        stores.insert(entry.id.clone(), store);
    }
    Ok(stores)
}

pub(super) async fn open(
    home: &Path,
    id: &str,
    keys: &E2eKeys,
    signer: Arc<Ed25519KeyPair>,
    create: bool,
) -> Result<Arc<FoldDB>, String> {
    let root = home.join("databases").join(id);
    let data = root.join("data");
    if !create && !data.join(NODE_MARKER).is_file() {
        return Err(format!("owned database {id} store is missing"));
    }
    std::fs::create_dir_all(&data)
        .map_err(|error| format!("create owned database dir: {error}"))?;
    let inbox = root.join("search-inbox");
    std::fs::create_dir_all(&inbox)
        .map_err(|error| format!("create owned search inbox: {error}"))?;
    fold_db::db_operations::search_index::queue_policy::prepare(&inbox)
        .map_err(|error| format!("owned search queue preparation failed: {error}"))?;
    fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig::local(data),
        keys,
        signer,
        fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: false,
            search_outbox_inbox: Some(inbox),
        },
    )
    .await
    .map_err(|error| format!("open owned database {id}: {error}"))
}

pub(super) async fn remove(home: &Path, id: &str) -> Result<(), String> {
    let root = home.join("databases").join(id);
    if root.is_dir() {
        std::fs::remove_dir_all(&root)
            .map_err(|error| format!("remove owned database {id}: {error}"))?;
    }
    Ok(())
}
