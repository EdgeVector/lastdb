//! Rebuild the grant index on open from stored birth and publisher grant entries.

use std::path::{Path, PathBuf};

use mutation_log::{DatabaseId, Hex32, NodeKey, SignedEntry};
use tokio::fs;

use crate::grant_index::GrantIndex;
use crate::grant_verify::{verify_birth, verify_publisher_grant};

/// Scan `<root>/logs` and fold each database's verified birth and publisher grants.
pub(crate) async fn rebuild(root: &Path) -> GrantIndex {
    let mut index = GrantIndex::default();
    for dir in sorted_dirs(&root.join("logs")).await {
        let Some(db) = dir_id(&dir) else { continue };
        load_database(&mut index, &db, &dir).await;
    }
    index
}

async fn load_database(index: &mut GrantIndex, db: &DatabaseId, dir: &Path) {
    let Some(publisher) = find_birth(index, db, dir).await else {
        return;
    };
    for device_dir in sorted_dirs(&dir.join(publisher.to_hex())).await {
        for file in sorted_files(&device_dir).await {
            let Some(entry) = read_entry(&file).await else {
                continue;
            };
            if verify_publisher_grant(db, &publisher, &entry).is_ok() {
                index.apply_grant(db, &entry);
            }
        }
    }
}

async fn find_birth(index: &mut GrantIndex, db: &DatabaseId, dir: &Path) -> Option<NodeKey> {
    for node_dir in sorted_dirs(dir).await {
        let Some(node_key) = dir_id(&node_dir) else {
            continue;
        };
        for device_dir in sorted_dirs(&node_dir).await {
            let Some(entry) = read_entry(&device_dir.join("1.json")).await else {
                continue;
            };
            if let Ok(birth) = verify_birth(db, &entry) {
                if birth.owner == node_key {
                    index.install_birth(*db, birth);
                    return Some(node_key);
                }
            }
        }
    }
    None
}

fn dir_id(path: &Path) -> Option<Hex32> {
    Hex32::from_hex(path.file_name()?.to_str()?).ok()
}

async fn read_entry(path: &Path) -> Option<SignedEntry> {
    let bytes = fs::read(path).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

async fn sorted_dirs(path: &Path) -> Vec<PathBuf> {
    let mut out = list(path).await;
    out.retain(|p| p.is_dir());
    out
}

async fn sorted_files(path: &Path) -> Vec<PathBuf> {
    let mut out = list(path).await;
    out.retain(|p| p.is_file());
    out
}

async fn list(path: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(mut rd) = fs::read_dir(path).await {
        while let Ok(Some(ent)) = rd.next_entry().await {
            out.push(ent.path());
        }
    }
    out.sort();
    out
}
