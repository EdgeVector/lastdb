//! Planted retired homes must fail open and must not replay journal keys.
use crate::storage::disk_pack::run_node::{self, RUN_DIR};
use crate::storage::disk_pack::PackNamespacedStore;
use crate::storage::error::StorageError;
use crate::storage::format::{
    MAIN_NAMESPACE, METADATA_NAMESPACE, NODE_MARKER, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
};
use crate::storage::retired::{self, JOURNAL_RETIRED, SHARED_RETIRED};
use laststore::disk_run::home::{HomeMutation, RunHome};
use std::io::Read;
use std::path::Path;

fn refused(dir: &Path, key: &[u8; 32]) -> StorageError {
    match PackNamespacedStore::open_keyed(dir, Some(key)) {
        Ok(_) => panic!("retired home opened"),
        Err(error) => error,
    }
}

fn fresh() -> (tempfile::TempDir, [u8; 32]) {
    let dir = tempfile::TempDir::new().expect("temp home");
    let mut bytes = Vec::new();
    std::fs::File::open("/dev/urandom")
        .expect("urandom")
        .take(32)
        .read_to_end(&mut bytes)
        .expect("node key");
    let key: [u8; 32] = bytes.try_into().expect("32 node-key bytes");
    PackNamespacedStore::create_run(dir.path(), &key).expect("personal home");
    (dir, key)
}

#[test]
fn retired_shared_marker_is_refused() {
    let (dir, key) = fresh();
    PackNamespacedStore::open_keyed(dir.path(), Some(&key)).expect("personal home reopens");
    std::fs::write(dir.path().join(NODE_MARKER), RETIRED_SHARED_NODE_BYTES).unwrap();
    let error = refused(dir.path(), &key);
    assert!(matches!(error, StorageError::RetiredFormat(SHARED_RETIRED)));
    assert!(error.to_string().contains("retired"));
}

#[test]
fn retired_shared_directory_is_refused() {
    let (dir, key) = fresh();
    std::fs::create_dir(dir.path().join(RETIRED_SHARED_DIR)).unwrap();
    let error = refused(dir.path(), &key);
    assert!(matches!(error, StorageError::RetiredFormat(SHARED_RETIRED)));
}

#[test]
fn portable_journal_key_is_not_replayed() {
    let (dir, key) = fresh();
    let home_key = run_node::home_key(&key);
    let home = RunHome::open(&dir.path().join(RUN_DIR), &home_key).unwrap();
    home.commit(vec![HomeMutation {
        collection: METADATA_NAMESPACE.into(),
        key: "portable-entry:planted".into(),
        body: Some(b"not-a-record".to_vec()),
    }])
    .unwrap();
    drop(home);
    let error = refused(dir.path(), &key);
    assert!(matches!(
        error,
        StorageError::RetiredFormat(JOURNAL_RETIRED)
    ));
    assert!(error.to_string().contains("retired"));
    assert!(!error.to_string().contains("not-a-record"));
}

#[test]
fn kept_personal_keys_stay_readable() {
    assert!(!retired::is_journal_key(
        METADATA_NAMESPACE,
        b"portable-local-author"
    ));
    assert!(!retired::is_journal_key(
        METADATA_NAMESPACE,
        b"portable-file-reference:record"
    ));
    assert!(!retired::is_journal_key(
        METADATA_NAMESPACE,
        b"portable-file-retention"
    ));
    assert!(retired::is_journal_key(
        METADATA_NAMESPACE,
        b"portable-entry:peer"
    ));
    assert!(retired::is_journal_key(MAIN_NAMESPACE, b"ade:1"));
    let (dir, key) = fresh();
    PackNamespacedStore::open_keyed(dir.path(), Some(&key)).expect("kept keys reopen");
}
