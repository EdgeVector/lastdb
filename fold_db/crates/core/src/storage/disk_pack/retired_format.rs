//! Planted retired homes must fail open, and journal keys must be deleted unread.
use crate::storage::disk_pack::run_node::{self, RUN_DIR};
use crate::storage::disk_pack::PackNamespacedStore;
use crate::storage::error::StorageError;
use crate::storage::format::metadata::LOCAL_AUTHOR_KEY;
use crate::storage::format::{
    MAIN_NAMESPACE, METADATA_NAMESPACE, NODE_MARKER, RECORDS_COLLECTION, RETIRED_SHARED_DIR,
    RETIRED_SHARED_NODE_BYTES,
};
use crate::storage::retired::{self, SHARED_RETIRED};
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHome, CURRENT};
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

fn plant(dir: &Path, key: &[u8; 32], rows: Vec<(&str, &str)>) {
    let home_key = run_node::home_key(key);
    let home = RunHome::open(&dir.join(RUN_DIR), &home_key).unwrap();
    let operations = rows
        .into_iter()
        .map(|(collection, key)| HomeMutation {
            collection: collection.into(),
            key: key.into(),
            body: Some(b"not-a-record".to_vec()),
        })
        .collect();
    home.commit(operations).unwrap();
}

fn absent(dir: &Path, key: &[u8; 32], collection: &str, name: &str) -> bool {
    let home_key = run_node::home_key(key);
    let home = RunHome::open(&dir.join(RUN_DIR), &home_key).unwrap();
    let mut read = home.read(ReadBudget {
        pages: 64,
        blocks: 256,
        bytes: 1024 * 1024,
        keys: 32,
    });
    read.get(collection, name).unwrap().is_none()
}

fn stored(dir: &Path, key: &[u8; 32], collection: &str) -> Vec<(String, Vec<u8>)> {
    let home_key = run_node::home_key(key);
    let home = RunHome::open(&dir.join(RUN_DIR), &home_key).unwrap();
    let mut read = home.read(ReadBudget {
        pages: 256,
        blocks: 1024,
        bytes: 4 * 1024 * 1024,
        keys: 256,
    });
    let mut range = read.administrative_range(collection, b"", b"\xff").unwrap();
    let page = range.next_page(128).unwrap();
    assert!(page.exhausted, "{collection} has another page");
    page.records
}

#[test]
fn portable_journal_key_is_not_replayed() {
    let (dir, key) = fresh();
    let home_key = run_node::home_key(&key);
    let home = RunHome::open(&dir.path().join(RUN_DIR), &home_key).unwrap();
    let mut operations: Vec<_> = (0..40)
        .map(|n| HomeMutation {
            collection: METADATA_NAMESPACE.into(),
            key: format!("portable-entry:{n:04}"),
            body: Some(b"not-a-record".to_vec()),
        })
        .collect();
    operations.push(HomeMutation {
        collection: RECORDS_COLLECTION.into(),
        key: "ade:1".into(),
        body: Some(b"not-a-record".to_vec()),
    });
    home.commit(operations).unwrap();
    drop(home);
    PackNamespacedStore::open_keyed(dir.path(), Some(&key)).expect("personal home opens");
    let metadata = stored(dir.path(), &key, METADATA_NAMESPACE);
    let records = stored(dir.path(), &key, RECORDS_COLLECTION);
    assert!(metadata.iter().any(|(name, _)| name == LOCAL_AUTHOR_KEY));
    assert!(metadata
        .iter()
        .chain(&records)
        .all(|(name, body)| !name.starts_with("portable-entry:")
            && !name.starts_with("ade:")
            && body != b"not-a-record"));
}

#[test]
fn portable_journal_cleanup_second_open_is_a_noop() {
    let (dir, key) = fresh();
    plant(
        dir.path(),
        &key,
        vec![(METADATA_NAMESPACE, "portable-entry:planted")],
    );
    PackNamespacedStore::open_keyed(dir.path(), Some(&key)).expect("cleanup open");
    let current = dir.path().join(RUN_DIR).join(CURRENT);
    let before = std::fs::read(&current).unwrap();
    PackNamespacedStore::open_keyed(dir.path(), Some(&key)).expect("second open");
    assert_eq!(std::fs::read(&current).unwrap(), before);
    assert!(absent(
        dir.path(),
        &key,
        METADATA_NAMESPACE,
        "portable-entry:planted"
    ));
}

#[test]
fn kept_personal_keys_stay_readable() {
    assert!(!retired::is_retired_key(
        METADATA_NAMESPACE,
        b"portable-local-author"
    ));
    assert!(!retired::is_retired_key(
        METADATA_NAMESPACE,
        b"portable-file-reference:record"
    ));
    assert!(!retired::is_retired_key(
        METADATA_NAMESPACE,
        b"portable-file-retention"
    ));
    assert!(retired::is_retired_key(
        METADATA_NAMESPACE,
        b"portable-entry:peer"
    ));
    assert!(retired::is_retired_key(MAIN_NAMESPACE, b"ade:1"));
    let (dir, key) = fresh();
    PackNamespacedStore::open_keyed(dir.path(), Some(&key)).expect("kept keys reopen");
    assert!(!absent(
        dir.path(),
        &key,
        METADATA_NAMESPACE,
        LOCAL_AUTHOR_KEY
    ));
}
