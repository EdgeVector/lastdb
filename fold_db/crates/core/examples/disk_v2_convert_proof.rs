//! Disk format v2, phase V4: the one-time conversion of a v1 node home.
//!
//! CI only. The proof creates a synthetic v1 node home, writes, overwrites,
//! and deletes records through the node key-value API, and closes it. It then
//! puts the home in the state that a crash during the switch leaves and checks
//! that the next open rolls back to v1 with the same data. Then it converts
//! the home with `LASTDB_DISK_FORMAT=2`, reads every key and three prefix
//! ranges, writes on the v2 home, reopens, reads again, checks the key
//! refusal and the kept v1 folder, and fails over 2x the live logical bytes.
//! It never opens a real LastDB home.

use fold_db::db_operations::DbOperations;
use fold_db::storage::disk_pack::{kept_v1_path, PackNamespacedStore};
use fold_db::storage::traits::{KvStore, NamespacedStore};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const RECORDS: usize = 50_000;
const BATCH: usize = 1_000;
const VALUE_BYTES: usize = 256;
const NAMESPACE: &str = "proof";
const V1_ENTRIES: [&str; 3] = ["disk-pack", "laststore-layout-v1", "laststore-pack-node-v1"];

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Records = BTreeMap<Vec<u8>, Vec<u8>>;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xd1b5_4a32_d192_ed03)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

fn key(n: usize) -> Vec<u8> {
    format!("k{n:06}").into_bytes()
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("disk_v2_convert_proof runs only in CI (GITHUB_ACTIONS is not set)");
        std::process::exit(2);
    }
    std::env::remove_var("LASTDB_DISK_FORMAT");
    let dir = tempfile::TempDir::new()?;
    let node = dir.path().join("node");
    let kept = kept_v1_path(&node)?;
    let mut seed = Rng::new(std::process::id() as u64);
    let node_key: [u8; 32] = seed.bytes(32).try_into().expect("32 bytes");

    // A v1 home with synthetic data.
    let store = reopen(&node, &node_key).await?;
    if store
        .disk_pack_runtime()
        .ok_or("no native runtime")?
        .is_disk_format_v2()
    {
        return Err("a new home without LASTDB_DISK_FORMAT=2 opened as v2".into());
    }
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    let mut expected = write_phase(&shared).await?;
    check(&shared, &expected, "v1 before conversion").await?;
    drop((shared, store));
    let v1_disk = directory_bytes(&node.join("disk-pack"))?;

    // The state a crash leaves after the v1 files moved and before the v2
    // marker: the next open moves them back.
    std::fs::create_dir(&kept)?;
    for name in V1_ENTRIES {
        std::fs::rename(node.join(name), kept.join(name))?;
    }
    std::fs::create_dir_all(node.join("disk-run"))?;
    let store = reopen(&node, &node_key).await?;
    if store
        .disk_pack_runtime()
        .ok_or("no native runtime")?
        .is_disk_format_v2()
        || kept.exists()
    {
        return Err("an unfinished switch did not roll back to v1".into());
    }
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    check(&shared, &expected, "v1 after rollback").await?;
    drop((shared, store));
    println!("rollback: an unfinished switch returned to v1 with the same data");

    // The conversion.
    std::env::set_var("LASTDB_DISK_FORMAT", "2");
    let started = Instant::now();
    let store = reopen(&node, &node_key).await?;
    let converted = started.elapsed();
    if !store
        .disk_pack_runtime()
        .ok_or("no native runtime")?
        .is_disk_format_v2()
    {
        return Err("LASTDB_DISK_FORMAT=2 did not convert the v1 home".into());
    }
    if !kept.join("disk-pack").is_dir() || node.join("disk-pack").exists() {
        return Err("the v1 files are not in the kept folder".into());
    }
    println!(
        "convert: {} records in {:.2} s (open included); v1 kept at {}",
        expected.len(),
        converted.as_secs_f64(),
        kept.file_name().unwrap_or_default().to_string_lossy()
    );
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    check(&shared, &expected, "v2 after conversion").await?;
    overwrite_phase(&shared, &mut expected).await?;
    check(&shared, &expected, "v2 after new writes").await?;
    drop((shared, store));

    std::env::remove_var("LASTDB_DISK_FORMAT");
    let store = reopen(&node, &node_key).await?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    check(&shared, &expected, "v2 after reopen").await?;
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    drop((shared, store));

    if reopen(&node, &[7u8; 32]).await.is_ok() {
        return Err("a wrong node key opened the converted home".into());
    }
    println!("wrong node key: refused");

    let logical: u64 = expected
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum();
    let disk = directory_bytes(&node)?;
    let ratio = disk as f64 / logical as f64;
    println!(
        "size: live records {}, logical {} B, v1 disk {} B, v2 disk {} B, ratio {:.2}x",
        expected.len(),
        logical,
        v1_disk,
        disk,
        ratio
    );
    if ratio > 2.0 {
        return Err(format!("disk is {ratio:.2}x the logical bytes; the limit is 2x").into());
    }
    println!("PASS: a v1 node home converts to v2 with every record equal, within 2x");
    Ok(())
}

/// Write, overwrite, and delete records; return the expected live records.
async fn write_phase(store: &Arc<dyn NamespacedStore>) -> Result<Records> {
    let kv = store.open_namespace(NAMESPACE).await?;
    let mut rng = Rng::new(1);
    let mut expected = BTreeMap::new();
    for chunk in (0..RECORDS).collect::<Vec<_>>().chunks(BATCH) {
        let items: Vec<_> = chunk
            .iter()
            .map(|&n| (key(n), rng.bytes(VALUE_BYTES)))
            .collect();
        expected.extend(items.iter().cloned());
        kv.batch_put(items).await?;
    }
    for chunk in (0..RECORDS).step_by(5).collect::<Vec<_>>().chunks(BATCH) {
        let items: Vec<_> = chunk
            .iter()
            .map(|&n| (key(n), rng.bytes(VALUE_BYTES)))
            .collect();
        expected.extend(items.iter().cloned());
        kv.batch_put(items).await?;
    }
    let deleted: Vec<_> = (3..RECORDS).step_by(10).map(key).collect();
    for chunk in deleted.chunks(BATCH) {
        kv.batch_delete(chunk.to_vec()).await?;
    }
    for key in &deleted {
        expected.remove(key);
    }
    store.restore_durability_barrier().await?;
    Ok(expected)
}

/// New writes and deletes on the converted home.
async fn overwrite_phase(store: &Arc<dyn NamespacedStore>, expected: &mut Records) -> Result<()> {
    let kv = store.open_namespace(NAMESPACE).await?;
    let mut rng = Rng::new(2);
    let items: Vec<_> = (0..RECORDS)
        .step_by(7)
        .map(|n| (key(n), rng.bytes(VALUE_BYTES)))
        .collect();
    expected.extend(items.iter().cloned());
    for chunk in items.chunks(BATCH) {
        kv.batch_put(chunk.to_vec()).await?;
    }
    let deleted: Vec<_> = (1..RECORDS).step_by(11).map(key).collect();
    for chunk in deleted.chunks(BATCH) {
        kv.batch_delete(chunk.to_vec()).await?;
    }
    for key in &deleted {
        expected.remove(key);
    }
    store.restore_durability_barrier().await?;
    Ok(())
}

/// Every key by batch read, and three prefix ranges, against the expected state.
async fn check(store: &Arc<dyn NamespacedStore>, expected: &Records, label: &str) -> Result<()> {
    let kv: Arc<dyn KvStore> = store.open_namespace(NAMESPACE).await?;
    let mut checked = 0usize;
    for chunk in (0..RECORDS).collect::<Vec<_>>().chunks(5_000) {
        let keys: Vec<_> = chunk.iter().map(|&n| key(n)).collect();
        let values = kv.get_many(keys.clone()).await?;
        for (key, value) in keys.iter().zip(values) {
            if value.as_ref() != expected.get(key) {
                let key = String::from_utf8_lossy(key);
                return Err(format!("{label}: key {key} does not match").into());
            }
            checked += 1;
        }
    }
    let mut ranged = 0usize;
    for prefix in ["k0000", "k0123", "k0499"] {
        let rows = kv.scan_prefix(prefix.as_bytes()).await?;
        let want: Vec<_> = expected
            .range(prefix.as_bytes().to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix.as_bytes()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if rows != want {
            return Err(format!("{label}: prefix {prefix} does not match").into());
        }
        ranged += rows.len();
    }
    println!("read {label}: {checked} keys equal, 3 prefixes with {ranged} records equal");
    Ok(())
}

/// Open after a close. The queue task can still hold the store for a
/// moment, so the wait is short and bounded.
async fn reopen(node: &Path, node_key: &[u8; 32]) -> Result<PackNamespacedStore> {
    let started = Instant::now();
    loop {
        match PackNamespacedStore::open_keyed(node, Some(node_key)) {
            Ok(store) => return Ok(store),
            Err(error) if error.to_string().contains("another") => {
                if started.elapsed() > Duration::from_secs(10) {
                    return Err(error.into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// File bytes under a small fixed tree.
fn directory_bytes(path: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}
