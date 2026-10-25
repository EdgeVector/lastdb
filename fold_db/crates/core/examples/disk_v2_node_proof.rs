//! Disk format v2, phase V2b: the node store on a v2 run home.
//!
//! CI only. The proof creates a synthetic v2 node home, opens the node
//! database operations on it, writes, overwrites, and deletes records through
//! the node key-value API, reads them back, closes and reopens the home,
//! reads again, checks the key refusal, and fails when the home uses more
//! than 2x the live logical bytes. It never opens a real LastDB home.

use fold_db::db_operations::DbOperations;
use fold_db::storage::disk_pack::PackNamespacedStore;
use fold_db::storage::traits::{KvStore, NamespacedStore};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const RECORDS: usize = 50_000;
const BATCH: usize = 1_000;
const VALUE_BYTES: usize = 256;
const NAMESPACE: &str = "proof";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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
        eprintln!("disk_v2_node_proof runs only in CI (GITHUB_ACTIONS is not set)");
        std::process::exit(2);
    }
    let dir = tempfile::TempDir::new()?;
    let node = dir.path().join("node");
    let mut seed = Rng::new(std::process::id() as u64);
    let node_key: [u8; 32] = seed.bytes(32).try_into().expect("32 bytes");

    let store = PackNamespacedStore::create_run(&node, &node_key)?;
    let runtime = store.disk_pack_runtime().ok_or("no native runtime")?;
    if !runtime.is_disk_format_v2() {
        return Err("create_run did not open a v2 home".into());
    }
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    println!("node database operations: opened on a new v2 home");

    let expected = write_phase(&shared).await?;
    check(&shared, &expected, "before reopen").await?;
    drop((shared, runtime, store));

    let store = reopen(&node, &node_key).await?;
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    check(&shared, &expected, "after reopen").await?;
    DbOperations::from_namespaced_store(Arc::clone(&shared)).await?;
    println!("node database operations: reopened on the v2 home");
    drop((shared, store));

    let wrong = reopen(&node, &[7u8; 32]).await;
    if wrong.is_ok() {
        return Err("a wrong node key opened the v2 home".into());
    }
    println!("wrong node key: refused");

    let logical: u64 = expected
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum();
    let disk = directory_bytes(&node)?;
    let ratio = disk as f64 / logical as f64;
    println!(
        "size: live records {}, logical {} B, disk {} B, ratio {:.2}x",
        expected.len(),
        logical,
        disk,
        ratio
    );
    if ratio > 2.0 {
        return Err(format!("disk is {ratio:.2}x the logical bytes; the limit is 2x").into());
    }
    println!("PASS: the node store works on a v2 run home within 2x");
    Ok(())
}

/// Write, overwrite, and delete records; return the expected live records.
async fn write_phase(store: &Arc<dyn NamespacedStore>) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let kv = store.open_namespace(NAMESPACE).await?;
    let mut rng = Rng::new(1);
    let mut expected = BTreeMap::new();
    let started = Instant::now();
    let mut commits = 0u64;
    for chunk in (0..RECORDS).collect::<Vec<_>>().chunks(BATCH) {
        let items: Vec<_> = chunk
            .iter()
            .map(|&n| (key(n), rng.bytes(VALUE_BYTES)))
            .collect();
        expected.extend(items.iter().cloned());
        kv.batch_put(items).await?;
        commits += 1;
    }
    for chunk in (0..RECORDS).step_by(5).collect::<Vec<_>>().chunks(BATCH) {
        let items: Vec<_> = chunk
            .iter()
            .map(|&n| (key(n), rng.bytes(VALUE_BYTES)))
            .collect();
        expected.extend(items.iter().cloned());
        kv.batch_put(items).await?;
        commits += 1;
    }
    let deleted: Vec<_> = (3..RECORDS).step_by(10).map(key).collect();
    for chunk in deleted.chunks(BATCH) {
        kv.batch_delete(chunk.to_vec()).await?;
        commits += 1;
    }
    for key in &deleted {
        expected.remove(key);
    }
    store.restore_durability_barrier().await?;
    let elapsed = started.elapsed();
    println!(
        "write: {commits} commits in {:.2} s ({:.1} ms per commit)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1000.0 / commits as f64
    );
    Ok(expected)
}

/// Every key by batch read, and three prefix ranges, against the expected state.
async fn check(
    store: &Arc<dyn NamespacedStore>,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    label: &str,
) -> Result<()> {
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

/// Reopen after the close. The close stops the merge thread; the queue task
/// can still hold the store for a moment, so the wait is short and bounded.
async fn reopen(node: &Path, node_key: &[u8; 32]) -> Result<PackNamespacedStore> {
    let started = Instant::now();
    loop {
        match PackNamespacedStore::open_keyed(node, Some(node_key)) {
            Ok(store) => {
                println!("reopen: opened in {:.2} s", started.elapsed().as_secs_f64());
                return Ok(store);
            }
            Err(error) if error.to_string().contains("another owner") => {
                if started.elapsed() > Duration::from_secs(10) {
                    return Err(error.into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

/// File bytes under the node home. The home is a small fixed tree.
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
