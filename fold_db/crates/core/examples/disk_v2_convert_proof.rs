//! Disk format v2, phase V4: the one-time conversion of a v1 node home.
//!
//! CI only. The proof creates a synthetic v1 node home, writes, overwrites,
//! and deletes records through the node key-value API, and closes it. It then
//! puts the home in states that a crash during the switch leaves (the intent
//! and a built copy, nothing moved; only the marker moved; all v1 files moved
//! and a run home in place) and checks that each next open rolls back to v1
//! with the same data. Then it converts the
//! home with `LASTDB_DISK_FORMAT=2`, reads every key and three prefix ranges,
//! writes on the v2 home, reopens, reads again, checks the key refusal and the
//! kept v1 folder, checks that a converted home that lost its marker is
//! refused and not rolled back, that an intent left after the v2 marker only
//! goes away, and fails over 2x the live logical bytes.
//!
//! The data includes CAS files (the `cas_blobs` namespace). After the
//! conversion they must be in `disk-cas` and not in `disk-run`, so that a
//! backup can send them to a different store.
//!
//! Each step opens the database operations with the node key, as the daemon
//! does, so the v1 home holds the content key proof that the conversion checks.
//!
//! Each open runs in its own child process of this program, as a daemon
//! restart does: a closed v1 store keeps background tasks until its process
//! ends. The crash states use the file names of `convert_v2`. It never opens
//! a real LastDB home.

use fold_db::atom::MoleculeKeyCodec;
use fold_db::db_operations::DbOperations;
use fold_db::storage::disk_pack::{kept_v1_path, PackNamespacedStore};
use fold_db::storage::traits::{KvStore, NamespacedStore};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

const RECORDS: usize = 50_000;
const BATCH: usize = 1_000;
const VALUE_BYTES: usize = 256;
const NAMESPACE: &str = "proof";
const CAS: &str = "cas_blobs";
const CAS_RECORDS: usize = 300;
const CAS_BYTES: usize = 16 * 1024;
/// The order in which the switch moves the v1 files.
const V1_ENTRIES: [&str; 3] = ["laststore-pack-node-v1", "laststore-layout-v1", "disk-pack"];
const INTENT: &str = "laststore-disk-convert-v2";

#[path = "disk_v2_convert_proof/data.rs"]
mod data;
use data::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Records = BTreeMap<Vec<u8>, Vec<u8>>;

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("GITHUB_ACTIONS").is_none() {
        eprintln!("disk_v2_convert_proof runs only in CI (GITHUB_ACTIONS is not set)");
        std::process::exit(2);
    }
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 4 {
        return step(&args[1], Path::new(&args[2]), &args[3]).await;
    }
    orchestrate()
}

/// The parent: it changes files between steps and runs each step as a child.
fn orchestrate() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let node = dir.path().join("node");
    let kept = kept_v1_path(&node)?;
    let mut seed = Rng::new(std::process::id() as u64);
    let node_key = hex(&seed.bytes(32));

    child("write-v1", &node, &node_key, false)?;
    let v1_disk = directory_bytes(&node.join("disk-pack"))?;

    check_rollbacks(&node, &kept, &node_key)?;

    let started = Instant::now();
    child("convert", &node, &node_key, true)?;
    println!(
        "convert: the process with the conversion, reads, and new writes took {:.2} s",
        started.elapsed().as_secs_f64()
    );
    if !kept.join("disk-pack").is_dir() || node.join("disk-pack").exists() {
        return Err("the v1 files are not in the kept folder".into());
    }
    if node.join(INTENT).exists() {
        return Err("the switch intent stayed after the conversion".into());
    }
    child("check-v2", &node, &node_key, false)?;

    let (main_logical, cas_logical) = check_cas_apart(&node)?;

    // An intent left after a durable v2 marker: the open only removes it.
    std::fs::write(node.join(INTENT), b"switch=v1-to-v2\n")?;
    child("check-v2", &node, &node_key, false)?;
    if node.join(INTENT).exists() || !kept.join("disk-pack").is_dir() {
        return Err("an intent after the v2 marker was not only removed".into());
    }
    println!("commit point: an intent after the v2 marker only went away");

    // A converted home that lost its marker has no intent: it is refused,
    // and nothing is rolled back or deleted.
    let marker = node.join("laststore-pack-node-v1");
    let aside = dir.path().join("marker-aside");
    std::fs::rename(&marker, &aside)?;
    child("refused", &node, &node_key, false)?;
    if !node.join("disk-run").is_dir() || node.join("disk-pack").exists() {
        return Err("a converted home without its marker was changed".into());
    }
    std::fs::rename(&aside, &marker)?;
    println!("lost marker: refused, and the v2 files stayed in place");

    child("refused", &node, &hex(&[7u8; 32]), false)?;
    println!("wrong node key: refused");
    child("check-v2", &node, &node_key, false)?;

    let expected = expected_v2();
    let logical = main_logical + cas_logical;
    let disk = directory_bytes(&node)?;
    let ratio = disk as f64 / logical as f64;
    println!(
        "size: live records {}, logical {} B, v1 disk {} B, v2 disk {} B, ratio {:.2}x",
        expected.len() + expected_cas_v2().len(),
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

/// States a crash leaves inside the switch. Each next open rolls back.
fn check_rollbacks(node: &Path, kept: &Path, node_key: &str) -> Result<()> {
    for (moved, label) in [
        (0, "intent and a built copy, nothing moved"),
        (1, "only the marker moved"),
        (3, "all v1 files moved"),
    ] {
        std::fs::write(node.join(INTENT), b"switch=v1-to-v2\n")?;
        if moved == 0 {
            std::fs::create_dir_all(node.join("disk-run.converting"))?;
            std::fs::create_dir_all(node.join("disk-cas.converting"))?;
        }
        std::fs::create_dir(kept)?;
        for name in &V1_ENTRIES[..moved] {
            std::fs::rename(node.join(name), kept.join(name))?;
        }
        if moved == 3 {
            std::fs::create_dir_all(node.join("disk-run"))?;
            std::fs::create_dir_all(node.join("disk-cas"))?;
        }
        child("check-v1", node, node_key, false)?;
        let left = [
            "disk-run",
            "disk-cas",
            "disk-run.converting",
            "disk-cas.converting",
        ]
        .iter()
        .any(|name| node.join(name).exists());
        if kept.exists() || node.join(INTENT).exists() || left {
            return Err(format!("crash state ({label}) left conversion files").into());
        }
        println!("rollback: crash state ({label}) returned to v1 with the same data");
    }

    Ok(())
}

/// The CAS files are separate files: all their bytes are in disk-cas, and
/// disk-run holds less than the other records take at 2x. Returns the
/// logical bytes of the records and of the CAS files.
fn check_cas_apart(node: &Path) -> Result<(u64, u64)> {
    let cas_logical: u64 = expected_cas_v2()
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum();
    let main_logical: u64 = expected_v2()
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum();
    let cas_disk = directory_bytes(&node.join("disk-cas"))?;
    let run_disk = directory_bytes(&node.join("disk-run"))?;
    println!(
        "cas: {} CAS files, {cas_logical} B logical, disk-cas {cas_disk} B, disk-run {run_disk} B",
        expected_cas_v2().len()
    );
    if cas_disk < cas_logical / 2 || run_disk > 2 * main_logical {
        return Err("the CAS files are not apart from the other records".into());
    }
    Ok((main_logical, cas_logical))
}

fn child(name: &str, node: &Path, node_key: &str, convert: bool) -> Result<()> {
    let mut command = Command::new(std::env::current_exe()?);
    command.args([name, &node.display().to_string(), node_key]);
    if convert {
        command.env("LASTDB_DISK_FORMAT", "2");
    } else {
        command.env_remove("LASTDB_DISK_FORMAT");
    }
    let status = command.status()?;
    if !status.success() {
        return Err(format!("step {name} failed: {status}").into());
    }
    Ok(())
}

/// One child: one open of the node home, then the process ends.
async fn step(name: &str, node: &Path, node_key: &str) -> Result<()> {
    let node_key = unhex(node_key)?;
    let opened = PackNamespacedStore::open_keyed(node, Some(&node_key));
    if name == "refused" {
        return match opened {
            Ok(_) => Err("the open was not refused".into()),
            Err(_) => Ok(()),
        };
    }
    let store = opened?;
    let v2 = store
        .disk_pack_runtime()
        .ok_or("no native runtime")?
        .is_disk_format_v2();
    let want_v2 = matches!(name, "convert" | "check-v2");
    if v2 != want_v2 {
        return Err(format!("step {name}: the home opened as v2={v2}").into());
    }
    let shared: Arc<dyn NamespacedStore> = Arc::new(store.clone());
    DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
        Arc::clone(&shared),
        Some(node_key),
        MoleculeKeyCodec::plain(),
        Some(node_key),
    )
    .await?;
    match name {
        "write-v1" => {
            write(&shared, NAMESPACE, v1_batches()).await?;
            write(&shared, CAS, cas_v1_batches()).await?;
            check_all(&shared, false, "v1 before conversion").await
        }
        "check-v1" => check_all(&shared, false, "v1 after rollback").await,
        "convert" => {
            check_all(&shared, false, "v2 after conversion").await?;
            write(&shared, NAMESPACE, v2_batches()).await?;
            write(&shared, CAS, cas_v2_batches()).await?;
            check_all(&shared, true, "v2 after new writes").await
        }
        "check-v2" => check_all(&shared, true, "v2 after reopen").await,
        _ => Err(format!("unknown step {name}").into()),
    }
}

async fn write(
    store: &Arc<dyn NamespacedStore>,
    namespace: &str,
    batches: Vec<Batch>,
) -> Result<()> {
    let kv = store.open_namespace(namespace).await?;
    for batch in batches {
        match batch {
            Batch::Put(items) => kv.batch_put(items).await?,
            Batch::Delete(keys) => kv.batch_delete(keys).await?,
        }
    }
    store.restore_durability_barrier().await?;
    Ok(())
}

/// The records and the CAS files against the expected state of v1 or v2.
async fn check_all(store: &Arc<dyn NamespacedStore>, v2: bool, label: &str) -> Result<()> {
    let (records, cas) = if v2 {
        (expected_v2(), expected_cas_v2())
    } else {
        (expected_v1(), expected_cas_v1())
    };
    check(store, &records, label).await?;
    let kv: Arc<dyn KvStore> = store.open_namespace(CAS).await?;
    let keys: Vec<_> = (0..CAS_RECORDS + 50).map(cas_key).collect();
    let values = kv.get_many(keys.clone()).await?;
    for (key, value) in keys.iter().zip(values) {
        if value.as_ref() != cas.get(key) {
            let key = String::from_utf8_lossy(key);
            return Err(format!("{label}: CAS file {key} does not match").into());
        }
    }
    println!("read {label}: {} CAS files equal", keys.len());
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

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Result<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return Err("a node key is 64 hex digits".into());
    }
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16)?;
    }
    Ok(key)
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
