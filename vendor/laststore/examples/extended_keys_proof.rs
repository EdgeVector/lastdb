//! One explicitly authorized CI-only native proof. No primary or user data is opened.

use lastdb_storage_policy::native_v1::{PAGES_PER_PART, PAGE_BYTES};
use laststore::disk_pack::home::{transfer, DiskHome, HomeBuilder, HomeDescriptor, HomeMutation};
use laststore::disk_pack::maintenance::SpacePolicy;
use laststore::disk_pack::{
    RangeBounds, ReadBudget, EXTENDED_STORAGE_KEY_BYTES, FEATURE_EXTENDED_KEYS,
};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const COLLECTION: &str = "extended-key-proof";
const HASH: &str = "generated-hash\0";

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
            return Err("this proof runs only in GitHub Actions".into());
        }
        let parent = fs::canonicalize(std::env::var("RUNNER_TEMP")?)?;
        let path = parent.join(format!("extended-keys-{}", uuid::Uuid::new_v4()));
        fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Only this create-new CI directory is owned by the proof.
        if let Err(error) = fs::remove_dir_all(&self.0) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!("extended_keys_proof: CI scratch cleanup failed");
            }
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 || args[0] != "--legacy-reader" {
        return Err("expected --legacy-reader CI_SOURCE_WORKER".into());
    }
    let scratch = Scratch::new()?;
    let legacy_reader = fs::canonicalize(&args[1])?;
    let extended = scratch.0.join("extended");
    let transferred = scratch.0.join("transferred");
    let legacy = scratch.0.join("legacy");
    let keys = vec![key('a', 1024 * 1024), key('b', 9 * 1024 * 1024)];
    assert!(keys[1].len() - 128 > PAGES_PER_PART as usize * (PAGE_BYTES - 128));
    HomeBuilder::create_empty_with_extended_keys(&extended, HomeDescriptor::new(1, 0)?)?;
    let home = DiskHome::open(&extended)?;
    // The first commit creates the locator. The second uses its sparse editor,
    // whose overflow chain must cross separately allocated metadata parts.
    for selected in &keys {
        put(&home, selected)?;
    }
    verify(&extended, &keys, true)?;
    verify_old_reader(&legacy_reader, &extended, false)?;
    let mut snapshot = home.snapshot()?;
    let compact = snapshot.compact(&SpacePolicy::default())?;
    assert_eq!(compact.records_copied, keys.len() as u64);
    drop(snapshot);
    drop(home);
    verify(&extended, &keys, true)?;
    transfer_round_trip(&scratch.0, &extended, &transferred)?;
    verify(&transferred, &keys, true)?;
    verify_old_reader(&legacy_reader, &transferred, false)?;
    verify_legacy(&legacy, &legacy_reader)?;
    verify_maximum(&scratch.0)?;
    fs::remove_dir_all(&scratch.0)?;
    println!("extended_keys_proof: passed; sizes=1048576,9437184,33554432; sparse_cross_part=true; exact=true; batch=true; range=true; compaction=true; transfer=true; reopen=true; legacy_refusal=true; old_reader_refusal=true; upper_boundary=true; maximum_keys=16; update=true; tombstone=true");
    Ok(())
}

fn key(first: char, bytes: usize) -> String {
    let mut key = format!("{HASH}{first}");
    key.push_str(&"q".repeat(bytes - key.len()));
    key
}

fn body(key: &str) -> Vec<u8> {
    format!(
        "generated-body:{}:{}",
        key.len(),
        key.as_bytes()[HASH.len()]
    )
    .into_bytes()
}

fn put(home: &DiskHome, key: &str) -> Result<()> {
    let operations = vec![HomeMutation {
        collection: COLLECTION.into(),
        key: key.into(),
        body: Some(body(key)),
    }];
    commit(home, operations)
}

fn commit(home: &DiskHome, operations: Vec<HomeMutation>) -> Result<()> {
    let mut snapshot = home.snapshot()?;
    let reservation = snapshot.reserve_mutation(&operations)?;
    snapshot.commit_mutation(operations, reservation)?;
    Ok(())
}

fn budget() -> ReadBudget {
    ReadBudget {
        pages: 4096,
        blocks: 16384,
        bytes: 256 * 1024 * 1024,
        keys: 10000,
    }
}

fn verify(path: &Path, keys: &[String], extended: bool) -> Result<()> {
    let home = DiskHome::open(path)?;
    let selected = home.read(budget())?;
    let limits = ReadBudget::for_current(selected.current());
    assert_eq!(
        selected.current().required_features & FEATURE_EXTENDED_KEYS != 0,
        extended
    );
    assert_eq!(
        selected.current().storage_key_byte_limit(),
        if extended {
            EXTENDED_STORAGE_KEY_BYTES
        } else {
            65535
        }
    );
    drop(selected);
    for key in keys {
        let mut read = home.read(limits)?;
        assert_eq!(read.get(COLLECTION, key)?, Some(body(key)));
        if key.len() > 8 * 1024 * 1024 {
            assert!(read.costs().overflow_pages > PAGES_PER_PART);
        }
    }
    for chunk in keys.chunks(2) {
        let coordinates: Vec<_> = chunk
            .iter()
            .map(|key| (COLLECTION.into(), key.clone()))
            .collect();
        let mut read = home.read(limits)?;
        let found = read.get_many(&coordinates)?;
        assert_eq!(found.len(), chunk.len());
        for (found, key) in found.into_iter().zip(chunk) {
            assert_eq!(found?, Some(body(key)));
        }
    }
    verify_ranges(&home, keys, limits)
}

fn verify_ranges(home: &DiskHome, keys: &[String], limits: ReadBudget) -> Result<()> {
    // Each explicit bounded interval fits one aggregate read allowance, even
    // when the complete hash contains more than 512 MiB of full stored keys.
    for (offset, chunk) in keys.chunks(2).enumerate() {
        let mut read = home.read(limits)?;
        let end = keys
            .get(2 * offset + chunk.len())
            .map(|key| key[HASH.len()..].to_string());
        let mut range = read.hash_range(
            COLLECTION,
            HASH,
            RangeBounds {
                start: chunk[0][HASH.len()..].to_string(),
                end,
            },
        )?;
        let page = range.next_page(chunk.len() + 1)?;
        assert!(page.exhausted && page.resume.is_none());
        assert_eq!(page.records.len(), chunk.len());
        for (record, key) in page.records.into_iter().zip(chunk) {
            assert!(record.key == *key, "range key bytes differ");
            assert_eq!(record.body, body(key));
        }
    }
    Ok(())
}

fn transfer_round_trip(parent: &Path, source: &Path, target: &Path) -> Result<()> {
    let stream = parent.join(format!("records-{}.bin", uuid::Uuid::new_v4()));
    let home = DiskHome::open(source)?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stream)?;
    let mut output = BufWriter::new(file);
    let receipt = transfer::export(&home, &mut output)?;
    output.flush()?;
    drop(output);
    let mut input = BufReader::new(File::open(&stream)?);
    assert_eq!(transfer::import(target, &mut input)?, receipt);
    Ok(())
}

fn verify_legacy(path: &Path, reader: &Path) -> Result<()> {
    HomeBuilder::create_empty_with_schema_record_refs(path, HomeDescriptor::new(1, 0)?)?;
    let home = DiskHome::open(path)?;
    assert!(home
        .read(ReadBudget {
            pages: 4097,
            ..budget()
        })
        .is_err());
    let keys = vec![key('a', 40000), key('b', 40000)];
    for key in &keys {
        put(&home, key)?;
    }
    verify(path, &keys, false)?;
    let before = fs::read(path.join("CURRENT"))?;
    let snapshot = home.snapshot()?;
    let oversized = vec![HomeMutation {
        collection: COLLECTION.into(),
        key: key('c', 65536),
        body: Some(vec![]),
    }];
    assert!(snapshot.reserve_mutation(&oversized).is_err());
    drop(snapshot);
    assert_eq!(fs::read(path.join("CURRENT"))?, before);
    drop(home);
    verify(path, &keys, false)?;
    verify_old_reader(reader, path, true)
}

fn verify_old_reader(reader: &Path, home: &Path, compatible: bool) -> Result<()> {
    let before = fs::read(home.join("CURRENT"))?;
    let output = Command::new(reader)
        .arg("verify")
        .arg("--home")
        .arg(home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .output()?;
    assert_eq!(output.status.success(), compatible);
    if !compatible {
        assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported CURRENT"));
    }
    assert_eq!(fs::read(home.join("CURRENT"))?, before);
    Ok(())
}

fn verify_maximum(parent: &Path) -> Result<()> {
    let source = parent.join("maximum");
    let target = parent.join("maximum-transfer");
    HomeBuilder::create_empty_with_extended_keys(&source, HomeDescriptor::new(1, 0)?)?;
    let home = DiskHome::open(&source)?;
    let mut keys: Vec<_> = (b'a'..=b'p')
        .map(|first| key(char::from(first), EXTENDED_STORAGE_KEY_BYTES))
        .collect();
    assert!(keys.len() * (EXTENDED_STORAGE_KEY_BYTES - 128).div_ceil(PAGE_BYTES - 128) > 32768);
    for key in &keys {
        put(&home, key)?;
    }
    verify(&source, &keys, true)?;
    commit(
        &home,
        vec![HomeMutation {
            collection: COLLECTION.into(),
            key: keys[7].clone(),
            body: Some(b"updated".to_vec()),
        }],
    )?;
    let limits = ReadBudget::for_current(home.read(budget())?.current());
    assert_eq!(
        home.read(limits)?.get(COLLECTION, &keys[7])?,
        Some(b"updated".to_vec())
    );
    put(&home, &keys[7])?;
    let deleted = keys.remove(8);
    commit(
        &home,
        vec![HomeMutation {
            collection: COLLECTION.into(),
            key: deleted.clone(),
            body: None,
        }],
    )?;
    assert_eq!(home.read(limits)?.get(COLLECTION, &deleted)?, None);
    let before = fs::read(source.join("CURRENT"))?;
    let snapshot = home.snapshot()?;
    let oversized = vec![HomeMutation {
        collection: COLLECTION.into(),
        key: key('c', EXTENDED_STORAGE_KEY_BYTES + 1),
        body: Some(vec![]),
    }];
    assert!(snapshot.reserve_mutation(&oversized).is_err());
    drop(snapshot);
    assert_eq!(fs::read(source.join("CURRENT"))?, before);
    let mut snapshot = home.snapshot()?;
    let compact = snapshot.compact(&SpacePolicy::default())?;
    assert_eq!(compact.records_copied, keys.len() as u64);
    assert_eq!(compact.tombstones_copied, 1);
    drop(snapshot);
    drop(home);
    verify(&source, &keys, true)?;
    transfer_round_trip(parent, &source, &target)?;
    verify(&target, &keys, true)?;
    assert_eq!(
        DiskHome::open(&target)?
            .read(limits)?
            .get(COLLECTION, &deleted)?,
        None
    );
    Ok(())
}
