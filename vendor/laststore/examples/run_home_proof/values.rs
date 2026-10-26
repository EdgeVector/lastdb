//! Compact stored values (phase V1b). Each value must read back with its
//! exact bytes after a reopen: by key, by batch, and by range. Node-shaped
//! JSON must take the compact form (tag 1) and fewer bytes. Texts near JSON
//! and other bytes may take either form, and keep their exact bytes.

use crate::gen::Rng;
use crate::value_gen::{near_json, node_value, other_bytes, text, timestamps, tree};
use crate::{budget, dir_bytes, Result, MIB};
use laststore::disk_run::home::{HomeMutation, RunHome, MAX_VALUE_BYTES};
use laststore::disk_run::value::{self, COMPACT_JSON};
use std::collections::BTreeMap;
use std::path::Path;

const COLLECTION: &str = "values";
const NODE_VALUES: u64 = 40_000;
const TREES: u64 = 5_000;
/// Node-shaped JSON must be stored in at most this share of its bytes.
const NODE_SHARE: f64 = 0.8;

/// The values of one kind.
struct Kind {
    name: &'static str,
    /// Every value must take the compact form, in at most `NODE_SHARE`.
    compact: bool,
    values: Vec<Vec<u8>>,
}

/// The table row, and the disk size ratio of the values home.
pub(crate) fn check(scratch: &Path, key: &[u8; 32]) -> Result<(String, f64)> {
    let mut rng = Rng::new(0x00c0_ffee_b1ab);
    let kinds = [
        Kind {
            name: "node-shaped JSON",
            compact: true,
            values: (0..NODE_VALUES).map(|n| node_value(&mut rng, n)).collect(),
        },
        Kind {
            name: "random JSON trees",
            compact: false,
            values: (0..TREES).map(|_| text(&tree(&mut rng, 0))).collect(),
        },
        Kind {
            name: "texts near JSON",
            compact: false,
            values: near_json(&mut rng),
        },
        Kind {
            name: "timestamps",
            compact: false,
            values: timestamps(&mut rng),
        },
        Kind {
            name: "other bytes",
            compact: false,
            values: other_bytes(&mut rng),
        },
    ];
    let mut notes = Vec::new();
    let mut expected = BTreeMap::new();
    for (index, kind) in kinds.into_iter().enumerate() {
        notes.push(codec(&kind)?);
        for (n, value) in kind.values.into_iter().enumerate() {
            expected.insert(format!("{index}:{n:08}"), value);
        }
    }
    let dir = scratch.join("values");
    write(&dir, key, &expected)?;
    read_back(&RunHome::open(&dir, key)?, &expected)?;
    let disk = dir_bytes(&dir)?;
    let logical: u64 = expected
        .iter()
        .map(|(k, v)| (COLLECTION.len() + k.len() + v.len()) as u64)
        .sum();
    let ratio = disk as f64 / logical as f64;
    let limit = check_limit(scratch, key)?;
    let row = format!(
        "{} values equal by key, batch, and range after reopen; {}; {limit}; {:.1} MiB on disk for {:.1} MiB logical: {ratio:.2}x",
        expected.len(),
        notes.join("; "),
        disk as f64 / MIB,
        logical as f64 / MIB
    );
    Ok((row, ratio))
}

/// Encode and decode each value of a kind without the store.
fn codec(kind: &Kind) -> Result<String> {
    let (mut compact, mut text_bytes, mut stored_bytes) = (0usize, 0usize, 0usize);
    for original in &kind.values {
        let stored = value::encode(original);
        compact += usize::from(stored[0] == COMPACT_JSON);
        text_bytes += original.len();
        stored_bytes += stored.len();
        if value::decode(stored, MAX_VALUE_BYTES)? != *original {
            return Err(format!("{}: a value decodes to different bytes", kind.name).into());
        }
    }
    let count = kind.values.len();
    let share = stored_bytes as f64 / text_bytes.max(1) as f64;
    if kind.compact && (compact != count || share > NODE_SHARE) {
        return Err(format!(
            "{}: {compact} of {count} compact, stored in {share:.2} of the bytes",
            kind.name
        )
        .into());
    }
    Ok(format!(
        "{}: {compact} of {count} compact, stored in {:.0}% of the bytes",
        kind.name,
        share * 100.0
    ))
}

/// Commit every value in batches of 1,000 with merges, newest keys first,
/// then close the home.
fn write(dir: &Path, key: &[u8; 32], expected: &BTreeMap<String, Vec<u8>>) -> Result<()> {
    let home = RunHome::create(dir, key)?;
    let entries: Vec<_> = expected.iter().collect();
    for chunk in entries.chunks(1000).rev() {
        let operations = chunk
            .iter()
            .map(|(k, v)| HomeMutation {
                collection: COLLECTION.to_string(),
                key: (*k).clone(),
                body: Some((*v).clone()),
            })
            .collect();
        home.commit(operations)?;
        while home.merge_step()?.is_some() {}
    }
    Ok(())
}

/// Every value by batch, a sample by key, and all of them by one range.
fn read_back(home: &RunHome, expected: &BTreeMap<String, Vec<u8>>) -> Result<()> {
    let mut read = home.read(budget());
    let entries: Vec<_> = expected.iter().collect();
    for chunk in entries.chunks(10_000) {
        let coordinates: Vec<(String, String)> = chunk
            .iter()
            .map(|(k, _)| (COLLECTION.to_string(), (*k).clone()))
            .collect();
        for ((k, v), found) in chunk.iter().zip(read.get_many(&coordinates)?) {
            if found?.as_ref() != Some(*v) {
                return Err(format!("batch read differs at {k}").into());
            }
        }
    }
    for (k, v) in entries.iter().step_by(31) {
        if read.get(COLLECTION, k)?.as_ref() != Some(*v) {
            return Err(format!("read by key differs at {k}").into());
        }
    }
    let mut ranged = Vec::new();
    let mut cursor = read.administrative_range(COLLECTION, b"", &[0xff])?;
    loop {
        let page = cursor.next_page(997)?;
        ranged.extend(page.records);
        if page.exhausted {
            break;
        }
    }
    let same = ranged.len() == entries.len()
        && ranged
            .iter()
            .zip(&entries)
            .all(|((rk, rv), (k, v))| rk == *k && rv == *v);
    if !same {
        return Err("the range read differs".into());
    }
    Ok(())
}

/// The largest value of a home reads back after a reopen; one more byte is
/// refused.
fn check_limit(scratch: &Path, key: &[u8; 32]) -> Result<String> {
    let dir = scratch.join("values-limit");
    let home = RunHome::create(&dir, key)?;
    let mut rng = Rng::new(7);
    let largest: Vec<u8> = (0..=MAX_VALUE_BYTES / 8)
        .flat_map(|_| rng.next_u64().to_le_bytes())
        .take(MAX_VALUE_BYTES)
        .collect();
    let put = |name: &str, body: Vec<u8>| HomeMutation {
        collection: COLLECTION.into(),
        key: name.into(),
        body: Some(body),
    };
    home.commit(vec![put("largest", largest.clone())])?;
    let refused = home
        .commit(vec![put("over", vec![0; MAX_VALUE_BYTES + 1])])
        .is_err();
    drop(home);
    let found = RunHome::open(&dir, key)?
        .read(budget())
        .get(COLLECTION, "largest")?;
    if found != Some(largest) || !refused {
        return Err("the largest value differs, or a larger value was accepted".into());
    }
    Ok(format!(
        "a {MAX_VALUE_BYTES} B value reads back, and one more byte is refused"
    ))
}
