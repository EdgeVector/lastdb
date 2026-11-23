//! Check stored signed entries for the personal database proof. It changes nothing.
//!
//! - `entry_audit disk DATA_DIR` reads every stream under `DATA_DIR/logs/<database>/<node>/<device>/`
//!   and reports, for each, the floor (`floor.json`), the first and last stored seq, the count, the
//!   hash of the last entry and every problem found: a seq that is not the one in the file name, a
//!   bad signature, a gap, a first entry that does not follow the floor, or a `prev_hash` that is not
//!   the hash of the entry before it. It also lists the file digests that stored data entries name
//!   and the manifests that stored `snapshot` entries name.
//! - `entry_audit chain` reads `{"floor": null | {"seq", "hash"}, "entries": [...]}` on stdin (entries
//!   as `log/read` returned them, from the first one after the floor) and reports the same for them,
//!   with the hash of every entry. A read has no floor hash on a relay that does not show it, so the
//!   first entry is then not linked to the one before it.

use std::collections::BTreeSet;
use std::io::Read;
use std::path::Path;

use mutation_log::{entry_hash, verify_entry_signature, SignedEntry};
use serde_json::{json, Value};

const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// The stored entries of one stream, ascending by seq, and what is known of its floor.
struct Stream {
    entries: Vec<(u64, Value)>,
    floor: Option<(u64, String)>,
    /// True when the first entry must be the one after the floor (or seq 1 with no floor).
    strict_start: bool,
}

fn check(stream: &Stream) -> Value {
    let mut problems: Vec<String> = Vec::new();
    let mut hashes: Vec<Value> = Vec::new();
    let mut previous = stream.floor.clone();
    let mut kinds: BTreeSet<String> = BTreeSet::new();
    if stream.strict_start {
        let expected = stream.floor.as_ref().map_or(1, |(seq, _)| seq + 1);
        if let Some((first, _)) = stream
            .entries
            .first()
            .filter(|(first, _)| *first != expected)
        {
            problems.push(format!("the first entry is {first}, expected {expected}"));
        }
    }
    for (seq, value) in &stream.entries {
        let signed: SignedEntry = match serde_json::from_value(value.clone()) {
            Ok(signed) => signed,
            Err(error) => {
                problems.push(format!("entry {seq} does not parse: {error}"));
                previous = None;
                continue;
            }
        };
        let header = signed.header();
        kinds.insert(value["kind"].as_str().unwrap_or("?").to_owned());
        if header.writer_seq != *seq {
            problems.push(format!("entry {seq} says it is {}", header.writer_seq));
        }
        if verify_entry_signature(&signed).is_err() {
            problems.push(format!("entry {seq} has a bad signature"));
        }
        let Ok(hash) = entry_hash(&signed) else {
            problems.push(format!("entry {seq} cannot be hashed"));
            previous = None;
            continue;
        };
        let hash = hash.to_hex();
        let names = header.prev_hash.to_hex();
        if let Some((before, before_hash)) = &previous {
            if before + 1 == *seq && names != *before_hash {
                problems.push(format!(
                    "entry {seq} names {names} as its previous entry, which hashes to {before_hash}"
                ));
            }
            if before + 1 != *seq {
                problems.push(format!("a gap: entry {seq} follows entry {before}"));
            }
        } else if *seq == 1 && names != ZERO_HASH {
            problems.push("entry 1 names a previous entry".to_owned());
        }
        hashes.push(json!({ "seq": seq, "hash": hash }));
        previous = Some((*seq, hash));
    }
    json!({
        "floor": stream.floor.as_ref().map_or(0, |(seq, _)| *seq),
        "first": stream.entries.first().map_or(0, |(seq, _)| *seq),
        "last": stream.entries.last().map_or(0, |(seq, _)| *seq),
        "count": stream.entries.len(),
        "last_hash": hashes.last().map_or(Value::Null, |item| item["hash"].clone()),
        "kinds": kinds,
        "hashes": hashes,
        "problems": problems,
    })
}

fn read_json(path: &Path) -> Result<Value, String> {
    let bytes = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

fn subdirs(path: &Path) -> Vec<(String, std::path::PathBuf)> {
    let Ok(listed) = std::fs::read_dir(path) else {
        return Vec::new();
    };
    let mut found: Vec<(String, std::path::PathBuf)> = listed
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| {
            (
                entry.file_name().to_string_lossy().into_owned(),
                entry.path(),
            )
        })
        .collect();
    found.sort();
    found
}

fn disk(data_dir: &Path) -> Result<Value, String> {
    let mut streams = Vec::new();
    let mut files: BTreeSet<String> = BTreeSet::new();
    let mut snapshots = Vec::new();
    for (database, database_dir) in subdirs(&data_dir.join("logs")) {
        for (node, node_dir) in subdirs(&database_dir) {
            for (device, device_dir) in subdirs(&node_dir) {
                let mut entries: Vec<(u64, Value)> = Vec::new();
                for file in std::fs::read_dir(&device_dir)
                    .map_err(|error| error.to_string())?
                    .flatten()
                {
                    let name = file.file_name().to_string_lossy().into_owned();
                    let Some(seq) = name
                        .strip_suffix(".json")
                        .and_then(|stem| stem.parse::<u64>().ok())
                    else {
                        continue;
                    };
                    entries.push((seq, read_json(&file.path())?));
                }
                entries.sort_by_key(|(seq, _)| *seq);
                let floor_path = device_dir.join("floor.json");
                let floor = if floor_path.is_file() {
                    let value = read_json(&floor_path)?;
                    let seq = value["seq"].as_u64().ok_or("floor.json has no seq")?;
                    let hash = value["hash"].as_str().ok_or("floor.json has no hash")?;
                    Some((seq, hash.to_owned()))
                } else {
                    None
                };
                for (seq, value) in &entries {
                    if let Some(digest) = value["file"]["sha256"].as_str() {
                        files.insert(digest.to_owned());
                    }
                    if value["kind"] == "snapshot" {
                        snapshots.push(json!({
                            "device_id": device,
                            "seq": seq,
                            "manifest_sha256": value["manifest_sha256"],
                            "manifest_bytes": value["manifest_bytes"],
                        }));
                    }
                }
                let mut report = check(&Stream {
                    entries,
                    floor,
                    strict_start: true,
                });
                // The hash of every entry of a large stream is not needed here.
                report["hashes"] = Value::Null;
                report["database_id"] = json!(database);
                report["node_key"] = json!(node);
                report["device_id"] = json!(device);
                streams.push(report);
            }
        }
    }
    Ok(json!({
        "streams": streams,
        "named_files": files,
        "snapshots": snapshots,
    }))
}

fn floor_of(value: &Value) -> Result<Option<(u64, String)>, String> {
    let Some(floor) = value.get("floor").filter(|floor| floor.is_object()) else {
        return Ok(None);
    };
    let seq = floor["seq"].as_u64().ok_or("the floor has no seq")?;
    let hash = floor["hash"].as_str().ok_or("the floor has no hash")?;
    Ok(Some((seq, hash.to_owned())))
}

fn chain() -> Result<Value, String> {
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| error.to_string())?;
    let value: Value = serde_json::from_str(&input).map_err(|error| error.to_string())?;
    let floor = floor_of(&value)?;
    let mut entries = Vec::new();
    for entry in value["entries"].as_array().ok_or("no entries")? {
        let seq = entry["writer_seq"]
            .as_u64()
            .ok_or("an entry has no writer_seq")?;
        entries.push((seq, entry.clone()));
    }
    Ok(check(&Stream {
        entries,
        floor,
        strict_start: false,
    }))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let result = match (args.get(1).map(String::as_str), args.get(2)) {
        (Some("disk"), Some(dir)) => disk(Path::new(dir)),
        (Some("chain"), None) => chain(),
        _ => {
            eprintln!("usage: entry_audit disk DATA_DIR | entry_audit chain < entries.json");
            std::process::exit(2);
        }
    };
    match result {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("entry_audit: {error}");
            std::process::exit(1);
        }
    }
}
