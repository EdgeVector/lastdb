//! Explicit native-home generation retention, compaction, and exact-path reclaim.

use laststore::disk_pack::home::DiskHome;
use laststore::disk_pack::lifetime::{
    list_references, release_reference, DurableReference, GenerationHold, ReferencePurpose,
};
use laststore::disk_pack::maintenance::{visit_generation, MaintenanceReceipt, SpacePolicy};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("usage: lastdb-pack-maintain <compact|reclaim|inventory|reference|references|release|hold|records> --home PATH [options]");
        println!("  compact/reclaim: --grace-ms N (default 86400000), --free-reserve-bytes N, --replacement-reserve-bytes N");
        println!("  reference: --purpose backup|rollback|conversion-source|cloud-recovery [--expiry-ms N]");
        println!("  release: --reference UUID; hold: --hold-seconds N (1..3600)");
        println!("  records prints original versions and body hashes through the supported administrative stream");
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("native file maintenance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, reason)
}

fn run() -> Result<()> {
    let started = Instant::now();
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(|| invalid("command required"))?;
    let mut values = BTreeMap::new();
    while let Some(flag) = args.next() {
        if ![
            "--home",
            "--grace-ms",
            "--free-reserve-bytes",
            "--replacement-reserve-bytes",
            "--purpose",
            "--expiry-ms",
            "--reference",
            "--hold-seconds",
        ]
        .contains(&flag.as_str())
        {
            return Err(invalid("unknown option").into());
        }
        let value = args
            .next()
            .ok_or_else(|| invalid("option requires a value"))?;
        if values.insert(flag, value).is_some() {
            return Err(invalid("duplicate option").into());
        }
    }
    let path = PathBuf::from(
        values
            .get("--home")
            .ok_or_else(|| invalid("explicit native home required"))?,
    );
    let home = DiskHome::open(&path)?;
    let number = |flag: &str, default: u64| -> Result<u64> {
        Ok(values
            .get(flag)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(default))
    };
    let policy = SpacePolicy {
        grace_ms: number("--grace-ms", 86_400_000)?,
        free_space_reserve_bytes: number("--free-reserve-bytes", 64 * 1024 * 1024)?,
        replacement_reserve_bytes: number("--replacement-reserve-bytes", 64 * 1024 * 1024)?,
        ..SpacePolicy::default()
    };
    let output = match command.as_str() {
        "compact" => maintenance(home.journal()?.compact(&policy)?),
        "reclaim" => maintenance(home.journal()?.reclaim(&policy)?),
        "inventory" => inventory(&path)?,
        "reference" => reference(&path, &values, number("--expiry-ms", 0)?)?,
        "references" => json!({"references":list_references(&path)?.iter().map(|item|json!({
            "reference":item.identity.to_string(),"generation":item.generation,"purpose":format!("{:?}",item.purpose),
            "created_ms":item.created_ms,"expiry_ms":item.expiry_ms})).collect::<Vec<_>>()}),
        "release" => {
            let identity = Uuid::parse_str(
                values
                    .get("--reference")
                    .ok_or_else(|| invalid("reference required"))?,
            )?;
            release_reference(&path, identity)?;
            json!({"released":identity.to_string()})
        }
        "hold" => {
            let seconds = number("--hold-seconds", 1)?;
            if !(1..=3600).contains(&seconds) {
                return Err(invalid("hold seconds require 1..3600").into());
            }
            let (current, _hold) = GenerationHold::capture(&path)?;
            println!("{}", json!({"held":true,"generation":current.generation}));
            io::stdout().flush()?;
            std::thread::sleep(std::time::Duration::from_secs(seconds));
            json!({"released":true,"generation":current.generation})
        }
        "records" => records(&path)?,
        _ => return Err(invalid("unknown command").into()),
    };
    println!(
        "{}",
        json!({"result":output,"elapsed_nanos":started.elapsed().as_nanos()})
    );
    Ok(())
}

fn maintenance(receipt: MaintenanceReceipt) -> Value {
    json!({"generation":receipt.generation,"records_copied":receipt.records_copied,
        "tombstones_copied":receipt.tombstones_copied,"parts_before":receipt.parts_before,
        "parts_after":receipt.parts_after,"files_removed":receipt.files_removed,
        "bytes_removed":receipt.bytes_removed,"reclaim_deferred":receipt.reclaim_deferred})
}

fn inventory(path: &std::path::Path) -> Result<Value> {
    let (_, hold) = GenerationHold::capture(path)?;
    let inventory = hold.inventory()?;
    Ok(
        json!({"generation":inventory.current.generation,"frontier":inventory.current.frontier,
        "receipt":inventory.receipt.to_string(),"files":inventory.files.iter().map(|file|json!({
            "path":file.path,"bytes":file.bytes,"creation_generation":file.creation_generation})).collect::<Vec<_>>()}),
    )
}

fn reference(
    path: &std::path::Path,
    values: &BTreeMap<String, String>,
    expiry: u64,
) -> Result<Value> {
    let purpose = match values.get("--purpose").map(String::as_str) {
        Some("backup") => ReferencePurpose::Backup,
        Some("rollback") => ReferencePurpose::Rollback,
        Some("conversion-source") => ReferencePurpose::ConversionSource,
        Some("cloud-recovery") => ReferencePurpose::CloudRecovery,
        _ => return Err(invalid("explicit supported reference purpose required").into()),
    };
    let (current, _hold) = GenerationHold::capture(path)?;
    let reference = DurableReference::create(path, &current, purpose, expiry)?;
    Ok(json!({"reference":reference.info().identity.to_string(),"generation":current.generation}))
}

fn records(path: &std::path::Path) -> Result<Value> {
    let (current, _hold) = GenerationHold::capture(path)?;
    let mut count = 0;
    visit_generation(path, &current, |record| {
        let mut hash = Sha256::new();
        if let Some(mut body) = record.body {
            io::copy(&mut body, &mut HashOutput(&mut hash))?;
        }
        println!(
            "{}",
            json!({"collection":record.collection,"group":record.group,"key":record.key,
            "batch":record.version.batch,"operation":record.version.operation,"body_bytes":record.body_bytes,
            "body_sha256":hex(&hash.finalize())})
        );
        count += 1;
        Ok(())
    })?;
    Ok(json!({"generation":current.generation,"records":count}))
}

struct HashOutput<'a>(&'a mut Sha256);
impl Write for HashOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                DIGITS[(byte >> 4) as usize] as char,
                DIGITS[(byte & 15) as usize] as char,
            ]
        })
        .collect()
}
