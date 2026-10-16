//! Explicit local physical backup and source-independent restore.

use laststore::disk_pack::backup::{create_backup, restore_backup};
use laststore::disk_pack::home::DiskHome;
use serde_json::json;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("usage: lastdb-disk-recovery <backup|restore> --home PATH --backup PATH");
        println!("       lastdb-disk-recovery <convert|reverse> --home PATH --target PATH");
        println!("backup: explicit native root; checkpoint and export to a new bundle");
        println!("restore: verify the bundle and create a new native root; no source required");
        println!("convert/reverse: stopped source only; create a new destination and seal the old source");
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("native recovery command failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn options() -> Result<(String, PathBuf, PathBuf)> {
    let invalid = |message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message);
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(|| invalid("command required"))?;
    if !["backup", "restore", "convert", "reverse"].contains(&command.as_str()) {
        return Err(invalid("unknown command").into());
    }
    let (mut home, mut backup) = (None, None);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| invalid("flag needs a value"))?;
        match flag.as_str() {
            "--home" if home.is_none() => home = Some(PathBuf::from(value)),
            "--backup" if backup.is_none() => backup = Some(PathBuf::from(value)),
            "--target" if backup.is_none() => backup = Some(PathBuf::from(value)),
            _ => return Err(invalid("unknown or duplicate flag").into()),
        }
    }
    Ok((
        command,
        home.ok_or_else(|| invalid("explicit home required"))?,
        backup.ok_or_else(|| invalid("explicit backup required"))?,
    ))
}

fn run() -> Result<()> {
    let (command, home, backup) = options()?;
    let started = Instant::now();
    if command == "convert" || command == "reverse" {
        stopped(&home)?;
        let output = if command == "convert" {
            let source = laststore::LastStore::open(&home)?;
            let receipt = source.prepare_native_conversion(&backup)?.commit()?;
            json!({"copied_records":receipt.copied_records,"catchup_keys":receipt.catchup_keys,
                "source_csn":receipt.source_csn,"generation":receipt.generation,"frontier":receipt.frontier})
        } else {
            let receipt = laststore::disk_pack::conversion::reverse_to_legacy(
                &DiskHome::open(&home)?,
                &backup,
            )?;
            json!({"records":receipt.records,"stored_bytes":receipt.stored_bytes,
                "generation":receipt.generation,"frontier":receipt.frontier})
        };
        println!(
            "{}",
            json!({"command":command,"complete":true,"source_sealed":true,
            "receipt":output,"elapsed_nanos":started.elapsed().as_nanos()})
        );
        return Ok(());
    }
    let receipt = if command == "backup" {
        create_backup(&DiskHome::open(&home)?, &backup)?
    } else {
        restore_backup(&backup, &home)?
    };
    println!(
        "{}",
        json!({"command":command,"generation":receipt.generation,
        "frontier":receipt.frontier,"files":receipt.files,"bytes":receipt.bytes,
        "elapsed_nanos":started.elapsed().as_nanos(),"complete":true})
    );
    Ok(())
}

fn stopped(home: &std::path::Path) -> Result<()> {
    let home = std::fs::canonicalize(home)?;
    for root in [Some(home.as_path()), home.parent()].into_iter().flatten() {
        if root.join("current-session.json").try_exists()? {
            return Err("source has a current node session; use a clean stopped source".into());
        }
        for name in ["folddb.sock", "folddb-full.sock"] {
            if std::os::unix::net::UnixStream::connect(root.join(name)).is_ok() {
                return Err("source database socket is active".into());
            }
        }
    }
    Ok(())
}
