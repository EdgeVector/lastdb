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
        println!("       lastdb-disk-recovery finish-conversion --home PATH --target PATH");
        println!("backup: explicit native root; export its complete snapshot to a new bundle");
        println!("restore: verify the bundle and create a new native root; no source required");
        println!("backup/restore include data only; retain the original node identity and encryption keys separately");
        println!(
            "finish-conversion: complete an existing forward fence; no source store is opened"
        );
        println!("generic convert and legacy reverse conversion are retired");
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
    if !["backup", "restore", "finish-conversion"].contains(&command.as_str()) {
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
    if command == "finish-conversion" {
        laststore::disk_pack::conversion::finish_conversion(&home, &backup)?;
        println!("{}", json!({"command":command,"complete":true}));
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
        "elapsed_nanos":started.elapsed().as_nanos(),"complete":true,"includes_identity":false})
    );
    Ok(())
}
