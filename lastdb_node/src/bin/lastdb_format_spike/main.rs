//! An isolated executable spike: whole records -> configured encoding -> commit.
mod format;
mod writer;

use clap::{Parser, Subcommand};
use lastdb_model::record_replacement::RecordReplacement;
use serde_json::{Map, Value};
use std::io::Read;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(about = "Whole-record format writer spike; uses only its own fresh home")]
struct Cli {
    #[arg(long)]
    home: PathBuf,
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Create a fresh spike home. An existing directory is refused.
    Init,
    /// Replace the complete record with the JSON object on stdin.
    Put { kind: String, key: String },
    /// Read one complete record; a missing key returns null.
    Get { kind: String, key: String },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.action {
        Action::Init => {
            writer::Store::create(&cli.home)?;
            println!("{}", serde_json::json!({"ok": true, "created": true}));
        }
        Action::Put { kind, key } => {
            let fields = read_record()?;
            let record = RecordReplacement::new(kind, key, fields);
            let frontier = writer::Store::open(&cli.home)?.replace(&record)?;
            println!("{}", serde_json::json!({"ok": true, "frontier": frontier}));
        }
        Action::Get { kind, key } => {
            let record = writer::Store::open(&cli.home)?.get(&kind, &key)?;
            println!("{}", serde_json::to_string(&record)?);
        }
    }
    Ok(())
}

fn read_record() -> Result<Map<String, Value>> {
    let limit = format::HOME.max_record_bytes;
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err("input exceeds the configured record limit".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "stdin must contain one complete JSON object".into())
}
