//! An isolated executable spike: whole records -> configured encoding -> commit.
mod format;
mod writer;

use clap::{Parser, Subcommand, ValueEnum};
use format::Format;
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
    #[arg(long, value_enum, default_value = "nested")]
    format: FormatChoice,
    #[command(subcommand)]
    action: Action,
}

#[derive(Clone, Copy, ValueEnum)]
enum FormatChoice {
    Plain,
    Nested,
}

#[derive(Subcommand)]
enum Action {
    /// Create a fresh spike home. An existing directory is refused.
    Init,
    /// Replace the complete record with the JSON object on stdin.
    Put { kind: String, key: String },
    /// Read one complete record; a missing key returns null.
    Get { kind: String, key: String },
    /// Read records through the format's named alternate key; returns primary keys.
    Lookup { kind: String, value: String },
    /// Perform at most this many native merge steps; zero reports current state.
    Maintain {
        #[arg(long, default_value_t = 8)]
        steps: u8,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.format {
        FormatChoice::Plain => run(cli, format::PLAIN),
        FormatChoice::Nested => run(cli, format::NESTED),
    }
}

fn run(cli: Cli, format: impl Format) -> Result<()> {
    match cli.action {
        Action::Init => {
            let definition: Value = serde_json::from_slice(&format.marker()?)?;
            writer::Store::create(&cli.home, format)?;
            println!(
                "{}",
                serde_json::json!({"ok": true, "created": true, "format": definition})
            );
        }
        Action::Put { kind, key } => {
            let fields = read_record(format.layout().max_record_bytes)?;
            let record = RecordReplacement::new(kind, key, fields);
            let receipt = writer::Store::open(&cli.home, format)?.replace(&record)?;
            println!(
                "{}",
                serde_json::json!({"ok": true, "frontier": receipt.frontier, "operations": receipt.operations})
            );
        }
        Action::Get { kind, key } => {
            let record = writer::Store::open(&cli.home, format)?.get(&kind, &key)?;
            println!("{}", serde_json::to_string(&record)?);
        }
        Action::Lookup { kind, value } => {
            let result = writer::Store::open(&cli.home, format)?.lookup(&kind, &value)?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Action::Maintain { steps } => {
            let report = writer::Store::open(&cli.home, format)?.maintain(steps)?;
            println!("{}", serde_json::to_string(&report)?);
        }
    }
    Ok(())
}

fn read_record(limit: usize) -> Result<Map<String, Value>> {
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
