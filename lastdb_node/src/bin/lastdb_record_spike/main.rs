//! Isolated whole-record mutation CLI. Never opens a LastDB daemon home.
mod format;
mod writer;

use clap::{Parser, Subcommand, ValueEnum};
use lastdb_model::whole_record::{DeleteRecord, Key, PutRecord, Schema};
use serde::de::DeserializeOwned;
use std::io::Read;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(
    about = "Whole records, explicit keys, conditional replacement; isolated spike homes only"
)]
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
    /// Create a new spike home. Refuses an existing directory.
    Init,
    /// Install an immutable key declaration from stdin.
    Schema { name: String },
    /// Replace a whole record from stdin, including explicit keys and expectation.
    Put,
    /// Delete a record by key and revision from stdin.
    Delete,
    /// Read a complete record and revision by its external primary key.
    Get {
        schema: String,
        #[arg(long)]
        hash: String,
        #[arg(long)]
        range: Option<String>,
    },
    /// Read through a named alternate by an exact external key.
    Lookup {
        schema: String,
        name: String,
        #[arg(long)]
        hash: String,
        #[arg(long)]
        range: Option<String>,
    },
    Maintain {
        #[arg(long, default_value_t = 8)]
        steps: u8,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let format = match cli.format {
        FormatChoice::Plain => format::PLAIN,
        FormatChoice::Nested => format::NESTED,
    };
    if matches!(&cli.action, Action::Init) {
        writer::Store::create(&cli.home, format)?;
        println!(
            "{}",
            serde_json::json!({"created": true, "format": cli.format_name()})
        );
        return Ok(());
    }
    let store = writer::Store::open(&cli.home, format)?;
    let output = match cli.action {
        Action::Init => unreachable!(),
        Action::Schema { name } => {
            serde_json::to_value(store.install_schema(&name, &input::<Schema>()?)?)?
        }
        Action::Put => serde_json::to_value(store.put(input::<PutRecord>()?)?)?,
        Action::Delete => serde_json::to_value(store.delete(&input::<DeleteRecord>()?)?)?,
        Action::Get {
            schema,
            hash,
            range,
        } => serde_json::to_value(store.get(&schema, &Key { hash, range })?)?,
        Action::Lookup {
            schema,
            name,
            hash,
            range,
        } => serde_json::to_value(store.lookup(&schema, &name, &Key { hash, range })?)?,
        Action::Maintain { steps } => store.maintain(steps)?,
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

impl Cli {
    fn format_name(&self) -> &'static str {
        match self.format {
            FormatChoice::Plain => "plain",
            FormatChoice::Nested => "nested",
        }
    }
}

fn input<T: DeserializeOwned>() -> Result<T> {
    const LIMIT: u64 = 128 * 1024;
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > LIMIT {
        return Err("mutation input exceeds 128 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
