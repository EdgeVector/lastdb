//! Explicit native-home construction and bounded disk reads.
//! The node backend and memory acknowledgement path remain separate integrations.

use laststore::disk_pack::home::{DiskHome, HomeBuilder, HomeDescriptor};
use laststore::disk_pack::{MutationVersion, RangeBounds, ReadBudget};
use laststore::observe_disk_reads;
use serde_json::Value;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

#[path = "disk_home/io.rs"]
mod disk_home_io;
use disk_home_io::*;
#[path = "disk_home/output.rs"]
mod disk_home_output;
use disk_home_output::{CommandOutput, RangePage, RangeRecord, ReadResult};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const LINE_BYTES: u64 = 2_200_000;

fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("usage: lastdb-disk-home <build|plan|read|range|write> --home PATH --input JSON [--fanout N]");
        println!(
            "  build: ordered JSONL collection/key/body_hex or deleted; initial versions are (0,0)"
        );
        println!("  read: JSON object with coordinates:[{{collection,key}}] and optional budget");
        println!(
            "  range: JSON object with collection,prefix,start,end,page_size,max_pages,budget"
        );
        println!("  write: commit one complete JSONL batch directly to the snapshot");
        println!("  plan: complete snapshot quote for a JSONL batch; no writer is opened");
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "native disk home command failed: {}",
                disk_home_output::command_error(error.as_ref())
            );
            ExitCode::FAILURE
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn options() -> Result<(String, PathBuf, Option<PathBuf>, u16)> {
    let mut args = std::env::args().skip(1);
    let command = args.next().ok_or_else(|| invalid("command required"))?;
    if !["build", "plan", "read", "range", "write"].contains(&command.as_str()) {
        return Err(invalid("unknown command").into());
    }
    let (mut home, mut input, mut fanout) = (None, None, None);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| invalid("flag needs a value"))?;
        match flag.as_str() {
            "--home" if home.is_none() => home = Some(PathBuf::from(value)),
            "--input" if input.is_none() => input = Some(PathBuf::from(value)),
            "--fanout" if fanout.is_none() && command == "build" => fanout = Some(value.parse()?),
            _ => return Err(invalid("unknown or duplicate flag").into()),
        }
    }
    if input.is_none() {
        return Err(invalid("explicit input required").into());
    }
    Ok((
        command,
        home.ok_or_else(|| invalid("explicit home required"))?,
        input,
        fanout.unwrap_or(16),
    ))
}

fn run() -> Result<()> {
    let (command, home, input, fanout) = options()?;
    let started = Instant::now();
    let (result, disk) = observe_disk_reads(|| -> Result<CommandOutput> {
        if command == "build" {
            return build(&home, input.as_deref().expect("input"), fanout);
        }
        let home = DiskHome::open(&home)?;
        match command.as_str() {
            "plan" => plan(&home, input.as_deref().expect("input")),
            "write" => write(&home, input.as_deref().expect("input")),
            "read" | "range" => {
                let request = request(input.as_deref().expect("input"))?;
                if command == "read" {
                    read(&home, request)
                } else {
                    range(&home, request)
                }
            }
            _ => unreachable!("validated command"),
        }
    });
    disk_home_output::write_response(result?, started.elapsed().as_nanos(), disk)
}

fn build(home: &std::path::Path, input: &std::path::Path, fanout: u16) -> Result<CommandOutput> {
    let descriptor = HomeDescriptor::new(fanout, 0)?;
    let mut builder = HomeBuilder::create(home, descriptor.clone(), 0)?;
    let mut input = BufReader::new(File::open(input)?);
    let mut pending = next_line(&mut input)?;
    let mut count = 0u64;
    while let Some(first) = pending.take() {
        let collection = text(&first, "collection")?.to_string();
        let group = descriptor.group(text(&first, "key")?)?;
        let mut writer = builder.group(&collection, group)?;
        let mut row = first;
        loop {
            let key = text(&row, "key")?;
            let version = MutationVersion {
                batch: 0,
                operation: 0,
            };
            if row.get("deleted").and_then(Value::as_bool) == Some(true) {
                writer.tombstone(key, version)?;
            } else {
                let bytes = decode_hex(text(&row, "body_hex")?)?;
                writer.put(key, version, bytes.len() as u64, &mut bytes.as_slice())?;
            }
            count += 1;
            pending = next_line(&mut input)?;
            let Some(next) = pending.as_ref() else { break };
            if text(next, "collection")? != collection
                || descriptor.group(text(next, "key")?)? != group
            {
                break;
            }
            row = pending.take().expect("next row");
        }
        writer.finish()?;
    }
    let current = builder.finish()?;
    Ok(CommandOutput::Build {
        built: true,
        records: count,
        generation: current.generation,
        frontier: current.frontier,
        required_features: current.required_features,
        hash_fanout: fanout,
        placement_codec: current.placement_codec,
        key_codec: current.key_codec,
    })
}

fn budget(request: &Value) -> Result<ReadBudget> {
    let field = |name, default| {
        request
            .get("budget")
            .and_then(|b| b.get(name))
            .and_then(Value::as_u64)
            .unwrap_or(default)
    };
    let budget = ReadBudget {
        pages: field("pages", 4096),
        blocks: field("blocks", 16384),
        bytes: field("bytes", 256 * 1024 * 1024),
        keys: field("keys", 10000),
    };
    // The held native reader validates against its captured format feature.
    Ok(budget)
}

fn read(home: &DiskHome, request: Value) -> Result<CommandOutput> {
    let addresses = request
        .get("coordinates")
        .and_then(Value::as_array)
        .filter(|items| items.len() <= 10000)
        .ok_or_else(|| invalid("coordinates require at most 10000 keys"))?;
    let coordinates: Result<Vec<_>> = addresses
        .iter()
        .map(|item| {
            Ok((
                text(item, "collection")?.to_string(),
                text(item, "key")?.to_string(),
            ))
        })
        .collect();
    let mut call = home.read(budget(&request)?)?;
    let results = call
        .get_many(&coordinates?)?
        .into_iter()
        .map(|result| match result {
            Ok(body) => ReadResult::Body {
                body_hex: body.map(|bytes| hex(&bytes)),
            },
            Err(error) => ReadResult::failed(&error),
        })
        .collect();
    Ok(CommandOutput::Read {
        results,
        generation: call.current().generation,
        frontier: call.current().frontier,
        costs: call.costs().into(),
    })
}

fn range(home: &DiskHome, request: Value) -> Result<CommandOutput> {
    let page_size = request
        .get("page_size")
        .and_then(Value::as_u64)
        .unwrap_or(128);
    let max_pages = request
        .get("max_pages")
        .and_then(Value::as_u64)
        .unwrap_or(1);
    if !(1..=10000).contains(&page_size) || !(1..=10000).contains(&max_pages) {
        return Err(invalid("page size and page count require 1..10000").into());
    }
    let mut call = home.read(budget(&request)?)?;
    let mut cursor = call.hash_range(
        text(&request, "collection")?,
        text(&request, "prefix")?,
        RangeBounds {
            start: request
                .get("start")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            end: request
                .get("end")
                .and_then(Value::as_str)
                .map(str::to_string),
        },
    )?;
    let (mut pages, mut exhausted) = (Vec::new(), false);
    for _ in 0..max_pages {
        let page = cursor.next_page(page_size as usize)?;
        exhausted = page.exhausted;
        pages.push(RangePage {
            exhausted,
            records: page
                .records
                .into_iter()
                .map(|record| RangeRecord {
                    key: record.key,
                    body_hex: hex(&record.body),
                    batch: record.version.batch,
                    operation: record.version.operation,
                })
                .collect(),
        });
        if exhausted {
            break;
        }
    }
    drop(cursor);
    Ok(CommandOutput::Range {
        pages,
        exhausted,
        generation: call.current().generation,
        frontier: call.current().frontier,
        costs: call.costs().into(),
    })
}
