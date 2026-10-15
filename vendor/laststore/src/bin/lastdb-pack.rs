//! Construct or read an explicitly selected immutable disk-pack artifact.
//! This command cannot activate the format in an existing LastStore home.

use laststore::disk_pack::{ArtifactBuilder, MutationVersion, ReadBudget, ReadCall};
use laststore::observe_disk_reads;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const LINE_LIMIT: u64 = 2_200_000;

struct Options {
    command: String,
    artifact: PathBuf,
    input: PathBuf,
    collection: String,
    group: u16,
    frontier: u64,
    placement: u16,
    key_codec: u16,
    budget: ReadBudget,
}

fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("usage: lastdb-pack <build|read|read-batch> --artifact PATH --input JSONL [--collection NAME --group N]");
        println!("  build: --frontier N; input objects: key, body_hex (or deleted=true), batch, operation");
        println!("  read: input JSON strings; --pages N --blocks N --bytes N --keys N");
        println!("  read-batch: input objects with collection, group, key; one shared read call");
        println!(
            "  both: --placement-codec N --key-codec N (default 1); explicit artifact paths only"
        );
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("disk-pack command failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn options() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or_else(|| invalid("build or read is required"))?;
    if command != "build" && command != "read" && command != "read-batch" {
        return Err(invalid("unknown command").into());
    }
    let mut values = BTreeMap::new();
    while let Some(name) = args.next() {
        if ![
            "--artifact",
            "--input",
            "--collection",
            "--group",
            "--frontier",
            "--placement-codec",
            "--key-codec",
            "--pages",
            "--blocks",
            "--bytes",
            "--keys",
        ]
        .contains(&name.as_str())
        {
            return Err(invalid("unknown argument").into());
        }
        let value = args
            .next()
            .ok_or_else(|| invalid("argument needs a value"))?;
        if values.insert(name, value).is_some() {
            return Err(invalid("duplicate argument").into());
        }
    }
    let required = |name: &str| {
        values
            .get(name)
            .cloned()
            .ok_or_else(|| invalid("required argument missing"))
    };
    let number = |name: &str, default: u64| -> Result<u64> {
        Ok(values
            .get(name)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(default))
    };
    Ok(Options {
        artifact: required("--artifact")?.into(),
        input: required("--input")?.into(),
        collection: if command == "read-batch" {
            String::new()
        } else {
            required("--collection")?
        },
        group: if command == "read-batch" {
            0
        } else {
            required("--group")?.parse()?
        },
        frontier: number("--frontier", 0)?,
        placement: number("--placement-codec", 1)?.try_into()?,
        key_codec: number("--key-codec", 1)?.try_into()?,
        budget: ReadBudget {
            pages: number("--pages", 512)?,
            blocks: number("--blocks", 1024)?,
            bytes: number("--bytes", 16 * 1024 * 1024)?,
            keys: number("--keys", 256)?,
        },
        command,
    })
}

fn run() -> Result<()> {
    let options = options()?;
    options.budget.validate()?;
    let mut input = BufReader::new(File::open(&options.input)?);
    match options.command.as_str() {
        "build" => build(&options, &mut input),
        "read" | "read-batch" => read(&options, &mut input),
        _ => unreachable!("validated command"),
    }
}

fn line(input: &mut impl BufRead) -> Result<Option<Value>> {
    let mut bytes = Vec::new();
    let count = input.take(LINE_LIMIT + 1).read_until(b'\n', &mut bytes)?;
    if count == 0 {
        return Ok(None);
    }
    if count as u64 > LINE_LIMIT {
        return Err(invalid("input record exceeds command byte limit").into());
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn build(options: &Options, input: &mut impl BufRead) -> Result<()> {
    let started = Instant::now();
    let mut builder = ArtifactBuilder::create(
        &options.artifact,
        options.placement,
        options.key_codec,
        1,
        options.frontier,
    )?;
    let mut group = builder.group(&options.collection, options.group)?;
    let (mut puts, mut deletes, mut body_bytes) = (0u64, 0u64, 0u64);
    while let Some(value) = line(input)? {
        let key = value
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("key must be a string"))?;
        let version = MutationVersion {
            batch: integer(&value, "batch")?,
            operation: integer(&value, "operation")?.try_into()?,
        };
        if value
            .get("deleted")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if value.get("body_hex").is_some() {
                return Err(invalid("tombstone contains body bytes").into());
            }
            group.tombstone(key, version)?;
            deletes += 1;
        } else {
            let text = value
                .get("body_hex")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("body_hex must be a string"))?;
            let body = decode_hex(text)?;
            group.put(key, version, body.len() as u64, &mut body.as_slice())?;
            puts += 1;
            body_bytes = body_bytes
                .checked_add(body.len() as u64)
                .ok_or_else(|| invalid("body byte count overflow"))?;
        }
    }
    group.finish()?;
    let current = builder.finish()?;
    println!(
        "{}",
        json!({"format":1,"artifact_only":true,"generation":current.generation,
        "frontier":current.frontier,"puts":puts,"tombstones":deletes,"body_bytes":body_bytes,
        "elapsed_nanos":started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64})
    );
    Ok(())
}

fn integer(value: &Value, name: &str) -> Result<u64> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("mutation version must be an unsigned integer").into())
}

fn read(options: &Options, input: &mut impl BufRead) -> Result<()> {
    let mut keys = Vec::new();
    let mut input_bytes = 0u64;
    while let Some(value) = line(input)? {
        if keys.len() as u64 >= options.budget.keys {
            return Err(invalid("input exceeds key budget").into());
        }
        let (collection, group, key) = if options.command == "read-batch" {
            (
                value
                    .get("collection")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("collection must be a string"))?,
                value
                    .get("group")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| invalid("group must be an integer"))?
                    .try_into()?,
                value
                    .get("key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("key must be a string"))?,
            )
        } else {
            (
                options.collection.as_str(),
                options.group,
                value
                    .as_str()
                    .ok_or_else(|| invalid("read input must contain JSON strings"))?,
            )
        };
        input_bytes = input_bytes
            .checked_add((collection.len() + key.len() + 4) as u64)
            .ok_or_else(|| invalid("input byte count overflow"))?;
        if collection.len() > 65_531
            || group >= 1024
            || key.len() > 65_535
            || input_bytes > options.budget.bytes
        {
            return Err(invalid("input keys exceed the byte budget or format limit").into());
        }
        keys.push((collection.to_string(), group, key.to_string()));
    }
    let addresses = keys
        .iter()
        .map(|(collection, group, key)| (collection.as_str(), *group, key.as_str()))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let (result, disk) = observe_disk_reads(|| -> Result<_> {
        let mut call = ReadCall::open(
            &options.artifact,
            options.placement,
            options.key_codec,
            options.budget,
        )?;
        let values = call.get_many(&addresses)?;
        Ok((values, call.costs()))
    });
    let (values, costs) = result?;
    let results = values
        .into_iter()
        .map(|body| body.map(|body| encode_hex(&body)))
        .collect::<Vec<_>>();
    println!(
        "{}",
        json!({"results":results,"costs":{
        "directory_pages":costs.directory_pages,"locator_pages":costs.locator_pages,
        "overflow_pages":costs.overflow_pages,"map_pages":costs.map_pages,
        "payload_blocks":costs.payload_blocks,"shared_pages":costs.shared_pages,"shared_blocks":costs.shared_blocks,
        "file_bytes":costs.file_bytes,"budget_bytes":costs.budget_bytes,"examined_keys":costs.examined_keys},
        "disk":{"group_opens":disk.group_opens,"legacy_replay_bytes":disk.legacy_replay_bytes,
        "journal_replay_bytes":disk.journal_replay_bytes,
        "footer_bytes":disk.footer_bytes,"directory_reads":disk.directory_reads,"file_opens":disk.file_opens,
        "read_operations":disk.read_operations,"payload_bytes":disk.payload_bytes,
        "format_probe_bytes":disk.format_probe_bytes,"temporary_bytes_peak":disk.temporary_bytes_peak},
        "filesystem_pages":"unknown","elapsed_nanos":started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64})
    );
    Ok(())
}

fn decode_hex(text: &str) -> Result<Vec<u8>> {
    if text.len() % 2 != 0 {
        return Err(invalid("body_hex must contain byte pairs").into());
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char)
                .to_digit(16)
                .ok_or_else(|| invalid("invalid hexadecimal body"))?;
            let lo = (pair[1] as char)
                .to_digit(16)
                .ok_or_else(|| invalid("invalid hexadecimal body"))?;
            Ok((hi * 16 + lo) as u8)
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}
