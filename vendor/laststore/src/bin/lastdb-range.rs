//! Inspect a bounded ordered hash range in one separate artifact generation.

use laststore::disk_pack::{HashScope, RangeBounds, ReadBudget, ReadCall};
use laststore::observe_disk_reads;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Request {
    artifact: PathBuf,
    scope: HashScope,
    bounds: RangeBounds,
    budget: ReadBudget,
    page_size: usize,
    max_pages: usize,
    placement: u16,
    key_codec: u16,
}

fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("usage: lastdb-range --artifact PATH --input REQUEST.json [--placement-codec N --key-codec N]");
        println!("  request: collection, hash_prefix (one NUL-terminated encoded hash), groups, start, end");
        println!("  required budget: pages, blocks, bytes, keys; page_size and max_pages default to 128 and 1");
        println!(
            "  separate artifacts only; pages share one captured CURRENT and one total call budget"
        );
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("range command failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, reason)
}

fn request() -> Result<Request> {
    let mut args = std::env::args().skip(1);
    let mut values = BTreeMap::new();
    while let Some(name) = args.next() {
        if !["--artifact", "--input", "--placement-codec", "--key-codec"].contains(&name.as_str()) {
            return Err(invalid("unknown argument").into());
        }
        let value = args
            .next()
            .ok_or_else(|| invalid("argument needs a value"))?;
        if values.insert(name, value).is_some() {
            return Err(invalid("duplicate argument").into());
        }
    }
    let input = values
        .remove("--input")
        .ok_or_else(|| invalid("input path is required"))?;
    let value = read_request(&input)?;
    let string = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| invalid("required string field is absent"))
    };
    let groups = value
        .get("groups")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("groups must be an array"))?;
    if groups.len() > 16 {
        return Err(invalid("at most sixteen groups are permitted").into());
    }
    let groups = groups
        .iter()
        .map(|group| -> Result<u16> {
            Ok(group
                .as_u64()
                .ok_or_else(|| invalid("group must be an unsigned integer"))?
                .try_into()?)
        })
        .collect::<Result<Vec<_>>>()?;
    let budget = value
        .get("budget")
        .ok_or_else(|| invalid("a finite budget is required"))?;
    let budget = ReadBudget {
        pages: number(budget, "pages")?,
        blocks: number(budget, "blocks")?,
        bytes: number(budget, "bytes")?,
        keys: number(budget, "keys")?,
    };
    budget.validate()?;
    let page_size = value
        .get("page_size")
        .map(|_| number(&value, "page_size"))
        .transpose()?
        .unwrap_or(128)
        .try_into()?;
    let max_pages = value
        .get("max_pages")
        .map(|_| number(&value, "max_pages"))
        .transpose()?
        .unwrap_or(1)
        .try_into()?;
    if page_size == 0 || page_size as u64 > budget.keys || max_pages == 0 || max_pages > 10_000 {
        return Err(invalid("page size and page count must be positive and bounded").into());
    }
    let end = match value.get("end") {
        None | Some(Value::Null) => None,
        Some(Value::String(end)) => Some(end.clone()),
        _ => return Err(invalid("end must be a string or null").into()),
    };
    Ok(Request {
        artifact: values
            .remove("--artifact")
            .ok_or_else(|| invalid("artifact path is required"))?
            .into(),
        scope: HashScope::new(string("collection")?, string("hash_prefix")?, groups)?,
        bounds: RangeBounds {
            start: string("start")?,
            end,
        },
        budget,
        page_size,
        max_pages,
        placement: values
            .remove("--placement-codec")
            .map(|n| n.parse())
            .transpose()?
            .unwrap_or(1),
        key_codec: values
            .remove("--key-codec")
            .map(|n| n.parse())
            .transpose()?
            .unwrap_or(1),
    })
}

fn number(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid("required unsigned integer is absent").into())
}

fn read_request(path: &str) -> Result<Value> {
    let mut bytes = Vec::new();
    File::open(path)?.take(2_200_001).read_to_end(&mut bytes)?;
    if bytes.len() > 2_200_000 {
        return Err(invalid("request exceeds the command byte limit").into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn run() -> Result<()> {
    let request = request()?;
    laststore::disk_pack::require_artifact_path(&request.artifact)?;
    let started = Instant::now();
    let (result, disk) = observe_disk_reads(|| -> Result<_> {
        let mut call = ReadCall::open(
            &request.artifact,
            request.placement,
            request.key_codec,
            request.budget,
        )?;
        let mut cursor = call.hash_range(request.scope, request.bounds)?;
        let (mut count, mut exhausted) = (0, false);
        for number in 1..=request.max_pages {
            let page = cursor.next_page(request.page_size)?;
            let records = page
                .records
                .into_iter()
                .map(|record| {
                    json!({
                        "key":record.key,"body_hex":hex(&record.body),
                        "batch":record.version.batch,"operation":record.version.operation
                    })
                })
                .collect::<Vec<_>>();
            exhausted = page.exhausted;
            count += records.len();
            println!(
                "{}",
                json!({"page":number,"exhausted":exhausted,"records":records})
            );
            if exhausted {
                break;
            }
        }
        drop(cursor);
        Ok((call.current().generation, call.costs(), count, exhausted))
    });
    let (generation, costs, count, exhausted) = result?;
    println!(
        "{}",
        json!({"artifact_only":true,"generation":generation,"records_returned":count,"range_exhausted":exhausted,
        "costs":{"directory_pages":costs.directory_pages,"locator_pages":costs.locator_pages,"overflow_pages":costs.overflow_pages,
        "map_pages":costs.map_pages,"payload_blocks":costs.payload_blocks,"shared_pages":costs.shared_pages,"shared_blocks":costs.shared_blocks,
        "batch_read_runs":costs.batch_read_runs,"file_handles_peak":costs.file_handles_peak,"file_handle_reuses":costs.file_handle_reuses,
        "file_bytes":costs.file_bytes,"budget_bytes":costs.budget_bytes,"examined_keys":costs.examined_keys},
        "disk":{"group_opens":disk.group_opens,"legacy_replay_bytes":disk.legacy_replay_bytes,
        "footer_bytes":disk.footer_bytes,"directory_reads":disk.directory_reads,"file_opens":disk.file_opens,"read_operations":disk.read_operations,
        "format_probe_bytes":disk.format_probe_bytes,"payload_bytes":disk.payload_bytes,"temporary_bytes_peak":disk.temporary_bytes_peak},
        "filesystem_pages":"unknown","elapsed_nanos":started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64})
    );
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}
