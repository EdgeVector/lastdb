//! Construct and inspect complete journal batches in a separate pack artifact.

use laststore::disk_pack::journal::{JournalMutation, JournalRecovery, JournalWriter};
use laststore::disk_pack::publish_artifact_journal;
use laststore::observe_disk_reads;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Options {
    command: String,
    artifact: PathBuf,
    input: Option<PathBuf>,
    batch_size: usize,
    accepted_ms: u64,
}

fn main() -> ExitCode {
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        println!("usage: lastdb-journal <append|recover|publish> --artifact PATH [--input JSONL] [--batch-size N] [--accepted-ms N]");
        println!("  append input: collection, group, key, body_hex (or deleted=true)");
        println!("  recover emits verified batches, then a final complete=true summary");
        println!("  publish includes the captured complete journal suffix in artifact CURRENT");
        println!("  separate artifacts only; no home writer or memory ack change is enabled");
        return ExitCode::SUCCESS;
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("journal command failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, reason)
}

fn options() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let command = args
        .next()
        .ok_or_else(|| invalid("append, recover, or publish is required"))?;
    if command != "append" && command != "recover" && command != "publish" {
        return Err(invalid("unknown command").into());
    }
    let mut values = BTreeMap::new();
    while let Some(name) = args.next() {
        if !["--artifact", "--input", "--batch-size", "--accepted-ms"].contains(&name.as_str()) {
            return Err(invalid("unknown argument").into());
        }
        let value = args
            .next()
            .ok_or_else(|| invalid("argument needs a value"))?;
        if values.insert(name, value).is_some() {
            return Err(invalid("duplicate argument").into());
        }
    }
    let artifact = values
        .remove("--artifact")
        .ok_or_else(|| invalid("artifact path is required"))?
        .into();
    let input = values.remove("--input").map(PathBuf::from);
    if (command == "append") != input.is_some() {
        return Err(invalid("only append requires an input file").into());
    }
    let batch_size = values
        .remove("--batch-size")
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(64);
    if !(1..=4096).contains(&batch_size) {
        return Err(invalid("batch size must be 1 through 4096").into());
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_millis())
        .unwrap_or(0);
    let accepted_ms = values
        .remove("--accepted-ms")
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(u64::try_from(now)?);
    Ok(Options {
        command,
        artifact,
        input,
        batch_size,
        accepted_ms,
    })
}

fn run() -> Result<()> {
    let options = options()?;
    if options.command == "append" {
        append(&options)
    } else if options.command == "publish" {
        publish(&options)
    } else {
        recover(&options)
    }
}

fn publish(options: &Options) -> Result<()> {
    let started = Instant::now();
    let (result, disk) = observe_disk_reads(|| publish_artifact_journal(&options.artifact));
    let result = result?;
    println!(
        "{}",
        json!({
            "complete":true,"artifact_only":true,"generation":result.current.generation,
            "previous_generation":result.current.previous_generation,"frontier":result.current.frontier,
        "receipt_uuid":result.receipt.to_string(),"dependency_parts":result.dependency_parts,
            "mutations":result.mutations,"metadata_pages_written":result.metadata_pages_written,
            "metadata_parts_written":result.metadata_parts_written,
            "group_opens":disk.group_opens,"legacy_replay_bytes":disk.legacy_replay_bytes,
            "journal_replay_bytes":disk.journal_replay_bytes,"footer_bytes":disk.footer_bytes,
            "directory_reads":disk.directory_reads,"file_opens":disk.file_opens,
            "read_operations":disk.read_operations,"format_probe_bytes":disk.format_probe_bytes,
            "temporary_bytes_peak":disk.temporary_bytes_peak,
            "elapsed_nanos":started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
        })
    );
    Ok(())
}

fn append(options: &Options) -> Result<()> {
    let started = Instant::now();
    let mut input = BufReader::new(File::open(options.input.as_ref().expect("append input"))?);
    let mut writer = JournalWriter::create(&options.artifact)?;
    let mut pending = Vec::new();
    let (mut bytes, mut batches, mut count) = (256u64, 0u64, 0u64);
    while let Some(mutation) = next_mutation(&mut input)? {
        bytes = bytes
            .checked_add(mutation.encoded_bytes()?)
            .ok_or_else(|| invalid("batch length overflow"))?;
        if bytes > 128 * 1024 * 1024 {
            return Err(invalid("batch input exceeds the 128 MiB admission limit").into());
        }
        pending.push(mutation);
        if pending.len() == options.batch_size {
            count += pending.len() as u64;
            let result = writer.append(std::mem::take(&mut pending), options.accepted_ms)?;
            println!(
                "{}",
                json!({"batch_durable":true,"sequence":result.sequence,
                "operations":result.locations.len(),"encoded_bytes":result.encoded_bytes,
                "marker_sha256":hex(&result.marker_digest)})
            );
            bytes = 256;
            batches += 1;
        }
    }
    if !pending.is_empty() {
        count += pending.len() as u64;
        let result = writer.append(pending, options.accepted_ms)?;
        println!(
            "{}",
            json!({"batch_durable":true,"sequence":result.sequence,
            "operations":result.locations.len(),"encoded_bytes":result.encoded_bytes,
            "marker_sha256":hex(&result.marker_digest)})
        );
        batches += 1;
    }
    println!(
        "{}",
        json!({"complete":true,"artifact_only":true,"batches":batches,"mutations":count,
        "first_batch":writer.tail().first_batch,"last_batch":writer.tail().last_batch,
        "encoded_bytes":writer.tail().encoded_bytes,"reserved_suffix_bytes":writer.reserved_suffix_bytes(),
        "elapsed_nanos":started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64})
    );
    Ok(())
}

fn next_mutation(input: &mut impl BufRead) -> Result<Option<JournalMutation>> {
    const LIMIT: u64 = 2_200_000;
    let mut bytes = Vec::new();
    let count = input.take(LIMIT + 1).read_until(b'\n', &mut bytes)?;
    if count == 0 {
        return Ok(None);
    }
    if count as u64 > LIMIT {
        return Err(invalid("input record exceeds command byte limit").into());
    }
    let value: Value = serde_json::from_slice(&bytes)?;
    let string = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("missing string field"))
    };
    let deleted = match value.get("deleted") {
        None => false,
        Some(Value::Bool(flag)) => *flag,
        _ => return Err(invalid("deleted must be a boolean").into()),
    };
    let body = if deleted {
        if value.get("body_hex").is_some() {
            return Err(invalid("tombstone contains body bytes").into());
        }
        None
    } else {
        Some(decode_hex(string("body_hex")?)?)
    };
    let mutation = JournalMutation {
        collection: string("collection")?.to_string(),
        group: value
            .get("group")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("group must be an integer"))?
            .try_into()?,
        key: string("key")?.to_string(),
        body,
    };
    mutation.encoded_bytes()?;
    Ok(Some(mutation))
}

fn recover(options: &Options) -> Result<()> {
    let started = Instant::now();
    let (result, disk) = observe_disk_reads(|| -> Result<_> {
        let mut recovery = JournalRecovery::open(&options.artifact)?;
        let (mut batches, mut count) = (0u64, 0u64);
        while let Some(batch) = recovery.next_batch()? {
            let mutations = batch.mutations.iter().map(|item| json!({
                "collection":item.mutation.collection,"group":item.mutation.group,
                "storage_id_digest":hex(&Sha256::digest(item.mutation.key.as_bytes())),
                "body_bytes":item.mutation.body.as_ref().map(Vec::len),
                "body_sha256":item.mutation.body.as_ref().map(|body| hex(&Sha256::digest(body))),
                "operation":item.location.version.operation,"record_sha256":hex(&item.location.digest),
                "first_block":item.location.first_block,"first_position":item.location.first_position,
                "blocks":item.location.blocks})).collect::<Vec<_>>();
            count += batch.mutations.len() as u64;
            println!(
                "{}",
                json!({"batch_verified":true,"sequence":batch.sequence,"previous":batch.previous,
                "accepted_ms":batch.accepted_ms,"encoded_bytes":batch.encoded_bytes,
                "marker_sha256":hex(&batch.marker_digest),"mutations":mutations})
            );
            batches += 1;
        }
        if !recovery.is_complete() {
            return Err(invalid("recovery did not reach its captured TAIL").into());
        }
        Ok((batches, count))
    });
    let (batches, count) = result?;
    println!(
        "{}",
        json!({"complete":true,"artifact_only":true,"batches":batches,"mutations":count,
        "journal_replay_bytes":disk.journal_replay_bytes,"legacy_replay_bytes":disk.legacy_replay_bytes,
        "group_opens":disk.group_opens,"directory_reads":disk.directory_reads,"footer_bytes":disk.footer_bytes,
        "file_opens":disk.file_opens,"read_operations":disk.read_operations,"format_probe_bytes":disk.format_probe_bytes,
        "decoded_bytes":disk.decoded_bytes,"temporary_bytes_peak":disk.temporary_bytes_peak,
        "elapsed_nanos":started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64})
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

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 15) as usize] as char);
    }
    text
}
