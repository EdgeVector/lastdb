//! LastStore cloud restore probe verdict aggregator.
//!
//! The shell harness owns daemon lifecycle, cloud snapshot, and restore. This
//! binary refuses primary homes, compares the source/restored fixture evidence,
//! checks Mini search is an honest 503 (no native index), proves the primary pid set did
//! not change, and emits a single GREEN/RED verdict.

mod corruption;
mod fixture_io;
mod pids;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::Parser;
use corruption::{prove_corrupt_restore, CorruptRestoreEvidence};
use fixture_io::*;
use lastdb_node::offline_home::refuse_primary;
use pids::{read_pid_set, write_primary_pid_census, write_primary_pids_out};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(name = "lastdb_restore_probe")]
struct Args {
    #[arg(long)]
    source_home: PathBuf,
    #[arg(long)]
    restored_home: PathBuf,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    source_query: PathBuf,
    #[arg(long)]
    restored_query: PathBuf,
    #[arg(long)]
    semantic_search: PathBuf,
    #[arg(long)]
    snapshot_report: PathBuf,
    #[arg(long)]
    restore_report: PathBuf,
    #[arg(long)]
    primary_pids_before: PathBuf,
    #[arg(long)]
    primary_pids_after: PathBuf,
    /// Structured evidence from `--prove-corrupt-restore`, not a log message.
    #[arg(long)]
    red_path_log: PathBuf,
    #[arg(long)]
    report_out: Option<PathBuf>,
    /// Source-home `identity.key` (32-byte Ed25519 seed). Used to open
    /// field-level `ENC:` atom content the restored query may still return
    /// sealed when mutation-log apply stored at-rest payloads.
    #[arg(long)]
    identity_key: Option<PathBuf>,
    #[arg(long, default_value_t = true)]
    refuse_primary: bool,
}

#[derive(Debug, Deserialize)]
struct Fixture {
    records: Vec<FixtureRecord>,
    semantic_phrase: String,
}

#[derive(Debug, Deserialize)]
struct FixtureRecord {
    bucket: String,
    id: String,
    title: String,
    body_sha256: String,
}

#[derive(Debug, Serialize)]
struct ProbeReport {
    ok: bool,
    verdict: &'static str,
    source_home: String,
    restored_home: String,
    fixture_records: usize,
    source_records: usize,
    restored_records: usize,
    samples_matched: usize,
    semantic_index_served: bool,
    search_plane_honest: bool,
    snapshot_counter: u64,
    snapshot_chunks_referenced: u64,
    restore_counter: u64,
    restore_chunks_installed: u64,
    restore_bytes_installed: u64,
    primary_pid_set_unchanged: bool,
    red_path_proven: bool,
    notes: Vec<String>,
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("--prove-corrupt-restore") {
        let result = argv
            .get(2)
            .filter(|_| argv.len() == 3)
            .ok_or_else(|| "usage: --prove-corrupt-restore REPORT_JSON".to_string())
            .and_then(|out| prove_corrupt_restore(Path::new(out)));
        if let Err(error) = result {
            eprintln!("RED lastdb_restore_probe cause=corrupt_restore:{error}");
            std::process::exit(1);
        }
        return;
    }
    if let Some(out) = write_primary_pids_out(&argv) {
        match write_primary_pid_census(&out) {
            Ok(()) => return,
            Err(e) => {
                eprintln!("RED lastdb_restore_probe cause=primary_pid_census:{e}");
                std::process::exit(1);
            }
        }
    }
    match run() {
        Ok(report) if report.ok => {
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            println!(
                "GREEN lastdb_restore_probe records={} chunks_installed={} search_plane_honest=true primary_pid_set_unchanged=true red_path_proven=true",
                report.restored_records, report.restore_chunks_installed
            );
        }
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            eprintln!("RED lastdb_restore_probe cause=criteria_not_met");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("RED lastdb_restore_probe cause={e}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<ProbeReport, String> {
    let args = Args::parse();
    if args.refuse_primary {
        refuse_primary(&args.source_home)?;
        refuse_primary(&args.restored_home)?;
    }

    let fixture: Fixture = read_json(&args.fixture)?;
    let content_key = load_content_key(args.identity_key.as_deref())?;
    let source_rows = rows_by_id(
        &read_json::<Value>(&args.source_query)?,
        content_key.as_ref(),
    )?;
    let restored_rows = rows_by_id(
        &read_json::<Value>(&args.restored_query)?,
        content_key.as_ref(),
    )?;
    let snapshot = read_json::<Value>(&args.snapshot_report)?;
    let restore = read_json::<Value>(&args.restore_report)?;
    let semantic = read_json::<Value>(&args.semantic_search)?;

    let mut notes = Vec::new();
    let samples_matched = count_matched_records(
        &fixture,
        &source_rows,
        &restored_rows,
        content_key.as_ref(),
        &mut notes,
    )?;

    let semantic_index_served = native_index_served_hit(&semantic, &fixture.semantic_phrase);
    let search_plane_honest = mini_search_plane_honest(&semantic);
    if semantic_index_served {
        notes.push(
            "Mini served native-index hits; that is a strip-native-index regression (use LastSeek)"
                .to_string(),
        );
    }
    if !search_plane_honest {
        notes.push(
            "Mini native-index route was not an honest 503 search_plane_required".to_string(),
        );
    }

    let RestoreCounters {
        snapshot_counter,
        snapshot_chunks_referenced,
        restore_counter,
        restore_chunks_installed,
        restore_bytes_installed,
    } = read_restore_counters(&snapshot, &restore, &mut notes);
    let before = read_pid_set(&args.primary_pids_before)?;
    let after = read_pid_set(&args.primary_pids_after)?;
    let primary_pid_set_unchanged = before == after;
    if !primary_pid_set_unchanged {
        notes.push("primary lastdbd pid set changed".to_string());
    }

    let red_evidence: CorruptRestoreEvidence = read_json(&args.red_path_log)?;
    let red_path_proven = red_evidence.proven();
    if !red_path_proven {
        notes.push(
            "corrupt restore evidence did not prove the control and digest refusal".to_string(),
        );
    }

    let ok = notes.is_empty()
        && !fixture.records.is_empty()
        && source_rows.len() == fixture.records.len()
        && restored_rows.len() == fixture.records.len()
        && samples_matched == fixture.records.len()
        && search_plane_honest
        && !semantic_index_served
        && primary_pid_set_unchanged
        && red_path_proven;

    let report = ProbeReport {
        ok,
        verdict: if ok { "GREEN" } else { "RED" },
        source_home: args.source_home.display().to_string(),
        restored_home: args.restored_home.display().to_string(),
        fixture_records: fixture.records.len(),
        source_records: source_rows.len(),
        restored_records: restored_rows.len(),
        samples_matched,
        semantic_index_served,
        search_plane_honest,
        snapshot_counter,
        snapshot_chunks_referenced,
        restore_counter,
        restore_chunks_installed,
        restore_bytes_installed,
        primary_pid_set_unchanged,
        red_path_proven,
        notes,
    };

    write_report(&args, &report)?;
    Ok(report)
}

/// Counters read from the snapshot and restore reports.
struct RestoreCounters {
    snapshot_counter: u64,
    snapshot_chunks_referenced: u64,
    restore_counter: u64,
    restore_chunks_installed: u64,
    restore_bytes_installed: u64,
}

fn read_restore_counters(
    snapshot: &Value,
    restore: &Value,
    notes: &mut Vec<String>,
) -> RestoreCounters {
    let snapshot_report = snapshot.get("report").unwrap_or(snapshot);
    let snapshot_counter = u64_field(snapshot_report, "counter");
    let snapshot_chunks_referenced = u64_field(snapshot_report, "chunks_referenced");
    if snapshot_counter == 0 || snapshot_chunks_referenced == 0 {
        notes.push("snapshot report did not show a committed LastStore backup".to_string());
    }

    let restore_counter = u64_field(restore, "counter");
    let restore_chunks_installed = u64_field(restore, "chunks_installed");
    let restore_bytes_installed = u64_field(restore, "bytes_installed");
    if restore_counter == 0 || restore_chunks_installed == 0 || restore_bytes_installed == 0 {
        notes.push("restore report did not show installed backup chunks".to_string());
    }

    RestoreCounters {
        snapshot_counter,
        snapshot_chunks_referenced,
        restore_counter,
        restore_chunks_installed,
        restore_bytes_installed,
    }
}

/// Count the fixture records whose bucket, title and body hash match in both
/// the source and the restored home. Mismatches land in `notes`.
fn count_matched_records(
    fixture: &Fixture,
    source_rows: &BTreeMap<String, Value>,
    restored_rows: &BTreeMap<String, Value>,
    content_key: Option<&[u8; 32]>,
    notes: &mut Vec<String>,
) -> Result<usize, String> {
    let mut samples_matched = 0usize;
    for record in &fixture.records {
        let Some(source) = source_rows.get(&record.id) else {
            notes.push(format!("source missing id={}", record.id));
            continue;
        };
        let Some(restored) = restored_rows.get(&record.id) else {
            notes.push(format!("restored missing id={}", record.id));
            continue;
        };
        let source_hash = field_sha256(source, "body", content_key)?;
        let restored_hash = field_sha256(restored, "body", content_key)?;
        if field_plain(source, "bucket", content_key)?.as_deref() == Some(record.bucket.as_str())
            && field_plain(restored, "bucket", content_key)?.as_deref()
                == Some(record.bucket.as_str())
            && field_plain(source, "title", content_key)?.as_deref() == Some(record.title.as_str())
            && field_plain(restored, "title", content_key)?.as_deref()
                == Some(record.title.as_str())
            && source_hash == record.body_sha256
            && restored_hash == record.body_sha256
        {
            samples_matched += 1;
        } else {
            notes.push(format!("fixture mismatch id={}", record.id));
        }
    }
    Ok(samples_matched)
}

fn write_report(args: &Args, report: &ProbeReport) -> Result<(), String> {
    if let Some(path) = &args.report_out {
        let encoded =
            serde_json::to_vec_pretty(report).map_err(|e| format!("encode report: {e}"))?;
        std::fs::write(path, encoded).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    Ok(())
}
