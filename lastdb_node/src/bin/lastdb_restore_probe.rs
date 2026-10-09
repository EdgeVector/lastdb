//! LastStore cloud restore probe verdict aggregator.
//!
//! The shell harness owns daemon lifecycle, cloud snapshot, and restore. This
//! binary refuses primary homes, compares the source/restored fixture evidence,
//! checks Mini search is an honest 503 (no native index), proves the primary pid set did
//! not change, and emits a single GREEN/RED verdict.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use clap::Parser;
use fold_db::hex::sha256_hex;
use lastdb_node::offline_home::{existing_primary_homes, refuse_primary};
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
        let source_hash = field_sha256(source, "body", content_key.as_ref())?;
        let restored_hash = field_sha256(restored, "body", content_key.as_ref())?;
        if field_plain(source, "bucket", content_key.as_ref())?.as_deref()
            == Some(record.bucket.as_str())
            && field_plain(restored, "bucket", content_key.as_ref())?.as_deref()
                == Some(record.bucket.as_str())
            && field_plain(source, "title", content_key.as_ref())?.as_deref()
                == Some(record.title.as_str())
            && field_plain(restored, "title", content_key.as_ref())?.as_deref()
                == Some(record.title.as_str())
            && source_hash == record.body_sha256
            && restored_hash == record.body_sha256
        {
            samples_matched += 1;
        } else {
            notes.push(format!("fixture mismatch id={}", record.id));
        }
    }

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

    let snapshot_report = snapshot.get("report").unwrap_or(&snapshot);
    let snapshot_counter = u64_field(snapshot_report, "counter");
    let snapshot_chunks_referenced = u64_field(snapshot_report, "chunks_referenced");
    if snapshot_counter == 0 || snapshot_chunks_referenced == 0 {
        notes.push("snapshot report did not show a committed LastStore backup".to_string());
    }

    let restore_counter = u64_field(&restore, "counter");
    let restore_chunks_installed = u64_field(&restore, "chunks_installed");
    let restore_bytes_installed = u64_field(&restore, "bytes_installed");
    if restore_counter == 0 || restore_chunks_installed == 0 || restore_bytes_installed == 0 {
        notes.push("restore report did not show installed backup chunks".to_string());
    }

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

    if let Some(path) = &args.report_out {
        let encoded =
            serde_json::to_vec_pretty(&report).map_err(|e| format!("encode report: {e}"))?;
        std::fs::write(path, encoded).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    Ok(report)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", path.display()))
}

fn rows_by_id(value: &Value, key: Option<&[u8; 32]>) -> Result<BTreeMap<String, Value>, String> {
    let results = value
        .get("results")
        .or_else(|| value.pointer("/data/results"))
        .and_then(Value::as_array)
        .ok_or_else(|| "query JSON missing results array".to_string())?;
    let mut out = BTreeMap::new();
    for result in results {
        let fields = result.get("fields").unwrap_or(result);
        let id = field_plain(fields, "id", key)?
            .ok_or_else(|| format!("query row missing string id: {fields}"))?;
        out.insert(id, fields.clone());
    }
    Ok(out)
}

fn field_str<'a>(row: &'a Value, field: &str) -> Option<&'a str> {
    row.get(field).and_then(Value::as_str)
}

fn load_content_key(path: Option<&Path>) -> Result<Option<[u8; 32]>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    if bytes.len() != 32 {
        return Err(format!(
            "{} must be 32 bytes, got {}",
            path.display(),
            bytes.len()
        ));
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    let e2e = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|e| format!("E2E derive from {}: {e}", path.display()))?;
    Ok(Some(e2e.encryption_key()))
}

fn open_field_text(raw: &str, key: Option<&[u8; 32]>) -> Result<String, String> {
    if !raw.starts_with("ENC:") {
        return Ok(raw.to_string());
    }
    let Some(key) = key else {
        return Err("ENC: field present but --identity-key was not provided".to_string());
    };
    let opened = fold_db::atom::open_content_value(key, Value::String(raw.to_string()))
        .map_err(|e| format!("open ENC: field: {e}"))?;
    match opened {
        Value::String(s) => Ok(s),
        other => Ok(other.to_string()),
    }
}

fn field_plain(row: &Value, field: &str, key: Option<&[u8; 32]>) -> Result<Option<String>, String> {
    let Some(raw) = field_str(row, field) else {
        return Ok(None);
    };
    Ok(Some(open_field_text(raw, key)?))
}

fn field_sha256(row: &Value, field: &str, key: Option<&[u8; 32]>) -> Result<String, String> {
    let value =
        field_plain(row, field, key)?.ok_or_else(|| format!("row missing string field {field}"))?;
    Ok(sha256_hex(value.as_bytes()))
}

fn u64_field(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or(0)
}

fn mini_search_plane_honest(semantic: &Value) -> bool {
    if native_index_served_hit(semantic, "") {
        return false;
    }
    let message = semantic
        .get("message")
        .or_else(|| semantic.get("error"))
        .and_then(Value::as_str)
        .unwrap_or("");
    semantic
        .get("search_plane_required")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || semantic
            .get("retired")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || message.contains("search_plane_required")
        || (!semantic.get("ok").and_then(Value::as_bool).unwrap_or(true)
            && message.to_ascii_lowercase().contains("lastseek"))
}

fn native_index_served_hit(semantic: &Value, phrase: &str) -> bool {
    semantic.get("ok").and_then(Value::as_bool).unwrap_or(false)
        && !semantic
            .get("degraded")
            .and_then(Value::as_bool)
            .unwrap_or(true)
        && semantic
            .get("results")
            .and_then(Value::as_array)
            .is_some_and(|results| {
                results.iter().any(|result| {
                    let match_type = result
                        .pointer("/metadata/match_type")
                        .and_then(Value::as_str);
                    let served = match_type == Some("semantic") || match_type == Some("index");
                    let value = result.get("value").and_then(Value::as_str).unwrap_or("");
                    served && value.contains(phrase)
                })
            })
}

fn write_primary_pids_out(argv: &[String]) -> Option<PathBuf> {
    argv.windows(2)
        .find_map(|pair| (pair[0] == "--write-primary-pids").then(|| PathBuf::from(&pair[1])))
}

fn write_primary_pid_census(out: &Path) -> Result<(), String> {
    write_pid_census(out, live_primary_lastdbd_pids())
}

/// A census that could not be taken is an error, never an empty file: an
/// empty `before` and an empty `after` would compare equal and let the
/// verdict claim `primary_pid_set_unchanged=true` without ever seeing a pid.
fn write_pid_census(out: &Path, census: Result<BTreeSet<String>, String>) -> Result<(), String> {
    let pids = census?;
    let mut text = String::new();
    for pid in &pids {
        text.push_str(pid);
        text.push('\n');
    }
    std::fs::write(out, text).map_err(|e| format!("write {}: {e}", out.display()))
}

/// Pids that are actually serving the live `~/.lastdb` (or `~/.folddb`)
/// primary — not repair copies, smoke COWs, safe-upgrade helpers, or
/// this probe's own throwaway daemons.
fn live_primary_lastdbd_pids() -> Result<BTreeSet<String>, String> {
    let homes = existing_primary_homes()?;
    live_primary_lastdbd_pids_from(pids_holding_primary_sockets(&homes), || {
        pids_from_process_table(&homes)
    })
}

/// `lsof` on the primary socket is the sandbox-safe source; the process
/// table is only consulted when the socket read names nobody. A process-table
/// read that fails propagates as `Err` so a denied `ps` can never yield an
/// empty-but-`Ok` census.
fn live_primary_lastdbd_pids_from(
    socket_holders: BTreeSet<String>,
    process_table: impl FnOnce() -> Result<BTreeSet<String>, String>,
) -> Result<BTreeSet<String>, String> {
    if socket_holders.is_empty() {
        process_table()
    } else {
        Ok(socket_holders)
    }
}

fn pids_holding_primary_sockets(homes: &[PathBuf]) -> BTreeSet<String> {
    let mut pids = BTreeSet::new();
    for home in homes {
        let sock = home.join("data").join("folddb.sock");
        if !sock.exists() {
            continue;
        }
        let output = std::process::Command::new("lsof")
            .args(["-t", "--"])
            .arg(&sock)
            .output();
        let Ok(output) = output else {
            continue;
        };
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let pid = line.trim();
            if pid.bytes().all(|b| b.is_ascii_digit()) && !pid.is_empty() {
                pids.insert(pid.to_string());
            }
        }
    }
    pids
}

fn pids_from_process_table(homes: &[PathBuf]) -> Result<BTreeSet<String>, String> {
    let output = std::process::Command::new("ps")
        .args(["-ax", "-o", "pid=,command="])
        .output()
        .map_err(|e| format!("ps: {e}"))?;
    pids_from_ps_output(&output, homes)
}

/// A `ps` that ran but exited non-zero (a seatbelt-denied sysctl prints an
/// error and exits 1) has not listed the table; treat it like a spawn failure
/// rather than an empty table.
fn pids_from_ps_output(
    output: &std::process::Output,
    homes: &[PathBuf],
) -> Result<BTreeSet<String>, String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("ps {}: {}", output.status, stderr.trim()));
    }
    let mut pids = BTreeSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((pid, command)) = line.split_once(char::is_whitespace) else {
            continue;
        };
        let pid = pid.trim();
        if !pid.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if command_serves_primary_home(command.trim(), homes) {
            pids.insert(pid.to_string());
        }
    }
    Ok(pids)
}

/// A process is the live primary iff it is actually `lastdbd` and either
/// `--data-dir` is a primary home or (no `--data-dir`) the binary itself
/// lives under a primary home. `~/.lastdb-test-copies/...` must not match
/// `~/.lastdb`.
fn command_serves_primary_home(command: &str, primary_homes: &[PathBuf]) -> bool {
    if !is_lastdbd_process(command) {
        return false;
    }
    if let Some(dir) = data_dir_arg(command) {
        return primary_homes.iter().any(|home| path_is_primary(&dir, home));
    }
    let bin = first_token(command);
    primary_homes
        .iter()
        .any(|home| path_is_primary(Path::new(bin), home))
}

fn is_lastdbd_process(command: &str) -> bool {
    Path::new(first_token(command))
        .file_name()
        .is_some_and(|name| name == "lastdbd")
}

fn first_token(command: &str) -> &str {
    command
        .split_whitespace()
        .next()
        .unwrap_or(command)
        .trim_matches(|c| c == '"' || c == '\'')
}

fn data_dir_arg(command: &str) -> Option<PathBuf> {
    let mut tokens = command.split_whitespace();
    while let Some(token) = tokens.next() {
        if let Some(rest) = token.strip_prefix("--data-dir=") {
            return Some(PathBuf::from(rest));
        }
        if token == "--data-dir" {
            return tokens.next().map(PathBuf::from);
        }
    }
    None
}

fn path_is_primary(path: &Path, home: &Path) -> bool {
    path == home || path.starts_with(home)
}

fn read_pid_set(path: &Path) -> Result<BTreeSet<String>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorruptRestoreEvidence {
    version: u32,
    control_chunks_installed: usize,
    control_value_restored: bool,
    corrupt_chunk_responses: usize,
    expected_sha256: String,
    served_sha256: String,
    digest_refused: bool,
    corrupt_chunk_not_installed: bool,
    restore_error: String,
}

impl CorruptRestoreEvidence {
    fn proven(&self) -> bool {
        let is_sha =
            |value: &str| value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit());
        self.version == 1
            && self.control_chunks_installed > 0
            && self.control_value_restored
            && self.corrupt_chunk_responses == 1
            && is_sha(&self.expected_sha256)
            && is_sha(&self.served_sha256)
            && self.expected_sha256 != self.served_sha256
            && self.digest_refused
            && self.corrupt_chunk_not_installed
            && !self.restore_error.is_empty()
    }
}

fn prove_corrupt_restore(out: &Path) -> Result<(), String> {
    #[cfg(not(feature = "cloud-sync"))]
    {
        let _ = out;
        Err("the corruption proof requires cloud-sync".to_string())
    }
    #[cfg(feature = "cloud-sync")]
    {
        refuse_primary(out)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let evidence = runtime.block_on(corruption::run())?;
        let json = serde_json::to_vec_pretty(&evidence).map_err(|e| e.to_string())?;
        std::fs::write(out, &json).map_err(|e| format!("write {}: {e}", out.display()))?;
        println!("{}", String::from_utf8_lossy(&json));
        if evidence.proven() {
            Ok(())
        } else {
            Err("corruption criteria not met".to_string())
        }
    }
}

/// Offline proof of the public restore boundary. All stores are generated here;
/// no primary files, credentials, or cloud endpoints enter this fixture.
#[cfg(feature = "cloud-sync")]
mod corruption {
    use super::*;
    use fold_db::storage::{laststore::BackupManifest, LastStoreNamespacedStore, NamespacedStore};
    use fold_db::sync::{
        auth::{AuthClient, SyncAuth},
        engine::{restore_laststore_cloud_backup, LastStoreCloudRestoreReport},
        error::{SyncError, SyncResult},
        s3::S3Client,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const MAX_FIXTURE_BYTES: u64 = 1024 * 1024;
    const KEY: &[u8] = b"atom:restore-corruption-proof";
    const VALUE: &[u8] = b"restore control value";

    struct Scratch(PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            // Only the exact fresh UUID directory created by this proof.
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open(root: &Path) -> Result<LastStoreNamespacedStore, String> {
        LastStoreNamespacedStore::open_with_high_water(
            &root.join("data"),
            root.join("high-water.json"),
        )
        .map_err(|e| e.to_string())
    }

    pub(super) async fn run() -> Result<CorruptRestoreEvidence, String> {
        let path =
            std::env::temp_dir().join(format!("lastdb-corrupt-restore-{}", uuid::Uuid::new_v4()));
        refuse_primary(&path)?;
        std::fs::create_dir(&path).map_err(|e| e.to_string())?;
        let scratch = Scratch(path);
        let source = open(&scratch.0.join("source"))?;
        source
            .open_namespace("main")
            .await
            .map_err(|e| e.to_string())?
            .put(KEY, VALUE.to_vec())
            .await
            .map_err(|e| e.to_string())?;
        let manifest = source
            .cut_backup_manifest(None)
            .map_err(|e| e.to_string())?;
        let chunk = manifest
            .atom_chunks
            .first()
            .ok_or("fixture has no atom chunk")?;
        let mut objects = BTreeMap::new();
        let mut total = 0u64;
        for candidate in source
            .enumerate_backup_chunk_candidates(None)
            .map_err(|e| e.to_string())?
        {
            total += std::fs::metadata(&candidate.path)
                .map_err(|e| e.to_string())?
                .len();
            if total > MAX_FIXTURE_BYTES {
                return Err("fixture exceeds 1 MiB".to_string());
            }
            objects.insert(
                candidate.chunk.sha256,
                std::fs::read(candidate.path).map_err(|e| e.to_string())?,
            );
        }
        let control = open(&scratch.0.join("control"))?;
        let (result, _) = attempt(&manifest, &objects, &control, None).await?;
        let control_report = result.map_err(|e| format!("valid control restore failed: {e}"))?;
        let control_value_restored = control
            .open_namespace("main")
            .await
            .map_err(|e| e.to_string())?
            .get(KEY)
            .await
            .map_err(|e| e.to_string())?
            .as_deref()
            == Some(VALUE);

        let corrupt = objects
            .get_mut(&chunk.sha256)
            .ok_or("fixture chunk absent")?;
        let last = corrupt.last_mut().ok_or("fixture chunk empty")?;
        *last ^= 1; // Same length: the download cap cannot stand in for the SHA guard.
        let served_sha256 = sha256_hex(corrupt);
        let target = open(&scratch.0.join("corrupt"))?;
        let (result, corrupt_chunk_responses) =
            attempt(&manifest, &objects, &target, Some(&chunk.sha256)).await?;
        // Demand the download boundary's typed Crypto error. If that guard is
        // removed, the install layer's Storage error must not satisfy the proof.
        let digest_refused = matches!(&result, Err(SyncError::Crypto(message))
            if message.contains("backup chunk sha256 mismatch")
                && message.contains(&chunk.sha256) && message.contains(&served_sha256));
        let restore_error = match result {
            Err(e) => e.to_string(),
            Ok(_) => String::new(),
        };
        let corrupt_chunk_not_installed = !target
            .enumerate_backup_chunk_candidates(None)
            .map_err(|e| e.to_string())?
            .iter()
            .any(|candidate| candidate.chunk.chunk_uuid == chunk.chunk_uuid);
        Ok(CorruptRestoreEvidence {
            version: 1,
            control_chunks_installed: control_report.chunks_installed,
            control_value_restored,
            corrupt_chunk_responses,
            expected_sha256: chunk.sha256.clone(),
            served_sha256,
            digest_refused,
            corrupt_chunk_not_installed,
            restore_error,
        })
    }

    async fn attempt(
        manifest: &BackupManifest,
        objects: &BTreeMap<String, Vec<u8>>,
        target: &LastStoreNamespacedStore,
        corrupt_sha: Option<&str>,
    ) -> Result<(SyncResult<LastStoreCloudRestoreReport>, usize), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let base = format!(
            "http://{}",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let http = Arc::new(
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .map_err(|e| e.to_string())?,
        );
        let auth = AuthClient::new(
            Arc::clone(&http),
            base.clone(),
            SyncAuth::ApiKey("local-proof-only".to_string()),
        )
        .with_request_timeout(Duration::from_secs(3));
        let s3 = S3Client::with_request_timeout(http, Duration::from_secs(3));
        let served = AtomicUsize::new(0);
        // Both futures are scoped. Completion or timeout drops the listener and
        // any accepted connection; no server task survives a failed restore.
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::select! {
                result = restore_laststore_cloud_backup(&auth, &s3, target) => Ok(result),
                result = serve(listener, &base, manifest, objects, corrupt_sha, &served) =>
                    Err(format!("mock server ended before restore: {result:?}")),
            }
        })
        .await
        .map_err(|_| "local restore exceeded 15 seconds".to_string())??;
        Ok((result, served.load(Ordering::Relaxed)))
    }

    async fn serve(
        listener: TcpListener,
        base: &str,
        manifest: &BackupManifest,
        objects: &BTreeMap<String, Vec<u8>>,
        corrupt_sha: Option<&str>,
        served: &AtomicUsize,
    ) -> Result<(), String> {
        let manifest_bytes = serde_json::to_vec(manifest).map_err(|e| e.to_string())?;
        let manifest_sha = sha256_hex(&manifest_bytes);
        for _ in 0..32 {
            let (mut stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let (route, body) = request(&mut stream).await?;
            let response = if route == "/api/sync/presign" {
                let body: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
                let response = match body["action"].as_str() {
                    Some("backup_latest_get") => {
                        serde_json::json!({"ok": true, "key": "local/latest",
                        "latest": {"store_uuid": manifest.store_uuid, "epoch": manifest.epoch,
                        "counter": manifest.counter, "manifest_sha256": manifest_sha, "updated_at_unix_secs": 1}})
                    }
                    Some("presign_backup_manifest_download")
                        if body["manifest_sha256"] == manifest_sha =>
                    {
                        serde_json::json!({"ok": true, "urls": [{"url": format!("{base}/manifest"), "method": "GET", "expires_in_secs": 60}]})
                    }
                    Some("presign_backup_chunk_download") => {
                        let sha = body["chunk_sha256"].as_str().ok_or("absent chunk SHA")?;
                        if !objects.contains_key(sha) {
                            return Err("unknown chunk SHA".to_string());
                        }
                        serde_json::json!({"ok": true, "urls": [{"url": format!("{base}/chunk/{sha}"), "method": "GET", "expires_in_secs": 60}]})
                    }
                    _ => return Err("unexpected local auth request".to_string()),
                };
                serde_json::to_vec(&response).map_err(|e| e.to_string())?
            } else if route == "/manifest" {
                manifest_bytes.clone()
            } else if let Some(sha) = route.strip_prefix("/chunk/") {
                let bytes = objects.get(sha).ok_or("unknown object")?;
                if Some(sha) == corrupt_sha {
                    served.fetch_add(1, Ordering::Relaxed);
                }
                bytes.clone()
            } else {
                return Err("unexpected local object route".to_string());
            };
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            );
            stream
                .write_all(header.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            stream
                .write_all(&response)
                .await
                .map_err(|e| e.to_string())?;
            stream.shutdown().await.map_err(|e| e.to_string())?;
        }
        Err("local request budget exhausted".to_string())
    }

    async fn request(stream: &mut TcpStream) -> Result<(String, Vec<u8>), String> {
        let mut bytes = Vec::new();
        loop {
            if bytes.len() >= 16 * 1024 {
                return Err("local request exceeds 16 KiB".to_string());
            }
            let mut buffer = [0u8; 1024];
            let n = stream.read(&mut buffer).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("local request ended early".to_string());
            }
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                let header = std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?;
                let length = header
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then_some(value.trim())
                    })
                    .unwrap_or("0")
                    .parse::<usize>()
                    .map_err(|e| e.to_string())?;
                if length > 8 * 1024 {
                    return Err("local request body exceeds 8 KiB".to_string());
                }
                if bytes.len() >= end + 4 + length {
                    let route = header
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .ok_or("absent local request route")?
                        .to_string();
                    return Ok((route, bytes[end + 4..end + 4 + length].to_vec()));
                }
            }
        }
    }
}
