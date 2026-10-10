//! Rewrite one plain LastDB store while its daemon is stopped.
//!
//! The tool refuses to open a store whose socket still accepts a connection.
//!
//! The `reap` subcommand drops the keys that a plan directory names.

mod reap;

use laststore::{home_has_frame_aead_segments, LastStore, MaintenanceReport};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

const ORDER_LOG_RETENTION_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("maintenance failed: {error}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    if env::args().nth(1).as_deref() == Some("reap") {
        return Ok(reap::run(env::args().skip(2)));
    }
    let mut data_dir: Option<PathBuf> = None;
    let mut execute = false;
    let mut cutoff: Option<u64> = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                return Ok(ExitCode::SUCCESS);
            }
            "--execute" => execute = true,
            "--data-dir" => {
                let Some(value) = args.next() else {
                    eprintln!("--data-dir needs a path");
                    print_usage();
                    return Ok(ExitCode::from(2));
                };
                data_dir = Some(PathBuf::from(value));
            }
            "--cutoff-nanos" => {
                let Some(value) = args.next() else {
                    eprintln!("--cutoff-nanos needs an integer");
                    return Ok(ExitCode::from(2));
                };
                let Ok(parsed) = value.parse::<u64>() else {
                    eprintln!("--cutoff-nanos is not an integer");
                    return Ok(ExitCode::from(2));
                };
                cutoff = Some(parsed);
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage();
                return Ok(ExitCode::from(2));
            }
        }
    }
    let Some(data_dir) = data_dir else {
        eprintln!("--data-dir is required");
        print_usage();
        return Ok(ExitCode::from(2));
    };
    let layout_path = data_dir.join("laststore-layout-v1");
    let layout = fs::read_to_string(&layout_path).map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("layout file is missing: {}", layout_path.display()),
        )
    })?;
    if !layout.lines().any(|line| line == "packaging=plain") {
        eprintln!("refusing: packaging is not plain");
        return Ok(ExitCode::from(2));
    }
    if home_has_frame_aead_segments(&data_dir) {
        eprintln!("refusing: frame segments are present");
        return Ok(ExitCode::from(2));
    }
    let socket = data_dir.join("folddb.sock");
    if UnixStream::connect(&socket).is_ok() {
        eprintln!("refusing: the database socket is open");
        return Ok(ExitCode::from(2));
    }
    let lock_path = data_dir.join("maintenance.lock");
    let lock = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)?;
    if let Err(error) =
        rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive)
    {
        eprintln!("maintenance lock is held: {error}");
        return Ok(ExitCode::from(2));
    }
    let cutoff = cutoff.unwrap_or_else(default_cutoff);
    eprintln!(
        "maintenance data_dir={} execute={execute} cutoff_nanos={cutoff}",
        data_dir.display()
    );
    let store = LastStore::open(&data_dir)?;
    let report = store.maintenance_shrink(cutoff, execute)?;
    print_report(&report, execute, cutoff);
    Ok(ExitCode::SUCCESS)
}

fn default_cutoff() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let now = u64::try_from(now).unwrap_or(u64::MAX);
    now.saturating_sub(ORDER_LOG_RETENTION_NANOS)
}

fn print_report(report: &MaintenanceReport, execute: bool, cutoff: u64) {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "execute={}", if execute { 1 } else { 0 });
    let _ = writeln!(out, "cutoff_nanos={cutoff}");
    let _ = writeln!(out, "live_molecules={}", report.live_molecules);
    let _ = writeln!(out, "mk_keys={}", report.mk_keys);
    let _ = writeln!(out, "keys_seen={}", report.keys_seen);
    let _ = writeln!(out, "keys_dropped={}", report.keys_dropped);
    let _ = writeln!(out, "groups_rewritten={}", report.groups_rewritten);
    let _ = writeln!(out, "tips_bytes_before={}", report.tips_bytes_before);
    let _ = writeln!(out, "tips_bytes_after={}", report.tips_bytes_after);
    let _ = writeln!(out, "helper_bytes_before={}", report.helper_bytes_before);
    let _ = writeln!(out, "helper_bytes_after={}", report.helper_bytes_after);
}

fn print_usage() {
    eprintln!("usage: lastdb-maintenance --data-dir <store-root> [--cutoff-nanos N] [--execute]");
    eprintln!("       lastdb-maintenance reap --help");
}
