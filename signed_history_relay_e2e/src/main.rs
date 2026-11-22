//! Live-relay bundle bootstrap and restore proof (`LASTDB_RELAY_E2E_BIN`).
//!
//! Against the relay at `RELAY_URL` it uploads ~300 MiB of synthetic records as
//! signed, encrypted bundles, restores them with stream discovery into a fresh
//! FoldDB, proves grant/revoke and never-granted 403s, and prints one JSON
//! object as the last stdout line. Progress goes to stderr. Exit 0 only when
//! both phases finish inside the time limit and nongrantee calls return 403.
//!
//! Two further steps are opt-in, so a relay that lacks them keeps passing:
//! `LASTDB_RELAY_E2E_STREAMS=true` proves `log.streams` and
//! `LASTDB_RELAY_E2E_BLOB_FILE_BYTES=<n>` proves a large file through the blob
//! routes.

mod blob;
mod blob_file;
mod blob_stream;
mod chain;
mod grants;
mod identity;
mod materialize;
mod relay;
mod restore;
mod streams;
mod upload;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use clap::Parser;
use serde_json::{json, Value};

use blob::VALUE_TEXT_BYTES;
use identity::{new_device_id, Actor};
use relay::RelayClient;
use upload::UploadPlan;

#[derive(Parser, Debug)]
#[command(name = "signed_history_relay_e2e")]
struct Cli {
    /// Relay origin (live DEV or local), set by the proof script.
    #[arg(long, env = "RELAY_URL")]
    relay_url: Option<String>,
    /// Total synthetic record text to upload (default 300 MiB).
    #[arg(long, env = "LASTDB_RELAY_E2E_TOTAL_BYTES", default_value_t = 300 * 1024 * 1024)]
    total_bytes: u64,
    /// Blobs transferred at once, 1..=8.
    #[arg(long, env = "LASTDB_RELAY_E2E_CONCURRENCY", default_value_t = 4)]
    concurrency: usize,
    /// Each phase must finish under this many seconds.
    #[arg(long, env = "LASTDB_RELAY_E2E_LIMIT_SECONDS", default_value_t = 60.0)]
    limit_seconds: f64,
    /// Also prove `log.streams` (steps `streams_*`). Off by default: a relay that
    /// does not implement the route yet would fail it.
    #[arg(long, env = "LASTDB_RELAY_E2E_STREAMS", action = clap::ArgAction::SetTrue)]
    streams: bool,
    /// Also put and get one generated file of this many bytes through the blob
    /// routes, from and to disk (step `blob_file_stream`). 0 skips it.
    #[arg(long, env = "LASTDB_RELAY_E2E_BLOB_FILE_BYTES", default_value_t = 0)]
    blob_file_bytes: u64,
    /// Scratch directory for the restored FoldDB (never `~/.lastdb`).
    #[arg(long, env = "LASTDB_RELAY_E2E_WORK_DIR")]
    work_dir: Option<PathBuf>,
}

struct Outcome {
    ok: bool,
    report: Value,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli).await {
        Ok(outcome) => {
            println!("{}", outcome.report);
            if outcome.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(error) => {
            eprintln!("signed_history_relay_e2e: {error}");
            println!("{}", json!({ "ok": false, "error": error }));
            ExitCode::from(1)
        }
    }
}

async fn run(cli: &Cli) -> Result<Outcome, String> {
    let relay_url = cli
        .relay_url
        .as_deref()
        .filter(|url| !url.is_empty())
        .ok_or("RELAY_URL is not set")?;
    let work = cli.work_dir.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("lastdb-relay-e2e-{}", std::process::id()))
    });
    if work.starts_with(home_lastdb()) {
        return Err("refusing a work dir under ~/.lastdb".into());
    }
    std::fs::create_dir_all(&work).map_err(|e| format!("work dir: {e}"))?;
    let outcome = prove(cli, relay_url, &work).await;
    let _ = std::fs::remove_dir_all(&work);
    outcome
}

async fn prove(cli: &Cli, relay_url: &str, work: &Path) -> Result<Outcome, String> {
    let http = relay::http_client()?;
    let concurrency = cli.concurrency.clamp(1, 8);
    let reader = Actor::generate();
    let up = upload::bootstrap(
        &http,
        &UploadPlan {
            relay_url: relay_url.to_owned(),
            total_bytes: cli.total_bytes,
            concurrency,
            reader: reader.node_key,
        },
    )
    .await?;
    eprintln!(
        "uploaded {} bundles, {} blob bytes in {:.3}s",
        up.bundles, up.blob_bytes, up.seconds
    );

    let started = Instant::now();
    let client = RelayClient::new(
        relay_url,
        &http,
        &reader,
        up.invite.database_id,
        new_device_id(),
    );
    let restored = restore::restore(&client, &up.invite, concurrency).await?;
    let history_seconds = started.elapsed().as_secs_f64();
    let materialized = materialize::materialize(&work.join("store"), &restored.history).await?;
    let restore_seconds = started.elapsed().as_secs_f64();
    eprintln!(
        "restored {} streams, {} bundles, {} records in {restore_seconds:.3}s",
        restored.streams, restored.bundles, materialized.records
    );

    let expect = materialize::Expect {
        bulk: up.records,
        member: up.member,
        reader: reader.node_key,
    };
    materialize::verify(&materialized.store, &restored.history, &expect).await?;
    materialized
        .store
        .shutdown()
        .await
        .map_err(|e| e.to_string())?;
    if restored.bundles != up.bundles || materialized.records != up.records + 1 {
        return Err(format!(
            "restore saw {} bundles and {} records, upload wrote {} and {} plus the member's",
            restored.bundles, materialized.records, up.bundles, up.records
        ));
    }

    let (streams_steps, blob_file) = opt_in_steps(cli, &http, relay_url, work).await?;

    let limit = cli.limit_seconds;
    let nongrantee_ok = up.nongrantee_http_status == 403;
    let ok = up.seconds < limit && restore_seconds < limit && nongrantee_ok;
    let error = if !nongrantee_ok {
        Some(format!(
            "nongrantee_http_status={} (expected 403)",
            up.nongrantee_http_status
        ))
    } else if !ok {
        Some(format!("a phase took {limit}s or longer"))
    } else {
        None
    };
    Ok(Outcome {
        ok,
        report: json!({
            "ok": ok,
            "error": error,
            "upload_seconds": up.seconds,
            "restore_seconds": restore_seconds,
            "limit_seconds": limit,
            "bytes_uploaded": up.blob_bytes,
            "record_text_bytes": up.records * VALUE_TEXT_BYTES,
            "records": up.records,
            "member_records": 1,
            "bundles": up.bundles,
            "roles": ["read", "write", "revoked"],
            "grant_revoke_ok": true,
            "revoked_http_status": 403,
            "nongrantee_http_status": up.nongrantee_http_status,
            "streams_discovered": restored.streams,
            "history_seconds": history_seconds,
            "materialize_seconds": restore_seconds - history_seconds,
            "generate_encrypt_cpu_seconds": up.cpu_seconds,
            "database_id": up.invite.database_id.to_hex(),
            "streams_steps": streams_steps,
            "blob_file_stream": blob_file,
        }),
    })
}

/// The opt-in steps: the step names of `log.streams`, and the large-file report.
async fn opt_in_steps(
    cli: &Cli,
    http: &reqwest::Client,
    relay_url: &str,
    work: &Path,
) -> Result<(Option<Vec<&'static str>>, Option<Value>), String> {
    let streams_steps = if cli.streams {
        Some(streams::prove(http, relay_url).await?)
    } else {
        None
    };
    let blob_file = if cli.blob_file_bytes > 0 {
        let r = blob_stream::prove(http, relay_url, work, cli.blob_file_bytes).await?;
        Some(json!({
            "bytes": r.bytes,
            "sha256": r.sha256,
            "put_seconds": r.put_seconds,
            "get_seconds": r.get_seconds,
        }))
    } else {
        None
    };
    Ok((streams_steps, blob_file))
}

fn home_lastdb() -> PathBuf {
    let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("/"), PathBuf::from);
    home.join(".lastdb")
}
