//! DEV CI proofs for Phase 1 signed history against grant-blind `reference_relay`.
//!
//! Exit 0 only when every scenario prints PASS. No AWS/IAM/secrets.

mod device;
mod proof_store;
mod relay;
mod report;
mod scenarios;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use relay::RelayHandle;
use report::{write_json, Summary};

#[derive(Parser, Debug)]
#[command(name = "signed_history_dev")]
struct Cli {
    /// Path to the `reference_relay` binary (spawned unless `--relay-url` / `RELAY_URL`).
    #[arg(
        long,
        env = "RELAY_BIN",
        default_value = "target/debug/reference_relay"
    )]
    relay_bin: PathBuf,
    /// Use an already-running relay instead of spawning.
    #[arg(long, env = "RELAY_URL")]
    relay_url: Option<String>,
    /// Work directory for spawned relay data (never `~/.lastdb`).
    #[arg(long, env = "SIGNED_HISTORY_WORK_DIR")]
    work_dir: Option<PathBuf>,
    /// Write a JSON summary to this path.
    #[arg(long)]
    json_out: Option<PathBuf>,
    /// Blob size for `blob_perf` (default ~300 MiB).
    #[arg(long, env = "BLOB_BYTES", default_value_t = 300 * 1024 * 1024)]
    blob_bytes: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(err) => {
            eprintln!("signed_history_dev: {err}");
            ExitCode::from(2)
        }
    }
}

async fn run() -> Result<bool, String> {
    let cli = Cli::parse();
    let work = cli.work_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("signed-history-dev-{}", std::process::id()))
    });
    if work.starts_with(shellexpand_home_lastdb()) {
        return Err("refusing work-dir under ~/.lastdb".into());
    }
    let relay = if let Some(url) = cli.relay_url.as_deref() {
        RelayHandle::connect(url)
    } else {
        let data_dir = work.join("relay-data");
        RelayHandle::spawn(&cli.relay_bin, data_dir)?
    };
    println!("relay_url {}", relay.base_url);
    println!("blob_bytes {}", cli.blob_bytes);

    let results = scenarios::run_all(&relay, cli.blob_bytes, &work).await;
    let all_pass = results.iter().all(|r| r.pass);
    let summary = Summary {
        all_pass,
        scenarios: results,
    };
    if let Some(path) = cli.json_out.as_ref() {
        write_json(path, &summary)?;
        println!("json_out {}", path.display());
    }
    println!("SUMMARY {}", if all_pass { "PASS" } else { "FAIL" });
    Ok(all_pass)
}

fn shellexpand_home_lastdb() -> PathBuf {
    let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("/"), PathBuf::from);
    home.join(".lastdb")
}
