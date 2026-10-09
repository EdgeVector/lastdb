//! Operation Trinity (Son): re-seal plain atom `content` under the account E2E
//! content key so `LASTDB_ATOM_CONTENT_STRICT=1` can fail closed.
//!
//! **Never** point `--data-dir` at primary `~/.lastdb` unless you pass
//! `--i-know-this-is-primary` (prefer CoW / offline homes first).

use std::path::PathBuf;

use clap::Parser;
use fold_db::fold_db_core::factory::create_fold_db;
use fold_db::storage::config::{DatabaseConfig, StorageEngine};
use lastdb_node::offline_home::{load_e2e_keys, refuse_primary};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "lastdb_reseal_atom_content")]
#[command(about = "Re-seal plain atom content for Operation Trinity STRICT open")]
struct Args {
    /// Mini home (must contain identity.key; usually a CoW copy of ~/.lastdb).
    #[arg(long)]
    data_dir: PathBuf,

    /// Allow operating on primary homes (default: refuse).
    #[arg(long, default_value_t = false)]
    i_know_this_is_primary: bool,
}

#[derive(Serialize)]
struct Report {
    ok: bool,
    data_dir: String,
    scanned: usize,
    resealed: usize,
    already_sealed: usize,
    errors: usize,
    notes: Vec<String>,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("lastdb_reseal_atom_content FAILED: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args = Args::parse();
    if !args.i_know_this_is_primary {
        refuse_primary(&args.data_dir)
            .map_err(|e| format!("{e}; or pass --i-know-this-is-primary"))?;
    }

    // Dual-read so plain content remains readable while we seal it.
    std::env::remove_var("LASTDB_ATOM_CONTENT_STRICT");
    std::env::set_var("LASTDB_ATOM_CONTENT_DUAL_READ", "1");
    std::env::set_var("LASTDB_ENGINE", "laststore");
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");

    let (e2e, keypair) = load_e2e_keys(&args.data_dir)?;
    let data_path = args.data_dir.join("data");
    if !data_path.is_dir() {
        return Err(format!("missing data dir {}", data_path.display()));
    }
    let config = DatabaseConfig {
        path: data_path,
        engine: StorageEngine::Laststore,
        cloud_sync: None,
    };

    let db = create_fold_db(&config, &e2e, keypair)
        .await
        .map_err(|e| format!("create_fold_db: {e}"))?;

    let stats = db
        .db_ops()
        .atoms()
        .reseal_plain_atom_content()
        .await
        .map_err(|e| format!("reseal: {e}"))?;

    let report = Report {
        ok: stats.errors == 0,
        data_dir: args.data_dir.display().to_string(),
        scanned: stats.scanned,
        resealed: stats.resealed,
        already_sealed: stats.already_sealed,
        errors: stats.errors,
        notes: vec![
            "After GREEN: boot with LASTDB_ATOM_CONTENT_STRICT=1 for fail-closed open".into(),
        ],
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if !report.ok {
        return Err(format!("{} reseal errors", stats.errors));
    }
    Ok(())
}
