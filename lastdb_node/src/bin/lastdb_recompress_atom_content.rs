//! Recompress or binary-pack atom content on an offline Mini home.
//!
//! One invocation processes one bounded keyset page and prints a resume cursor.
//! Never point `--data-dir` at primary `~/.lastdb` unless an operator explicitly
//! passes `--i-know-this-is-primary`; prefer an encrypted CoW copy first.

use std::path::PathBuf;

use clap::Parser;
use fold_db::fold_db_core::factory::create_fold_db;
use fold_db::storage::config::{DatabaseConfig, StorageEngine};
use lastdb_node::offline_home::{load_e2e_keys, refuse_primary};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(name = "lastdb_recompress_atom_content")]
#[command(about = "Recompress one bounded page of legacy ENC: atom content (CoW first)")]
struct Args {
    /// Mini home (must contain identity.key; usually a CoW copy of ~/.lastdb).
    #[arg(long)]
    data_dir: PathBuf,

    /// Exclusive storage-key cursor returned as `next_after_key` by a prior run.
    #[arg(long, conflicts_with = "after_key_hex")]
    after_key: Option<String>,

    /// Hex-encoded resume cursor (required when a partition key contains NUL).
    #[arg(long, conflicts_with = "after_key")]
    after_key_hex: Option<String>,

    /// Maximum atom bodies to inspect in this invocation.
    #[arg(long, default_value_t = 1_000)]
    max_atoms: usize,

    /// Rewrite JSON-string atom content into the binary `ATB:` row container.
    #[arg(long, default_value_t = false)]
    binary: bool,

    /// Allow operating on the primary home (default: refuse; human gate).
    #[arg(long, default_value_t = false)]
    i_know_this_is_primary: bool,
}

#[derive(Serialize)]
struct Report {
    ok: bool,
    data_dir: String,
    scanned: usize,
    recompressed: usize,
    already_compressed: usize,
    already_binary: usize,
    compression_not_helpful: usize,
    not_legacy_enc: usize,
    errors: usize,
    sealed_bytes_before: u64,
    sealed_bytes_after: u64,
    bytes_saved: u64,
    percent_saved: f64,
    more_remaining: bool,
    next_after_key: Option<String>,
    next_after_key_hex: Option<String>,
    notes: Vec<String>,
}

#[derive(Serialize)]
struct BinaryReport {
    ok: bool,
    mode: &'static str,
    data_dir: String,
    scanned: usize,
    repacked: usize,
    already_binary: usize,
    errors: usize,
    inner_bytes_before: u64,
    inner_bytes_after: u64,
    inner_bytes_saved: u64,
    inner_percent_saved: f64,
    row_bytes_before: u64,
    row_bytes_after: u64,
    row_bytes_saved: u64,
    row_percent_saved: f64,
    outer_kv_bytes_saved: u64,
    more_remaining: bool,
    next_after_key: Option<String>,
    next_after_key_hex: Option<String>,
    notes: Vec<String>,
}

fn resolve_after_key(
    plain: Option<String>,
    encoded: Option<String>,
) -> Result<Option<String>, String> {
    match (plain, encoded) {
        (Some(value), None) => Ok(Some(value)),
        (None, Some(value)) => {
            let bytes = hex::decode(value.trim()).map_err(|e| format!("--after-key-hex: {e}"))?;
            String::from_utf8(bytes)
                .map(Some)
                .map_err(|e| format!("--after-key-hex is not UTF-8: {e}"))
        }
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err("pass only one of --after-key / --after-key-hex".into()),
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("lastdb_recompress_atom_content FAILED: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let args = Args::parse();
    if !args.i_know_this_is_primary {
        refuse_primary(&args.data_dir)
            .map_err(|e| format!("{e}; or pass --i-know-this-is-primary"))?;
    }
    if args.max_atoms == 0 {
        return Err("--max-atoms must be at least 1".into());
    }
    let after_key = resolve_after_key(args.after_key, args.after_key_hex)?;

    std::env::remove_var("LASTDB_ATOM_CONTENT_STRICT");
    std::env::set_var("LASTDB_ATOM_CONTENT_DUAL_READ", "1");
    std::env::set_var("LASTDB_ENGINE", "laststore");
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");

    let (e2e, keypair) = load_e2e_keys(&args.data_dir)?;
    let data_path = args.data_dir.join("data");
    if !data_path.is_dir() {
        return Err(format!("missing data dir {}", data_path.display()));
    }
    let db = create_fold_db(
        &DatabaseConfig {
            path: data_path,
            engine: StorageEngine::Laststore,
            cloud_sync: None,
        },
        &e2e,
        keypair,
    )
    .await
    .map_err(|e| format!("create_fold_db: {e}"))?;

    if args.binary {
        let page = db
            .db_ops()
            .atoms()
            .repack_atom_content_binary_page(after_key.as_deref(), args.max_atoms)
            .await
            .map_err(|e| format!("binary atom repack: {e}"))?;
        let inner_percent_saved = percent_saved(page.inner_bytes_before, page.inner_bytes_saved);
        let row_percent_saved = percent_saved(page.row_bytes_before, page.row_bytes_saved);
        let next_after_key_hex = page.next_after_key.as_deref().map(hex::encode);
        let report = BinaryReport {
            ok: page.errors == 0,
            mode: "binary-atom-content",
            data_dir: args.data_dir.display().to_string(),
            scanned: page.scanned,
            repacked: page.repacked,
            already_binary: page.already_binary,
            errors: page.errors,
            inner_bytes_before: page.inner_bytes_before,
            inner_bytes_after: page.inner_bytes_after,
            inner_bytes_saved: page.inner_bytes_saved,
            inner_percent_saved,
            row_bytes_before: page.row_bytes_before,
            row_bytes_after: page.row_bytes_after,
            row_bytes_saved: page.row_bytes_saved,
            row_percent_saved,
            // This pass changes the nested atom container only. The outer KV
            // seam has its own writer switch and measurement.
            outer_kv_bytes_saved: 0,
            more_remaining: page.more_remaining,
            next_after_key: page.next_after_key,
            next_after_key_hex,
            notes: vec![
                "inner_bytes measure the stored atom content value/container".into(),
                "row_bytes measure plaintext bytes presented to the outer KV seam".into(),
                "outer_kv_bytes_saved is zero here; measure LASTDB_KV_AT_REST_RAW separately"
                    .into(),
                "Run on an encrypted CoW copy first; primary execution remains a human gate".into(),
            ],
        };
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
        if !report.ok {
            return Err(format!("{} binary atom repack errors", report.errors));
        }
        return Ok(());
    }

    let page = db
        .db_ops()
        .atoms()
        .recompress_atom_content_page(after_key.as_deref(), args.max_atoms)
        .await
        .map_err(|e| format!("recompress: {e}"))?;
    let percent_saved = if page.sealed_bytes_before == 0 {
        0.0
    } else {
        100.0 * (page.bytes_saved as f64 / page.sealed_bytes_before as f64)
    };
    let next_after_key_hex = page.next_after_key.as_deref().map(hex::encode);
    let report = Report {
        ok: page.errors == 0,
        data_dir: args.data_dir.display().to_string(),
        scanned: page.scanned,
        recompressed: page.recompressed,
        already_compressed: page.already_compressed,
        already_binary: page.already_binary,
        compression_not_helpful: page.compression_not_helpful,
        not_legacy_enc: page.not_legacy_enc,
        errors: page.errors,
        sealed_bytes_before: page.sealed_bytes_before,
        sealed_bytes_after: page.sealed_bytes_after,
        bytes_saved: page.bytes_saved,
        percent_saved,
        more_remaining: page.more_remaining,
        next_after_key: page.next_after_key,
        next_after_key_hex,
        notes: vec![
            "Run on an encrypted CoW copy first; atom rewrites add versions until atoms compaction returns bytes".into(),
            "Primary execution remains a human gate and must wait for the atom keep-set".into(),
            "Resume with --after-key-hex when next_after_key contains an embedded NUL".into(),
        ],
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if !report.ok {
        return Err(format!("{} recompress errors", report.errors));
    }
    Ok(())
}

fn percent_saved(before: u64, saved: u64) -> f64 {
    if before == 0 {
        0.0
    } else {
        100.0 * (saved as f64 / before as f64)
    }
}
