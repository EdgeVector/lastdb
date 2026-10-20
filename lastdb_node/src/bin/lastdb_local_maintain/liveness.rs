//! Part of `lastdb_local_maintain`: rebuild the molecule and blob liveness
//! planes on a STOPPED home.
//!
//! The online route (`lastdb liveness bootstrap --isolated-copy`) needs a
//! daemon that serves a copy of the data, and it deletes every derived edge
//! before it rewrites them, so it cannot run next to live writers. This
//! command runs the same bootstrap in-process on the home itself, after the
//! daemon is stopped. It needs no copy of the home.
//!
//! The home is protected three ways before anything is opened:
//!
//! 1. No daemon is alive for the home, and neither daemon socket accepts a
//!    connection.
//! 2. The last session ended cleanly and left a valid shutdown flush receipt,
//!    so the previous daemon flushed everything before it stopped.
//! 3. This process holds an exclusive `flock` on `data/maintenance.lock`. The
//!    offline tools use that lock to exclude each other. A running daemon does
//!    not take it, so steps 1 and 2 are what exclude the daemon.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use fold_db::storage::config::{DatabaseConfig, StorageEngine};
use lastdb_node::offline_home::{load_e2e_keys, preflight_stopped_home, refuse_primary, Preflight};
use serde::Serialize;

/// Environment the primary daemon runs with that changes how a home is read or
/// written. The report lists the value this process saw for each name, so the
/// operator can compare it with the daemon's launch environment.
const REPORTED_ENV: [&str; 6] = [
    "LASTDB_ATOM_KEY_ENCODING",
    "LASTDB_AT_REST_RAW",
    "LASTDB_KV_AT_REST_RAW",
    "LASTDB_KV_AT_REST_COMPRESS",
    "LASTDB_MAX_ATOM_CONTENT_BYTES",
    "LASTDB_HASH_KEY_ENCODING",
];

pub(crate) struct LivenessBootstrapArgs<'a> {
    pub(crate) home: &'a Path,
    pub(crate) execute: bool,
    pub(crate) i_know_this_is_primary: bool,
    pub(crate) json: bool,
}

#[derive(Debug, Serialize)]
struct LivenessBootstrapOfflineReport {
    ok: bool,
    mode: &'static str,
    executed: bool,
    home: String,
    store_root: String,
    build: &'static str,
    preflight: Preflight,
    env: BTreeMap<&'static str, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bootstrap: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    molecule_complete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_complete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_secs: Option<u64>,
    notes: Vec<String>,
}

pub(crate) fn bootstrap_liveness(args: &LivenessBootstrapArgs<'_>) -> Result<(), String> {
    if !args.i_know_this_is_primary {
        refuse_primary(args.home).map_err(|e| format!("{e}; or pass --i-know-this-is-primary"))?;
    }
    let store_root = args.home.join("data");
    if !store_root.is_dir() {
        return Err(format!("missing data dir {}", store_root.display()));
    }
    if !store_root.join("laststore-pack-node-v1").is_file() {
        return Err("liveness bootstrap requires a native LastDB node home".into());
    }
    let (preflight, _lock) = preflight_stopped_home(args.home, &store_root)?;
    let mut report = LivenessBootstrapOfflineReport {
        ok: true,
        mode: "liveness-bootstrap-offline",
        executed: false,
        home: args.home.display().to_string(),
        store_root: store_root.display().to_string(),
        build: env!("CARGO_PKG_VERSION"),
        preflight,
        env: reported_env(),
        bootstrap: None,
        molecule_complete: None,
        blob_complete: None,
        elapsed_secs: None,
        notes: Vec::new(),
    };
    if !args.execute {
        report.notes.push(
            "Dry run: the home is stopped and locked, and nothing was opened. \
             Pass --execute to rebuild the molecule and blob liveness planes."
                .to_string(),
        );
        return print_report(&report, args.json);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Unoptimized builds keep the factory's futures on the stack.
        .thread_stack_size(16 * 1024 * 1024)
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let started = Instant::now();
    let outcome = runtime.block_on(run_bootstrap(args.home, store_root));
    report.executed = true;
    report.elapsed_secs = Some(started.elapsed().as_secs());
    match outcome {
        Ok(done) => {
            report.molecule_complete = Some(done.molecule_complete);
            report.blob_complete = Some(done.blob_complete);
            report.bootstrap = Some(done.report);
            report.ok = done.molecule_complete && done.blob_complete;
            report.notes.push(
                "Start the daemon again, then check `lastdb status`: the Liveness edges line \
                 must show molecule_complete=true and blob_complete=true."
                    .to_string(),
            );
        }
        Err(error) => {
            report.ok = false;
            report.notes.push(format!("bootstrap failed: {error}"));
            print_report(&report, args.json)?;
            return Err(error);
        }
    }
    print_report(&report, args.json)?;
    if report.ok {
        Ok(())
    } else {
        Err("a completeness marker is still missing after the bootstrap".to_string())
    }
}

fn reported_env() -> BTreeMap<&'static str, String> {
    REPORTED_ENV
        .iter()
        .map(|name| {
            let value = std::env::var(name).unwrap_or_else(|_| "(unset)".to_string());
            (*name, value)
        })
        .collect()
}

struct BootstrapDone {
    report: serde_json::Value,
    molecule_complete: bool,
    blob_complete: bool,
}

/// Open the home the way the sibling offline tools do (no cloud sync), run the
/// bootstrap, then finish every local flush barrier even when the bootstrap fails.
async fn run_bootstrap(home: &Path, store_root: PathBuf) -> Result<BootstrapDone, String> {
    let (e2e, keypair) = load_e2e_keys(home)?;
    let db = fold_db::fold_db_core::factory::create_fold_db(
        &DatabaseConfig {
            path: store_root,
            engine: StorageEngine::Laststore,
            cloud_sync: None,
        },
        &e2e,
        keypair,
    )
    .await
    .map_err(|e| format!("create_fold_db: {e}"))?;

    let outcome = bootstrap_and_verify(&db).await;
    let shutdown = db
        .shutdown_local_only()
        .await
        .map_err(|e| format!("local shutdown after the bootstrap: {e}"));
    match (outcome, shutdown) {
        (Ok(done), Ok(())) => Ok(done),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(shutdown_error)) => Err(shutdown_error),
        (Err(error), Err(shutdown_error)) => Err(format!("{error}; {shutdown_error}")),
    }
}

async fn bootstrap_and_verify(db: &fold_db::fold_db_core::FoldDB) -> Result<BootstrapDone, String> {
    let ops = db.db_ops();
    let report = ops
        .bootstrap_liveness_edges_on_isolated_copy(None)
        .await
        .map_err(|e| format!("liveness bootstrap: {e}"))?;
    let report = serde_json::to_value(&report).map_err(|e| format!("serialize report: {e}"))?;
    let molecule_complete = ops
        .atoms()
        .molecule_ref_edges_complete(None)
        .await
        .map_err(|e| format!("read molecule completeness: {e}"))?;
    let blob_complete = ops
        .atoms()
        .blob_ref_edges_complete(None)
        .await
        .map_err(|e| format!("read blob completeness: {e}"))?;
    Ok(BootstrapDone {
        report,
        molecule_complete,
        blob_complete,
    })
}

fn print_report(report: &LivenessBootstrapOfflineReport, json: bool) -> Result<(), String> {
    if json {
        let text = serde_json::to_string_pretty(report).map_err(|e| format!("serialize: {e}"))?;
        println!("{text}");
        return Ok(());
    }
    println!("liveness-bootstrap-offline");
    println!("  home: {}", report.home);
    println!("  executed: {}", report.executed);
    println!(
        "  preflight: daemon_alive={} sockets_closed={} last_session_clean={} \
         shutdown_flush_receipt={} maintenance_lock={}",
        report.preflight.daemon_alive,
        report.preflight.sockets_closed,
        report.preflight.last_session_clean,
        report.preflight.shutdown_flush_receipt,
        report.preflight.maintenance_lock,
    );
    for (name, value) in &report.env {
        println!("  env {name}={value}");
    }
    if let Some(done) = &report.bootstrap {
        let field = |pointer: &str| {
            done.pointer(pointer)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        println!(
            "  schema field sources: {}",
            field("/molecule_refs/schema_fields")
        );
        println!(
            "  protein member sources: {}",
            field("/molecule_refs/protein_members")
        );
        println!("  atoms read: {}", field("/blob_refs/atoms_read"));
        println!(
            "  blob edges written: {}",
            field("/blob_refs/edges_written")
        );
        println!(
            "  molecules measured: {}",
            field("/schema_counters/molecules")
        );
    }
    if let (Some(molecule), Some(blob)) = (report.molecule_complete, report.blob_complete) {
        println!("  molecule_complete={molecule} blob_complete={blob}");
    }
    if let Some(secs) = report.elapsed_secs {
        println!("  elapsed_secs: {secs}");
    }
    for note in &report.notes {
        println!("  note: {note}");
    }
    Ok(())
}
