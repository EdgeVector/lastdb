//! Phase 1 Mini cutover smoke: boot FoldDB against an **empty Last Store** home.
//!
//! Proves the factory path `engine=laststore` / `LASTDB_ENGINE=laststore` can:
//! 1. open FoldDB on a throwaway empty home
//! 2. put+get one durable record through the normal encrypting store stack
//! 3. re-open and read the same value
//!
//! **Never** targets primary `~/.lastdb` (or legacy `~/.folddb`). Default home is
//! under `$TMPDIR`. Prefer the wrapper:
//! `bash scripts/lastdbd/laststore-empty-home-smoke.sh`.
//!
//! Usage:
//! ```text
//! lastdb_laststore_empty_home_smoke [home]
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::factory::create_fold_db;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::{DatabaseConfig, StorageEngine};
use lastdb_node::offline_home::refuse_primary;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("laststore empty-home smoke FAILED: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let home = smoke_home()?;
    refuse_primary(&home)?;

    let data_path = home.join("data");
    std::fs::create_dir_all(&data_path)
        .map_err(|e| format!("create data dir {}: {e}", data_path.display()))?;

    // Deterministic throwaway identity — not a real node seed.
    let seed = [0x5a_u8; 32];
    let keypair = Arc::new(
        Ed25519KeyPair::from_secret_key(&seed)
            .map_err(|e| format!("ed25519 keypair from smoke seed: {e}"))?,
    );
    let e2e_keys = E2eKeys::from_ed25519_seed(&seed)
        .map_err(|e| format!("E2E key derivation from smoke seed: {e}"))?;

    let config = DatabaseConfig {
        path: data_path.clone(),
        engine: StorageEngine::Laststore,
        cloud_sync: None,
    };

    // Match factory env override path used by agents/scripts.
    // SAFETY: this binary is a one-shot smoke; it does not share the process
    // with other tests that might race on process-wide env.
    std::env::set_var("LASTDB_ENGINE", "laststore");
    // Skip native-index/embedder init so the smoke stays local and fast.
    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");

    let db = create_fold_db(&config, &e2e_keys, Arc::clone(&keypair))
        .await
        .map_err(|e| format!("create_fold_db(engine=laststore) on empty home: {e}"))?;

    // Durable put+get through FoldDB metadata (encrypting seam → Last Store).
    let node_id = db
        .get_node_id()
        .await
        .map_err(|e| format!("get_node_id (put path): {e}"))?;
    if node_id.trim().is_empty() {
        return Err("get_node_id returned empty id".to_string());
    }

    // Process-boundary re-open: drop FoldDB, open again, read same id.
    drop(db);

    let db2 = create_fold_db(&config, &e2e_keys, Arc::clone(&keypair))
        .await
        .map_err(|e| format!("re-open create_fold_db(engine=laststore): {e}"))?;
    let node_id2 = db2
        .get_node_id()
        .await
        .map_err(|e| format!("get_node_id after re-open: {e}"))?;
    if node_id2 != node_id {
        return Err(format!(
            "node_id mismatch after re-open: first={node_id} second={node_id2}"
        ));
    }

    println!(
        "laststore empty-home smoke OK home={} data={} node_id={node_id}",
        home.display(),
        data_path.display()
    );
    Ok(())
}

fn smoke_home() -> Result<PathBuf, String> {
    let mut args = std::env::args_os();
    let _bin = args.next();
    if let Some(path) = args.next() {
        if args.next().is_some() {
            return Err("usage: lastdb_laststore_empty_home_smoke [home]".to_string());
        }
        return Ok(PathBuf::from(path));
    }

    let ts = fold_db::clock::unix_nanos_wide();
    Ok(std::env::temp_dir().join(format!(
        "lastdb-laststore-empty-home-smoke-{}-{ts}",
        std::process::id()
    )))
}
