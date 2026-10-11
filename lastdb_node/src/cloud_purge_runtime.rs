//! Owner job acceptance is durable before the slow source or cloud work starts.

use crate::Host;
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub action: String,
    pub operation_id: Option<String>,
    pub account: Option<CloudAccount>,
    pub proof_receipt: Option<PathBuf>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    format_version: u32,
    operation_id: String,
    account: CloudAccount,
    phase: String,
    error: Option<String>,
    #[serde(default)]
    local_cut_started: bool,
}

struct Runtime {
    host: Weak<Host>,
    stop: Arc<AtomicBool>,
    running: Option<String>,
    cancel: Arc<AtomicBool>,
    fence: Option<(String, Arc<fold_db::sync::engine::CloudPurgeWriteFence>)>,
    engine: Option<Arc<fold_db::sync::SyncEngine>>,
    pause_operation: Option<String>,
}

fn runtimes() -> &'static Mutex<HashMap<PathBuf, Runtime>> {
    static RUNTIMES: OnceLock<Mutex<HashMap<PathBuf, Runtime>>> = OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(HashMap::new()))
}

mod actions;
mod state;
mod storage;
mod worker;

pub(crate) use actions::handle;
use actions::selected_intent;
pub(crate) use state::{
    cache_engine, cached_engine, has_write_fence, local_writes_paused, write_fence,
};
pub use state::{drain, release_for_shutdown};
use state::{ensure_fence, is_running, release_fence, spawn};
pub(crate) use storage::{atomic, read};

pub(crate) async fn peer_engine(host: &Host) -> Result<Arc<fold_db::sync::SyncEngine>, String> {
    crate::cloud_purge_job::source_engine(host).await
}

pub async fn attach(host: &Arc<Host>, stop: Arc<AtomicBool>) -> Result<(), String> {
    let engine = host.db.sync_engine();
    if let Some(engine) = &engine {
        crate::cloud_purge_adoption::install(host, engine, Arc::clone(&stop))
            .map_err(|error| error.to_string())?;
    }
    runtimes()
        .lock()
        .map_err(|_| "cloud purge runtime lock failed")?
        .insert(
            host.home.clone(),
            Runtime {
                host: Arc::downgrade(host),
                stop,
                running: None,
                cancel: Arc::new(AtomicBool::new(false)),
                fence: None,
                engine,
                pause_operation: None,
            },
        );
    let active = host.home.join("cloud-purge-resync/active.json");
    if active.exists() {
        let intent: Intent = read(&active)?;
        if !matches!(intent.phase.as_str(), "ready" | "cancelled") && intent.local_cut_started {
            ensure_fence(host, &intent.operation_id).await?;
        }
        if !matches!(intent.phase.as_str(), "ready" | "cancelled" | "failed") {
            spawn(host, &intent.operation_id)?;
        }
    }
    Ok(())
}
