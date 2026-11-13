//! One sync turn: pull, apply, then ack. Local capture is the native commit.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tracing::Instrument;

use crate::host::Host;

use super::cloud::CloudLog;
use super::disk;
use super::members;
use super::native;
use super::received;
use super::registry::{self, Database};
use super::run;

const GRANTS_LIMIT: usize = 65_536;

static AUTHORITY_WARNED: AtomicBool = AtomicBool::new(false);

pub(super) enum Pause {
    Idle,
    Retry,
    Snapshot,
}

enum Prep {
    Pause(Pause),
    Ready(Box<run::Ready>),
}

pub fn spawn(host: Arc<Host>, shutdown: Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            while !shutdown.load(Ordering::Acquire) {
                let pause = one_turn(&host).await;
                sleep_interruptible(&host, &shutdown, pause).await;
            }
        }
        .in_current_span(),
    )
}

async fn one_turn(host: &Arc<Host>) -> Pause {
    let databases = registry::databases(&host.home);
    let mut pause = if databases.is_empty() {
        Pause::Idle
    } else {
        Pause::Snapshot
    };
    for database in &databases {
        pause = shorter(pause, native::turn(host, database).await);
    }
    shorter(pause, received::turn(host).await)
}

pub(super) async fn open_native(
    host: &Arc<Host>,
    database: &Database,
    log: CloudLog,
    device: &str,
) -> Pause {
    if database.kind == "shareable" && !native_publisher(host, &database.id) {
        warn_once(
            &AUTHORITY_WARNED,
            "named database authority is not this node and sync stays closed",
        );
        return Pause::Idle;
    }
    match finish_open(host, &database.id, &database.kind, log, device).await {
        Prep::Pause(pause) => pause,
        Prep::Ready(mut ready) => {
            if super::snapshot::pending(&host.home, &database.id) {
                if let Err(reason) = super::snapshot::requested(host, &ready.log, database).await {
                    tracing::warn!(target: "lastdbd::log_sync", %reason, "snapshot publication awaits retry");
                    return shorter(Pause::Snapshot, run::pull_and_ack(host, &mut ready).await);
                }
            }
            run::run_ready(host.as_ref(), *ready).await
        }
    }
}

async fn finish_open(
    host: &Arc<Host>,
    database_id: &str,
    kind: &str,
    mut log: CloudLog,
    device: &str,
) -> Prep {
    if kind != "personal" {
        let Some(revision) = members::revision(&log).await else {
            return Prep::Pause(Pause::Retry);
        };
        log.member_revision = revision;
    }
    let state = match run::opened_state(database_id, kind, &log, device, &host.home) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(target: "lastdbd::log_sync", error = %error, "log sync state was refused");
            return Prep::Pause(Pause::Retry);
        }
    };
    Prep::Ready(Box::new(run::Ready {
        database_id: database_id.to_owned(),
        publisher: None,
        log,
        state,
    }))
}

/// Grant revision for the confirmation read. Personal databases use zero.
/// A missing or unreadable grants file uses zero.
pub(super) fn probe_revision(home: &Path, database_id: &str, kind: &str) -> u64 {
    if kind == "personal" {
        return 0;
    }
    let path = home.join("databases").join(database_id).join("grants.json");
    if let Ok(Some(bytes)) = disk::read_limited(&path, GRANTS_LIMIT) {
        if let Ok(grants) = serde_json::from_slice::<GrantsFile>(&bytes) {
            if grants.revision != 0 {
                return grants.revision;
            }
        }
    }
    0
}

pub(super) fn shorter(current: Pause, next: Pause) -> Pause {
    if pause_rank(&next) < pause_rank(&current) {
        next
    } else {
        current
    }
}

fn pause_rank(pause: &Pause) -> u8 {
    match pause {
        Pause::Idle => 0,
        Pause::Retry => 1,
        Pause::Snapshot => 2,
    }
}

pub(super) fn warn_once(flag: &AtomicBool, message: &str) {
    if !flag.swap(true, Ordering::Relaxed) {
        tracing::warn!(target: "lastdbd::log_sync", "{message}");
    }
}

async fn sleep_interruptible(host: &Host, shutdown: &AtomicBool, pause: Pause) {
    let seconds = match pause {
        Pause::Idle => 1,
        Pause::Retry => 5,
        Pause::Snapshot => 30,
    };
    for _ in 0..seconds {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        if snapshot_waiting(host) {
            return;
        }
    }
}

fn snapshot_waiting(host: &Host) -> bool {
    registry::databases(&host.home)
        .iter()
        .any(|database| super::snapshot::pending(&host.home, &database.id))
}

pub(super) fn native_publisher(host: &Host, database_id: &str) -> bool {
    match grants_authority(&host.home, database_id) {
        Some(publisher) => publisher == hex::encode(host.keypair.public_key_bytes()),
        None => true,
    }
}

fn grants_authority(home: &Path, database_id: &str) -> Option<String> {
    let path = home.join("databases").join(database_id).join("grants.json");
    let bytes = disk::read_limited(&path, GRANTS_LIMIT).ok().flatten()?;
    let grants = serde_json::from_slice::<GrantsFile>(&bytes).ok()?;
    let key = grants.authority_node_key?;
    (key.len() == 64
        && key
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    .then_some(key)
}

#[derive(Deserialize)]
struct GrantsFile {
    #[serde(default)]
    authority_node_key: Option<String>,
    revision: u64,
}
