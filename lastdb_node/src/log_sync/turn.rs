//! One sync turn: pull, apply, then ack. Local capture is the native commit.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tracing::Instrument;

use crate::host::Host;

use super::automatic_retention::Pacing;
use super::cloud::CloudLog;
use super::disk;
use super::members;
use super::native;
use super::received;
use super::registry::{self, Database};
use super::run;

const GRANTS_LIMIT: usize = 65_536;
const SHUTDOWN_CHECK: Duration = Duration::from_millis(250);
const FALLBACK_MARKER_CHECK: Duration = Duration::from_secs(30);

static AUTHORITY_WARNED: AtomicBool = AtomicBool::new(false);
static UNARMED_REOPEN_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pause {
    Reopen,
    Idle,
    Retry,
    Snapshot,
    Backoff,
    PaymentHold,
}

enum Prep {
    Pause(Pause),
    Ready(Box<run::Ready>),
}

pub fn spawn(
    host: Arc<Host>,
    shutdown: Arc<AtomicBool>,
    reopen: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut cleanup = Pacing::default();
            while !shutdown.load(Ordering::Acquire) {
                let pause = one_turn(&host, &mut cleanup).await;
                if matches!(pause, Pause::Reopen) {
                    reopen.store(true, Ordering::Release);
                    shutdown.store(true, Ordering::Release);
                    break;
                }
                sleep_interruptible(&host, &shutdown, pause).await;
            }
        }
        .in_current_span(),
    )
}

async fn one_turn(host: &Arc<Host>, cleanup: &mut Pacing) -> Pause {
    if super::signals::consume_wake(&host.home) {
        native::clear_payment_hold();
        host.cloud_sync.clear_payment_hold();
    }
    let captures = host.db.db_ops().native_runtime().captures_accepted();
    let creds_present = crate::host::has_cloud_credentials(&host.home);
    host.cloud_sync.refresh(&host.home, captures);
    if creds_present && !captures {
        if !host.booted_with_credentials {
            tracing::info!(
                target: "lastdbd::log_sync",
                "cloud credentials enrolled on running node; reopening to arm local capture"
            );
            return Pause::Reopen;
        }
        warn_once(
            &UNARMED_REOPEN_WARNED,
            "cloud credentials were present at boot without local capture; skipping automatic reopen",
        );
        host.cloud_sync
            .record_error("local capture disabled until the enrolled node restarts");
        return Pause::Backoff;
    }
    let databases = registry::databases(&host.home);
    let has_received = crate::registered_share::list(&host.home)
        .map_or(true, |shares| shares.iter().any(|share| share.signed_log));
    if !creds_present {
        for database in &databases {
            super::snapshot::fail_pending(
                &host.home,
                &database.id,
                "no saved cloud credentials — run 'lastdb backup connect' first",
            );
        }
        return if has_received {
            received::turn(host).await
        } else {
            Pause::Retry
        };
    }
    if databases.is_empty() && !has_received {
        return Pause::Idle;
    }
    let mut pause = Pause::PaymentHold;
    for database in &databases {
        let next = native::turn(host, database, cleanup).await;
        if matches!(next, Pause::Reopen) {
            return next;
        }
        pause = shorter(pause, next);
    }
    if has_received {
        pause = shorter(pause, received::turn(host).await);
    }
    pause
}

pub(super) async fn open_native(
    host: &Arc<Host>,
    database: &Database,
    log: CloudLog,
    device: &str,
    cleanup: &mut Pacing,
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
        Prep::Ready(ready) => {
            let snapshot = if super::snapshot::pending(&host.home, &database.id) {
                super::snapshot::requested(host, &ready.log, database).await
            } else {
                super::snapshot::ensure_initial(host, &ready.log, database).await
            };
            if let Err(reason) = snapshot {
                if reason == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
                    host.cloud_sync.record_payment_hold();
                    super::snapshot::fail_pending(&host.home, &database.id, &reason);
                    return native::hold_payment(&database.id);
                }
                if reason != super::snapshot::AWAIT_LOCAL_UPLOAD {
                    host.cloud_sync.record_error(reason.clone());
                    tracing::warn!(target: "lastdbd::log_sync", %reason, "snapshot publication awaits retry");
                }
                if reason == super::snapshot::AWAIT_INCOMING {
                    return shorter(Pause::Snapshot, run::run_ready(host, *ready).await);
                }
            }
            let log_after = ready.log.clone();
            let next = run::run_ready(host, *ready).await;
            if super::accepted::take_payment_hold(&database.id) {
                host.cloud_sync.record_payment_hold();
                super::snapshot::fail_pending(
                    &host.home,
                    &database.id,
                    crate::cloud::PAYMENT_REQUIRED_MESSAGE,
                );
                return native::hold_payment(&database.id);
            }
            if let Some(hold) = service_pending_snapshot(host, &log_after, database).await {
                return hold;
            }
            if matches!(next, Pause::Idle) {
                native::clear_hold(&database.id);
                host.cloud_sync.record_healthy();
                if database.kind == "personal" && !database.native {
                    cleanup.turn(&host.home, &log_after).await;
                }
            }
            next
        }
    }
}

async fn service_pending_snapshot(
    host: &Arc<Host>,
    log: &CloudLog,
    database: &Database,
) -> Option<Pause> {
    if !super::snapshot::pending(&host.home, &database.id) {
        return None;
    }
    if let Err(reason) = super::snapshot::requested(host, log, database).await {
        if reason == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
            host.cloud_sync.record_payment_hold();
            super::snapshot::fail_pending(&host.home, &database.id, &reason);
            return Some(native::hold_payment(&database.id));
        }
        if reason != super::snapshot::AWAIT_LOCAL_UPLOAD {
            host.cloud_sync.record_error(reason.clone());
            tracing::warn!(target: "lastdbd::log_sync", %reason, "snapshot publication awaits retry");
        }
    }
    None
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
        Pause::Reopen => 0,
        Pause::Idle => 1,
        Pause::Retry => 2,
        Pause::Snapshot => 3,
        Pause::Backoff => 4,
        Pause::PaymentHold => 5,
    }
}

pub(super) fn warn_once(flag: &AtomicBool, message: &str) {
    if !flag.swap(true, Ordering::Relaxed) {
        tracing::warn!(target: "lastdbd::log_sync", "{message}");
    }
}

async fn sleep_interruptible(host: &Host, shutdown: &AtomicBool, pause: Pause) {
    let duration = match pause {
        Pause::Reopen => return,
        Pause::Idle => Duration::from_secs(1),
        Pause::Retry => Duration::from_secs(5),
        Pause::Snapshot => Duration::from_secs(30),
        Pause::Backoff => Duration::from_secs(60),
        Pause::PaymentHold => Duration::from_secs(300),
    };
    let deadline = Instant::now() + duration;
    let mut notified = std::pin::pin!(host.cloud_sync.notified());
    if notified.as_mut().enable() {
        return;
    }
    if shutdown.load(Ordering::Acquire) || super::signals::wake_requested(&host.home) {
        return;
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return,
            () = &mut notified => return,
            () = tokio::time::sleep(remaining.min(FALLBACK_MARKER_CHECK)) => {
                if Instant::now() >= deadline || super::signals::any_wake_marker(&host.home) {
                    return;
                }
            }
        }
    }
}

async fn wait_for_shutdown(shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(SHUTDOWN_CHECK).await;
    }
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
