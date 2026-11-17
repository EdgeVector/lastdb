//! One sync turn: pull, apply, then ack. Local capture is the native commit.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::Instrument;

use crate::host::cloud_sync::ProgressMark;
use crate::host::owned::{registered_databases, RegisteredDatabase};
use crate::host::Host;

use super::automatic_retention::Pacing;
use super::cloud::CloudLog;
use super::native;
use super::received;
use super::run;
use super::session;

const SHUTDOWN_CHECK: Duration = Duration::from_millis(250);
const FALLBACK_MARKER_CHECK: Duration = Duration::from_secs(30);
/// Idle turns that still pause one second, so a database in use stays brisk.
const BRISK_IDLE_TURNS: u32 = 5;
/// The longest idle pause. Every node of every member polls, so an idle node
/// must not ask the service once a second.
const IDLE_PAUSE_MAX_SECS: u64 = 60;

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
            let mut quiet = Quiet::default();
            while !shutdown.load(Ordering::Acquire) {
                let pause = one_turn(&host, &mut cleanup).await;
                if matches!(pause, Pause::Reopen) {
                    reopen.store(true, Ordering::Release);
                    shutdown.store(true, Ordering::Release);
                    break;
                }
                let idle = quiet.observe(host.cloud_sync.progress_marks());
                if sleep_interruptible(&host, &shutdown, pause, idle).await {
                    quiet.woken();
                }
            }
        }
        .in_current_span(),
    )
}

/// How many turns in a row moved nothing.
///
/// A turn that uploads or applies anything changes a sync position. While the
/// positions stand still the idle pause grows from one second to a minute. A
/// local write or a wake marker ends the pause at once and makes the next
/// pauses short again.
#[derive(Default)]
struct Quiet {
    marks: Option<Vec<ProgressMark>>,
    turns: u32,
}

impl Quiet {
    fn observe(&mut self, marks: Vec<ProgressMark>) -> Duration {
        if self.marks.as_ref() == Some(&marks) {
            self.turns = self.turns.saturating_add(1);
        } else {
            self.turns = 0;
            self.marks = Some(marks);
        }
        idle_pause(self.turns)
    }

    fn woken(&mut self) {
        self.turns = 0;
    }
}

/// One second for the first few quiet turns, then doubling up to the maximum.
/// The longer pauses vary by a fifth, so nodes that went quiet together do not
/// all return in the same instant.
fn idle_pause(quiet_turns: u32) -> Duration {
    let doublings = quiet_turns.saturating_sub(BRISK_IDLE_TURNS).min(6);
    let secs = (1_u64 << doublings).min(IDLE_PAUSE_MAX_SECS);
    if secs == 1 {
        return Duration::from_secs(1);
    }
    let spread = secs * 200;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| u64::from(since.subsec_nanos()));
    Duration::from_millis(secs * 1000 - spread / 2 + nanos % spread)
}

async fn one_turn(host: &Arc<Host>, cleanup: &mut Pacing) -> Pause {
    if session::consume_wake(&host.home) {
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
    let databases = registered_databases(&host.home);
    host.cloud_sync.retain_databases(|id| {
        databases
            .iter()
            .any(|database| database.kind == "personal" && database.id == id)
    });
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
    database: &RegisteredDatabase,
    log: CloudLog,
    device: &str,
    cleanup: &mut Pacing,
) -> Pause {
    match finish_open(host, &database.id, database.kind, log, device).await {
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
    database: &RegisteredDatabase,
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
        let Some(revision) = received::revision(&log).await else {
            return Prep::Pause(Pause::Retry);
        };
        log.member_revision = revision;
    }
    let state = match run::opened_state(database_id, &log, device, &host.home) {
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

/// Sleep for the pause. Returns true when a local write, a wake marker, or a
/// shutdown ended the sleep before its time.
async fn sleep_interruptible(
    host: &Host,
    shutdown: &AtomicBool,
    pause: Pause,
    idle: Duration,
) -> bool {
    let duration = match pause {
        Pause::Reopen => return false,
        Pause::Idle => idle,
        Pause::Retry => Duration::from_secs(5),
        Pause::Snapshot => Duration::from_secs(30),
        Pause::Backoff => Duration::from_secs(60),
        Pause::PaymentHold => Duration::from_secs(300),
    };
    let deadline = Instant::now() + duration;
    let mut notified = std::pin::pin!(host.cloud_sync.notified());
    if notified.as_mut().enable() {
        return true;
    }
    if shutdown.load(Ordering::Acquire) || session::wake_requested(&host.home) {
        return true;
    }
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown) => return true,
            () = &mut notified => return true,
            () = tokio::time::sleep(remaining.min(FALLBACK_MARKER_CHECK)) => {
                if session::any_wake_marker(&host.home) {
                    return true;
                }
                if Instant::now() >= deadline {
                    return false;
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
