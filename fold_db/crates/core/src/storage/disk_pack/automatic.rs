//! Compatible native lookup replacement and small, independently gated cleanup steps.

use super::*;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};
use tracing::Instrument;

pub(super) struct AutomaticMaintenance {
    enabled: bool,
    trigger_bytes: u64,
    part_growth: u64,
    trigger_parts: AtomicU64,
    parts: AtomicU64,
    written_bytes: AtomicU64,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    stop: tokio::sync::watch::Sender<bool>,
    cleanup: AtomicBool,
    runs: AtomicU64,
    full_runs: AtomicU64,
    bytes_removed: AtomicU64,
    last_repack_ms: AtomicU64,
    last_cleanup_ms: AtomicU64,
    error: Mutex<Option<String>>,
}

#[derive(serde::Serialize)]
pub struct AutomaticMaintenanceStatus {
    enabled: bool,
    trigger_bytes: u64,
    trigger_parts: u64,
    parts: u64,
    written_lookup_bytes: u64,
    cleanup_pending: bool,
    runs: u64,
    full_runs: u64,
    bytes_removed: u64,
    last_repack_ms: u64,
    last_cleanup_ms: u64,
    last_error: Option<String>,
}

impl AutomaticMaintenance {
    pub(super) fn new(parts: u64) -> StorageResult<Self> {
        let part_growth = setting("LASTDB_LOOKUP_REPACK_PARTS", 2048, 32)?;
        Ok(Self {
            enabled: env_flag::var_parse("LASTDB_AUTO_MAINTENANCE").unwrap_or(true),
            trigger_bytes: setting("LASTDB_LOOKUP_REPACK_BYTES", 256 * 1024 * 1024, 16384)?,
            part_growth,
            trigger_parts: AtomicU64::new(part_growth),
            parts: AtomicU64::new(parts),
            written_bytes: AtomicU64::new(0),
            worker: Mutex::new(None),
            stop: tokio::sync::watch::channel(false).0,
            cleanup: AtomicBool::new(false),
            runs: AtomicU64::new(0),
            full_runs: AtomicU64::new(0),
            bytes_removed: AtomicU64::new(0),
            last_repack_ms: AtomicU64::new(0),
            last_cleanup_ms: AtomicU64::new(0),
            error: Mutex::new(None),
        })
    }

    fn wants_repack(&self) -> bool {
        self.parts.load(Relaxed) >= self.trigger_parts.load(Relaxed)
            || self.written_bytes.load(Relaxed) >= self.trigger_bytes
    }

    pub(super) fn published(&self, parts: u64, pages: u64) {
        self.parts.store(parts, Relaxed);
        self.written_bytes
            .fetch_add(pages.saturating_mul(16384), Relaxed);
    }

    pub(super) fn status(&self) -> AutomaticMaintenanceStatus {
        AutomaticMaintenanceStatus {
            enabled: self.enabled,
            trigger_bytes: self.trigger_bytes,
            trigger_parts: self.trigger_parts.load(Relaxed),
            parts: self.parts.load(Relaxed),
            written_lookup_bytes: self.written_bytes.load(Relaxed),
            cleanup_pending: self.cleanup.load(Relaxed),
            runs: self.runs.load(Relaxed),
            full_runs: self.full_runs.load(Relaxed),
            bytes_removed: self.bytes_removed.load(Relaxed),
            last_repack_ms: self.last_repack_ms.load(Relaxed),
            last_cleanup_ms: self.last_cleanup_ms.load(Relaxed),
            last_error: self.error.lock().expect("native maintenance error").clone(),
        }
    }
}

impl PackRuntime {
    /// Stop new maintenance before the daemon begins its final write barriers.
    pub fn request_automatic_stop(&self) {
        self.automatic.stop.send_replace(true);
    }

    /// Finish any current publication before the Tokio runtime can shut down.
    pub async fn stop_automatic(&self) -> StorageResult<()> {
        self.request_automatic_stop();
        let task = self
            .automatic
            .worker
            .lock()
            .expect("native maintenance task")
            .take();
        if let Some(task) = task {
            task.await.map_err(|error| {
                StorageError::BackendError(format!("native maintenance task did not stop: {error}"))
            })?;
        }
        Ok(())
    }

    pub(super) fn ensure_automatic(self: &Arc<Self>) {
        let mut worker = self
            .automatic
            .worker
            .lock()
            .expect("native maintenance task");
        if !self.automatic.enabled
            || tokio::runtime::Handle::try_current().is_err()
            || *self.automatic.stop.borrow()
            || worker.is_some()
        {
            return;
        }
        let weak = Arc::downgrade(self);
        let mut stop = self.automatic.stop.subscribe();
        *worker = Some(tokio::spawn(async move {
            loop {
                if *stop.borrow() { return; }
                let Some(runtime) = weak.upgrade() else { return };
                let pending = runtime.automatic.cleanup.load(Relaxed);
                let due = runtime.automatic.wants_repack();
                let mut failed = false;
                    if !runtime.initializing.load(Ordering::Acquire)
                        && !runtime.requires_recovery() && (pending || due) {
                    let gate = runtime.enter_write().await;
                    if *stop.borrow() { return; }
                    let working = Arc::clone(&runtime);
                    let result = super::super::laststore::LastStoreKvStore::run_blocking(move || {
                        let _gate = gate;
                        working.automatic_step(due)
                    }).await;
                    match result {
                        Ok(()) => *runtime.automatic.error.lock().expect("native maintenance error") = None,
                        Err(error) => {
                            *runtime.automatic.error.lock().expect("native maintenance error") = Some(error.to_string());
                            tracing::warn!(%error, "native automatic lookup maintenance deferred");
                            failed = true;
                        }
                    }
                }
                drop(runtime);
                if *stop.borrow() { return; }
                let delay = if failed { Duration::from_secs(30) } else if pending || due {
                    Duration::from_millis(250)
                } else { Duration::from_secs(1) };
                tokio::select! {
                    biased;
                    _ = stop.changed() => {},
                    () = tokio::time::sleep(delay) => {},
                }
            }
        }.instrument(tracing::info_span!("native_lookup_maintenance"))));
    }

    fn automatic_step(&self, repack: bool) -> StorageResult<()> {
        if self.requires_recovery() {
            return Ok(());
        }
        let policy = laststore::disk_pack::maintenance::SpacePolicy {
            grace_ms: 0,
            reclaim_generations: 8,
            ..self.space_policy.clone()
        };
        let started = Instant::now();
        let mut writer = self.journal.lock().expect("native journal owner");
        if repack {
            // Lookup-only steps retain live payload objects. Periodically pack
            // their current records too, so one live record cannot retain a
            // batch's obsolete bodies forever. Start with a complete pack.
            let full = self.automatic.runs.load(Relaxed).is_multiple_of(8);
            let packed = if full {
                writer.pack(&policy)
            } else {
                writer.repack_lookups(&policy)
            };
            let receipt = match packed {
                Ok(receipt) => receipt,
                Err(error) => {
                    if writer.is_poisoned() {
                        self.failed.store(true, Ordering::Release);
                    }
                    return Err(map(error));
                }
            };
            // Accepted requests were drained under the complete write gate.
            let mut view = self.view.write().expect("native complete read view");
            if !view.accepted.is_empty() {
                self.failed.store(true, Ordering::Release);
                return Err(StorageError::BackendError(
                    "automatic lookup publication retained accepted work".into(),
                ));
            }
            view.current = writer.current().clone();
            self.automatic.parts.store(receipt.parts_after, Relaxed);
            self.automatic.trigger_parts.store(
                receipt
                    .parts_after
                    .saturating_add(self.automatic.part_growth)
                    .min(49_152),
                Relaxed,
            );
            self.automatic.written_bytes.store(0, Relaxed);
            self.automatic.cleanup.store(true, Relaxed);
            self.automatic.runs.fetch_add(1, Relaxed);
            if full {
                self.automatic.full_runs.fetch_add(1, Relaxed);
            }
            self.automatic
                .last_repack_ms
                .store(started.elapsed().as_millis() as u64, Relaxed);
        } else {
            // A small cleanup excludes new read captures only for this step.
            let _view = self.view.write().expect("native complete read view");
            let receipt = writer.reclaim(&policy).map_err(map)?;
            self.automatic.cleanup.store(
                receipt.reclaim_deferred || receipt.reclaim_remaining,
                Relaxed,
            );
            self.automatic
                .bytes_removed
                .fetch_add(receipt.bytes_removed, Relaxed);
            self.automatic
                .last_cleanup_ms
                .store(started.elapsed().as_millis() as u64, Relaxed);
        }
        Ok(())
    }
}

fn setting(name: &str, default: u64, minimum: u64) -> StorageResult<u64> {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<u64>()
            .ok()
            .filter(|n| *n >= minimum)
            .ok_or_else(|| {
                StorageError::ConfigurationError(format!(
                    "{name} must be an integer of at least {minimum}"
                ))
            }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(StorageError::ConfigurationError(format!(
            "{name} must be a valid integer"
        ))),
    }
}
