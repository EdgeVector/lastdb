//! Optional restore counters. No object names, paths, or error text enter this contract.
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RestorePhase {
    Preflight,
    LatestPointer,
    ManifestChain,
    ChunkTransfer,
    Integrity,
    Commit,
    OpenDatabase,
    TailDownload,
    TailReplay,
    Flush,
    Complete,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct RestoreProgressSnapshot {
    pub schema: &'static str,
    pub version: u8,
    pub phase: RestorePhase,
    pub result: &'static str,
    pub elapsed_ms: u64,
    pub phase_elapsed_ms: u64,
    pub phase_timings_ms: BTreeMap<RestorePhase, u64>,
    pub manifests_verified: usize,
    pub chunks_total: Option<usize>,
    pub chunks_installed: usize,
    pub bytes_declared: Option<u64>,
    /// Complete response bodies, including bytes later removed by prefix recovery.
    /// Excludes HTTP overhead and partially received bodies from failed requests.
    pub response_body_bytes: u64,
    pub bytes_installed: u64,
    pub chunks_reused: usize,
    pub bytes_reused: u64,
    /// Sum of whole milliseconds for each local cache attempt, including misses.
    /// Includes lookup/read/hash; excludes cloud fallback and installation.
    pub cache_read_ms: u64,
    pub queued_downloads: usize,
    pub reserved_bytes: u64,
    /// Sum of completed operation durations. Concurrent operations overlap.
    pub authorizations_active: usize,
    pub transfers_active: usize,
    pub authorization_ms: u64,
    pub download_ms: u64,
    pub install_ms: u64,
    pub tail_objects_total: Option<usize>,
    pub tail_objects_downloaded: usize,
    pub replay_segments_total: Option<usize>,
    pub replay_segments_applied: usize,
    pub replay_records_applied: usize,
}

pub struct RestoreProgress {
    state: Mutex<(RestoreProgressSnapshot, Instant)>,
    started: Instant,
    emit: Box<dyn Fn(&RestoreProgressSnapshot) + Send + Sync>,
}

impl RestoreProgress {
    pub fn new(emit: impl Fn(&RestoreProgressSnapshot) + Send + Sync + 'static) -> Self {
        let started = Instant::now();
        Self {
            state: Mutex::new((
                RestoreProgressSnapshot {
                    schema: "lastdb.restore.progress",
                    version: 1,
                    phase: RestorePhase::Preflight,
                    result: "active",
                    elapsed_ms: 0,
                    phase_elapsed_ms: 0,
                    phase_timings_ms: BTreeMap::new(),
                    manifests_verified: 0,
                    chunks_total: None,
                    chunks_installed: 0,
                    bytes_declared: None,
                    response_body_bytes: 0,
                    bytes_installed: 0,
                    chunks_reused: 0,
                    bytes_reused: 0,
                    cache_read_ms: 0,
                    queued_downloads: 0,
                    reserved_bytes: 0,
                    authorizations_active: 0,
                    transfers_active: 0,
                    authorization_ms: 0,
                    download_ms: 0,
                    install_ms: 0,
                    tail_objects_total: None,
                    tail_objects_downloaded: 0,
                    replay_segments_total: None,
                    replay_segments_applied: 0,
                    replay_records_applied: 0,
                },
                started,
            )),
            started,
            emit: Box::new(emit),
        }
    }

    pub(crate) fn update(&self, f: impl FnOnce(&mut RestoreProgressSnapshot)) {
        if let Ok(mut state) = self.state.lock() {
            f(&mut state.0);
        }
    }

    /// Copy counters without holding the state lock during sink delivery.
    pub fn snapshot(&self) -> Option<RestoreProgressSnapshot> {
        self.state.lock().ok().map(|state| {
            let mut snapshot = state.0.clone();
            snapshot.elapsed_ms = millis(self.started);
            snapshot.phase_elapsed_ms = millis(state.1);
            snapshot
        })
    }

    /// The callback must be nonblocking. The CLI uses a bounded try_send queue.
    pub fn emit(&self) {
        if let Some(snapshot) = self.snapshot() {
            (self.emit)(&snapshot);
        }
    }

    pub fn phase(&self, phase: RestorePhase) {
        if let Ok(mut state) = self.state.lock() {
            let previous = state.0.phase;
            let elapsed = millis(state.1);
            let total = state.0.phase_timings_ms.entry(previous).or_default();
            *total = total.saturating_add(elapsed);
            state.0.phase = phase;
            state.0.result = match phase {
                RestorePhase::Complete => "success",
                RestorePhase::Failed => "failure",
                _ => "active",
            };
            if matches!(phase, RestorePhase::Complete | RestorePhase::Failed) {
                state.0.queued_downloads = 0;
                state.0.reserved_bytes = 0;
                state.0.authorizations_active = 0;
                state.0.transfers_active = 0;
            }
            state.1 = Instant::now();
        }
        self.emit();
    }
}

pub(crate) fn millis(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(super) fn update(
    progress: Option<&RestoreProgress>,
    f: impl FnOnce(&mut RestoreProgressSnapshot),
) {
    if let Some(progress) = progress {
        progress.update(f);
    }
}

pub(super) fn phase(progress: Option<&RestoreProgress>, phase: RestorePhase) {
    if let Some(progress) = progress {
        progress.phase(phase);
    }
}

/// Measure the complete synchronous cache attempt before any cloud fallback.
pub(super) fn measure_cache_read<T>(
    progress: Option<&RestoreProgress>,
    read: impl FnOnce() -> T,
) -> T {
    let Some(progress) = progress else {
        return read();
    };
    let started = Instant::now();
    let result = read();
    let elapsed = millis(started);
    progress.update(|p| p.cache_read_ms = p.cache_read_ms.saturating_add(elapsed));
    result
}

pub(super) enum TransferOperation {
    Authorization,
    Download,
}

/// Counts wall time even when an operation fails or its parent future is cancelled.
struct OperationTimer<'a> {
    progress: Option<&'a RestoreProgress>,
    operation: TransferOperation,
    started: Instant,
}

impl Drop for OperationTimer<'_> {
    fn drop(&mut self) {
        update(self.progress, |p| {
            let elapsed = millis(self.started);
            match self.operation {
                TransferOperation::Authorization => {
                    p.authorization_ms = p.authorization_ms.saturating_add(elapsed);
                    p.authorizations_active = p.authorizations_active.saturating_sub(1);
                }
                TransferOperation::Download => {
                    p.download_ms = p.download_ms.saturating_add(elapsed);
                    p.transfers_active = p.transfers_active.saturating_sub(1);
                }
            }
        });
    }
}

pub(super) async fn measure<F: std::future::Future>(
    progress: Option<&RestoreProgress>,
    operation: TransferOperation,
    future: F,
) -> F::Output {
    update(progress, |p| match operation {
        TransferOperation::Authorization => p.authorizations_active += 1,
        TransferOperation::Download => p.transfers_active += 1,
    });
    let _timer = OperationTimer {
        progress,
        operation,
        started: Instant::now(),
    };
    future.await
}
