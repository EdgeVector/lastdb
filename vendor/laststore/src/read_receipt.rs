//! Per-call physical read costs. A context follows explicitly attached workers.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Physical I/O issued by one observed synchronous call and its attached workers.
/// Bytes count successful userspace reads, including reads served by the OS cache;
/// they do not claim physical-device traffic. No paths or key values are retained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskReadReceipt {
    /// Exact logical keys requested by the embedding caller.
    pub requested_keys: u64,
    /// Logical records served by the embedding caller's warm set.
    pub logical_warm_hits: u64,
    /// Logical records absent from the embedding caller's warm set.
    pub logical_warm_misses: u64,
    /// Attempts to enter a cold loader, including cap refusals.
    pub loader_calls: u64,
    /// Existing group directories admitted to the storage parser.
    pub group_opens: u64,
    /// Read-only file open attempts, including failed opens.
    pub file_open_attempts: u64,
    /// Successful read-only file opens; repeated opens count separately.
    pub file_opens: u64,
    /// File status, existence, and directory-entry status operations.
    pub metadata_operations: u64,
    /// Directory enumeration attempts.
    pub directory_reads: u64,
    /// Directory entries returned to the caller.
    pub directory_entries: u64,
    /// File read calls, including EOF and failed reads.
    pub read_operations: u64,
    /// Bytes read to reconstruct legacy group state or replay append tails.
    pub legacy_replay_bytes: u64,
    /// Frame bytes read during explicit recovery of complete disk-pack journal batches.
    pub journal_replay_bytes: u64,
    /// Bytes read for sorted footers and key sidecars.
    pub footer_bytes: u64,
    /// Bytes read from addressed record or sorted-block extents.
    pub payload_bytes: u64,
    /// Bytes read for format, manifest, part, or allocation-bound probes.
    pub format_probe_bytes: u64,
    /// Output bytes produced by successful frame decode or plain replay copies.
    pub decoded_bytes: u64,
    /// Plaintext bytes produced by the embedding caller's outer value decrypt.
    pub decrypted_bytes: u64,
    /// Outer value decrypt time; frame authentication is included in decode_nanos.
    pub decrypt_nanos: u64,
    /// Sum of waits for explicitly instrumented locks, across attached workers.
    pub lock_wait_nanos: u64,
    /// Sum of waits before explicitly attached blocking workers start.
    pub queue_wait_nanos: u64,
    /// Peak concurrent capacity of instrumented transient byte buffers.
    /// This excludes returned bodies, warm caches, allocator overhead, and
    /// parsed metadata objects; it is not a process-memory measurement.
    pub temporary_bytes_peak: u64,
    /// Sum of file read durations, including failed calls and OS-cache reads.
    pub read_nanos: u64,
    /// Sum of authentication, decompression, and plain-copy durations.
    pub decode_nanos: u64,
    /// Inclusive admitted group reconstruction time in load_shard.
    /// This overlaps its nested open, metadata, read, and decode durations.
    /// Errors and unwinds contribute the elapsed reconstruction time.
    pub group_load_nanos: u64,
    /// Sum of read-only file open durations, including failed attempts.
    pub open_nanos: u64,
    /// Sum of status and directory enumeration durations.
    pub metadata_nanos: u64,
}

#[derive(Default)]
struct State {
    receipt: DiskReadReceipt,
    temporary_bytes: u64,
}

/// A per-call context that explicitly follows synchronous and parallel workers.
/// Capture before dispatch, then call `with` inside the worker. Nested observers
/// contribute to their parent once; unrelated threads have no context.
#[derive(Clone)]
pub struct DiskReadContext(Vec<Arc<Mutex<State>>>);

thread_local! {
    static CURRENT: RefCell<DiskReadContext> = const { RefCell::new(DiskReadContext(Vec::new())) };
}

impl Default for DiskReadContext {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskReadContext {
    /// Create an independent empty receipt.
    pub fn new() -> Self {
        Self(vec![Arc::new(Mutex::new(State::default()))])
    }

    /// Create a separate child receipt that also contributes to this context.
    pub fn child(&self) -> Self {
        let mut child = self.clone();
        child.0.push(Arc::new(Mutex::new(State::default())));
        child
    }

    /// Capture the currently active scope, including its parent scopes.
    pub fn capture() -> Option<Self> {
        CURRENT.with(|scope| {
            let current = scope.borrow();
            (!current.0.is_empty()).then(|| current.clone())
        })
    }

    /// Attach this context for synchronous work. Unwind restores the prior scope.
    pub fn with<T>(&self, work: impl FnOnce() -> T) -> T {
        struct Restore(DiskReadContext);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT.with(|scope| scope.replace(self.0.clone()));
            }
        }
        let old = CURRENT.with(|scope| {
            let mut combined = scope.borrow().clone();
            for state in &self.0 {
                if !combined.0.iter().any(|old| Arc::ptr_eq(old, state)) {
                    combined.0.push(Arc::clone(state));
                }
            }
            scope.replace(combined)
        });
        let _restore = Restore(old);
        work()
    }

    /// Read this scope's receipt. Join attached workers before the final read.
    pub fn snapshot(&self) -> DiskReadReceipt {
        self.0
            .last()
            .map_or_else(DiskReadReceipt::default, |state| {
                state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .receipt
            })
    }

    fn update(&self, mut update: impl FnMut(&mut State)) {
        for state in &self.0 {
            update(&mut state.lock().unwrap_or_else(|error| error.into_inner()));
        }
    }
}

/// Observe one synchronous storage call. Errors are returned with their receipt.
pub fn observe_disk_reads<T>(work: impl FnOnce() -> T) -> (T, DiskReadReceipt) {
    let context = DiskReadContext::new();
    let result = context.with(work);
    (result, context.snapshot())
}

pub(crate) struct ReadScope {
    context: DiskReadContext,
    previous: DiskReadContext,
}

pub(crate) fn enter_child() -> ReadScope {
    let context =
        DiskReadContext::capture().map_or_else(DiskReadContext::new, |scope| scope.child());
    let previous = CURRENT.with(|scope| scope.replace(context.clone()));
    ReadScope { context, previous }
}

impl ReadScope {
    pub(crate) fn snapshot(&self) -> DiskReadReceipt {
        self.context.snapshot()
    }
}

impl Drop for ReadScope {
    fn drop(&mut self) {
        CURRENT.with(|scope| scope.replace(self.previous.clone()));
    }
}

pub(crate) fn update(update: impl FnMut(&mut DiskReadReceipt)) {
    if let Some(context) = DiskReadContext::capture() {
        let mut update = update;
        context.update(|state| update(&mut state.receipt));
    }
}

/// Record logical requested keys once, at the embedding storage-call boundary.
pub fn record_requested_keys(keys: u64) {
    update(|receipt| receipt.requested_keys = receipt.requested_keys.saturating_add(keys));
}

/// Record logical warm hits at the embedding logical-record boundary.
pub fn record_logical_warm_hits(records: u64) {
    update(|receipt| receipt.logical_warm_hits = receipt.logical_warm_hits.saturating_add(records));
}

/// Record logical warm misses at the embedding logical-record boundary.
pub fn record_logical_warm_misses(records: u64) {
    update(|receipt| {
        receipt.logical_warm_misses = receipt.logical_warm_misses.saturating_add(records)
    });
}

/// Record one outer value decryption. Frame decryption is measured separately.
pub fn record_decryption(bytes: u64, elapsed: Duration) {
    let nanos = duration_nanos(elapsed);
    update(|receipt| {
        receipt.decrypted_bytes = receipt.decrypted_bytes.saturating_add(bytes);
        receipt.decrypt_nanos = receipt.decrypt_nanos.saturating_add(nanos);
    });
}

/// Record a wait for an explicitly instrumented lock.
pub fn record_lock_wait(elapsed: Duration) {
    let nanos = duration_nanos(elapsed);
    update(|receipt| receipt.lock_wait_nanos = receipt.lock_wait_nanos.saturating_add(nanos));
}

/// Record the interval between a blocking-work dispatch and the worker's start.
pub fn record_queue_wait(elapsed: Duration) {
    let nanos = duration_nanos(elapsed);
    update(|receipt| receipt.queue_wait_nanos = receipt.queue_wait_nanos.saturating_add(nanos));
}

fn duration_nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) struct Timer(Option<(DiskReadContext, Instant)>);

impl Timer {
    pub(crate) fn start() -> Self {
        Self(DiskReadContext::capture().map(|context| (context, Instant::now())))
    }

    pub(crate) fn finish(self, mut record: impl FnMut(&mut DiskReadReceipt, u64)) {
        if let Some((context, start)) = self.0 {
            let nanos = duration_nanos(start.elapsed());
            context.update(|state| record(&mut state.receipt, nanos));
        }
    }
}

pub(crate) struct GroupLoadTimer(Option<Timer>);

pub(crate) fn group_load_timer() -> GroupLoadTimer {
    GroupLoadTimer(Some(Timer::start()))
}

impl Drop for GroupLoadTimer {
    fn drop(&mut self) {
        if let Some(timer) = self.0.take() {
            timer.finish(|receipt, nanos| {
                receipt.group_load_nanos = receipt.group_load_nanos.saturating_add(nanos);
            });
        }
    }
}

/// Charge byte-buffer capacity for the guard's lifetime, across all attached workers.
pub(crate) struct TemporaryBytes {
    context: Option<DiskReadContext>,
    bytes: u64,
}

impl TemporaryBytes {
    pub(crate) fn new(bytes: usize) -> Self {
        let mut guard = Self {
            context: DiskReadContext::capture(),
            bytes: 0,
        };
        guard.resize(bytes);
        guard
    }

    pub(crate) fn resize(&mut self, bytes: usize) {
        let next = bytes as u64;
        if let Some(context) = &self.context {
            context.update(|state| {
                state.temporary_bytes = state.temporary_bytes.saturating_sub(self.bytes);
                state.temporary_bytes = state.temporary_bytes.saturating_add(next);
                state.receipt.temporary_bytes_peak = state
                    .receipt
                    .temporary_bytes_peak
                    .max(state.temporary_bytes);
            });
        }
        self.bytes = next;
    }

    pub(crate) fn grow(&mut self, bytes: usize) {
        self.resize((self.bytes as usize).saturating_add(bytes));
    }
}

impl Drop for TemporaryBytes {
    fn drop(&mut self) {
        self.resize(0);
    }
}
