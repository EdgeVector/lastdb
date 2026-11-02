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
    /// Records served by the embedding caller's warm set.
    pub logical_warm_hits: u64,
    /// Records absent from the embedding caller's warm set.
    pub logical_warm_misses: u64,
    /// Read-only file open attempts, including failed opens.
    pub file_open_attempts: u64,
    /// Successful read-only file opens; repeated opens count separately.
    pub file_opens: u64,
    /// Explicit file status operations.
    pub metadata_operations: u64,
    /// File read calls, including EOF and failed reads.
    pub read_operations: u64,
    /// Bytes read for disk run footers and index blocks.
    pub footer_bytes: u64,
    /// Bytes read from disk run data blocks and streamed frame payloads.
    pub payload_bytes: u64,
    /// Bytes read for streamed frame headers and tags.
    pub format_probe_bytes: u64,
    /// Output bytes produced by successful frame decode.
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
    /// Sum of frame open, authentication, and decompression durations.
    pub decode_nanos: u64,
    /// Per-chunk derivations and prepared Rust AES schedules. Native per-frame
    /// context setup is included in frame_auth_nanos instead.
    pub frame_key_setups: u64,
    /// Key setup time, also included in decode_nanos.
    pub frame_key_setup_nanos: u64,
    /// Frame authentication time, also included in decode_nanos.
    pub frame_auth_nanos: u64,
    /// Frame header, ciphertext and tag bytes submitted to authentication.
    pub frame_authenticated_bytes: u64,
    /// Large ordinary frames opened through the existing native GCM backend.
    pub frame_native_opens: u64,
    /// Authenticated frame bytes of those native opens.
    pub frame_native_bytes: u64,
    /// Frame zstd time, also included in decode_nanos.
    pub frame_decompress_nanos: u64,
    /// Read-side decompressor contexts created; a lookup can share one.
    pub frame_decompressor_setups: u64,
    /// Ordinary opens whose footer read also supplied the complete top frame.
    pub coalesced_metadata_reads: u64,
    /// Index block decode attempts; reuses do not enter this count or I/O bytes.
    pub metadata_decodes: u64,
    /// Authenticated index blocks reused inside one held read call.
    pub metadata_decode_reuses: u64,
    /// Sum of read-only file open durations, including failed attempts.
    pub open_nanos: u64,
    /// Sum of explicit file status durations.
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
pub struct DiskReadContext(Arc<Vec<Arc<Mutex<State>>>>);

thread_local! {
    static CURRENT: RefCell<Option<DiskReadContext>> = const { RefCell::new(None) };
}

impl Default for DiskReadContext {
    fn default() -> Self {
        Self::new()
    }
}

impl DiskReadContext {
    /// Create an independent empty receipt.
    pub fn new() -> Self {
        Self(Arc::new(vec![Arc::new(Mutex::new(State::default()))]))
    }

    /// Create a separate child receipt that also contributes to this context.
    pub fn child(&self) -> Self {
        let mut states = self.0.as_ref().clone();
        states.push(Arc::new(Mutex::new(State::default())));
        Self(Arc::new(states))
    }

    /// Capture the currently active scope, including its parent scopes.
    pub fn capture() -> Option<Self> {
        CURRENT.with(|scope| scope.borrow().clone())
    }

    /// Attach this context for synchronous work. Unwind restores the prior scope.
    pub fn with<T>(&self, work: impl FnOnce() -> T) -> T {
        struct Restore(Option<DiskReadContext>);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT.with(|scope| scope.replace(self.0.take()));
            }
        }
        let old = CURRENT.with(|scope| {
            let combined = scope
                .borrow()
                .as_ref()
                .map_or_else(|| self.clone(), |current| current.combined(self));
            scope.replace(Some(combined))
        });
        let _restore = Restore(old);
        work()
    }

    /// Scope attachment can join workers, but a timer only clones one Arc.
    /// Shared immutable parent lists avoid a per-frame vector allocation.
    fn combined(&self, other: &Self) -> Self {
        if Arc::ptr_eq(&self.0, &other.0)
            || other
                .0
                .iter()
                .all(|state| self.0.iter().any(|old| Arc::ptr_eq(old, state)))
        {
            return self.clone();
        }
        let mut states = self.0.as_ref().clone();
        for state in other.0.iter() {
            if !states.iter().any(|old| Arc::ptr_eq(old, state)) {
                states.push(Arc::clone(state));
            }
        }
        Self(Arc::new(states))
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
        for state in self.0.iter() {
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
}

impl Drop for TemporaryBytes {
    fn drop(&mut self) {
        self.resize(0);
    }
}
