//! Process-wide `tracing` capture for unit tests that assert on log output.
//!
//! ## Why one global subscriber, not `tracing::subscriber::with_default`
//!
//! `tracing-core` keeps two process-global caches: the max-level hint and,
//! per callsite, an `Interest`. Both are rebuilt from whichever dispatchers
//! are live at the moment a callsite is first hit — and with a scoped
//! (thread-local) subscriber, *another* thread can be the one that hits it
//! first. That thread usually has no subscriber at all, so its default is
//! `NoSubscriber`, whose `register_callsite` answers `Interest::never()`. The
//! event macro checks that cached interest before it asks any subscriber, so
//! from then on the event is skipped on **every** thread, including the one
//! running a scoped capture. The capture returns an empty buffer and the
//! assertion reads that as "the code did not log".
//!
//! That is exactly how `an_accepted_write_low_on_headroom_is_reported_before_it_wedges`
//! went red on a required gate while passing locally on the same commit
//! (fold PR 2085): a sibling test walked the size boundaries without a
//! subscriber, registered the headroom callsite as `never`, and the capture on
//! the other thread saw nothing. The silence tests in the same module have the
//! inverse defect — they pass for the wrong reason and never go red.
//!
//! One global dispatcher closes the whole family: `NoSubscriber` never answers
//! a registration again, the global's dynamic filter answers `sometimes` for
//! every callsite, and the max-level hint stays at `TRACE`. Per-test isolation
//! comes from a thread-local sink: the global subscriber formats an event only
//! when the emitting thread has armed a buffer, so parallel tests do not see
//! each other's lines, and a thread with no buffer pays one thread-local read
//! per event.
//!
//! ## What a capture proves
//!
//! [`capture_logs`] emits its own probe event before and after the closure and
//! fails, by name, when either probe does not land. An empty buffer returned
//! from this helper therefore means the code under test emitted nothing on this
//! thread — never that the capture was not wired.
//!
//! Events emitted from a thread the closure spawns are **not** captured; the
//! sink is thread-local by design.

use std::cell::RefCell;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use tracing_subscriber::layer::SubscriberExt;

type Buffer = Arc<Mutex<Vec<u8>>>;

thread_local! {
    static SINK: RefCell<Option<Buffer>> = const { RefCell::new(None) };
}

/// Target of the wiring probe. Lines carrying it are stripped from the
/// captured text, so a caller never sees or matches the probe itself.
const PROBE_TARGET: &str = "fold_db::test_helpers::tracing_capture::probe";

/// `MakeWriter` that resolves the emitting thread's armed buffer per event.
struct ThreadSink;

struct SinkWriter(Option<Buffer>);

impl Write for SinkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(sink) = &self.0 {
            sink.lock()
                .expect("log buffer poisoned")
                .extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for ThreadSink {
    type Writer = SinkWriter;

    fn make_writer(&'a self) -> Self::Writer {
        SinkWriter(SINK.with(|sink| sink.borrow().clone()))
    }
}

/// Install the shared capture subscriber as the process-global default, once.
///
/// Idempotent and cheap after the first call. A test that captures `tracing`
/// output through its *own* scoped subscriber should still call this first:
/// the global dispatcher is what keeps a callsite from being pinned to
/// `Interest::never()` by a bare sibling thread (see the module doc).
///
/// If some other global default is already installed this is a no-op; the
/// probe in [`capture_logs`] then reports the missing wiring by name instead
/// of returning an empty buffer.
pub fn install_global() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_writer(ThreadSink)
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish()
            // `dynamic_filter_fn`, not `filter_fn`: the static variant calls
            // the closure at callsite registration and caches always/never,
            // which would re-create the very defect this module exists to
            // close. The dynamic variant answers `Interest::sometimes()` and
            // asks again on every event.
            .with(tracing_subscriber::filter::dynamic_filter_fn(|_, _| {
                SINK.with(|sink| sink.borrow().is_some())
            }));
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Disarms the calling thread's sink on drop, so a panicking closure does not
/// leave the next test on this thread capturing into a dead buffer.
struct Armed;

impl Drop for Armed {
    fn drop(&mut self) {
        SINK.with(|sink| sink.borrow_mut().take());
    }
}

/// Run `f` and return every `tracing` line it emitted **on this thread**, in
/// the default `tracing_subscriber::fmt` text format (no ANSI), at every
/// level down to `TRACE`.
///
/// Panics, naming the fact, when the capture is not wired for this thread —
/// so an empty return value always means "nothing was logged", not "nothing
/// was heard".
#[track_caller]
pub fn capture_logs(f: impl FnOnce()) -> String {
    install_global();

    let buf: Buffer = Arc::default();
    SINK.with(|sink| {
        assert!(
            sink.borrow().is_none(),
            "capture_logs does not nest: this thread already has an armed capture"
        );
        *sink.borrow_mut() = Some(Arc::clone(&buf));
    });
    let _armed = Armed;

    // The probe runs at TRACE on purpose: a capture that cannot hear the lowest
    // level fails here, not later in a test that asserts DEBUG output.
    probe(&buf, "before");
    buf.lock().expect("log buffer poisoned").clear();

    f();

    probe(&buf, "after");
    let bytes = buf.lock().expect("log buffer poisoned").clone();
    let text = String::from_utf8(bytes).expect("log output is utf-8");
    text.lines()
        .filter(|line| !line.contains(PROBE_TARGET))
        .map(|line| format!("{line}\n"))
        .collect()
}

#[track_caller]
fn probe(buf: &Buffer, phase: &str) {
    tracing::trace!(target: PROBE_TARGET, phase, "capture probe");
    let heard = buf
        .lock()
        .expect("log buffer poisoned")
        .windows(PROBE_TARGET.len())
        .any(|window| window == PROBE_TARGET.as_bytes());
    assert!(
        heard,
        "capture_logs received nothing — not even its own {phase} probe. The \
         tracing capture is not wired for this thread (another global \
         subscriber installed first, or a scoped subscriber shadowing the \
         capture). An empty buffer here is a harness fault, not evidence that \
         the code under test did not log."
    );
}

