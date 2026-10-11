//! Accept loop, FD-exhaustion backoff and the per-connection dispatch for
//! [`UdsSocket::serve`] (invariant **I3a**).

use super::*;

// --- Accept loop (I3a) ------------------------------------------------------
//
// The loop that runs on the bound listener: it accepts connections, evaluates
// each one's peer credential (above), drops the rejected ones, and hands each
// accepted connection — with its access posture — to a dispatch
// callback. The caller supplies the verifier and the HTTP handler callback.
// This loop owns accept, peer evaluation, dispatch, and graceful shutdown;
// it does not parse HTTP.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Longest the accept loop waits before re-checking the shutdown flag when no
/// connection is pending.
///
/// This is a **shutdown-responsiveness** bound, not a service interval: the loop
/// waits on the listener fd itself ([`wait_for_pending_connection`]), so a peer
/// that connects mid-wait is picked up immediately and only an *idle* loop ever
/// waits this long.
///
/// It used to be a blind `thread::sleep` between non-blocking `accept` calls,
/// which made the loop a free-running poll grid: a connection landing just after
/// a wake sat in the backlog until the next one. Every request pays that wait
/// (the control socket answers `Connection: close`, so each request is a fresh
/// connection), and a *serial* client is the worst case rather than the average —
/// its next `connect` lands just after the wake its previous response came from,
/// so it pays a near-full interval every time instead of the half-interval mean.
///
/// Measured on the primary 2026-08-04: wakes every ~196ms serving 17-25 queued
/// connections each, a 3.93x overshoot of the 50ms constant (`thread::sleep` is
/// a floor, and macOS coalesces timers for an idle process). Client-visible cost
/// was ~186ms per serial request against a handler that answers in 0.1ms — 99.9%
/// of the wait was this grid, and it was invisible to the node's own request
/// telemetry because it elapses before the handler starts.
/// Brain: `papercut-lastdb-183ms-fixed-latency-per-socket-request-on-an-idle-node`.
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait until the listener has a connection pending, or `timeout` elapses.
///
/// The listener stays non-blocking (so `accept` never parks and a set shutdown
/// flag is still observed within `timeout`), but the *wait* happens on the fd
/// rather than on the clock — `poll` returns as soon as a peer connects, so
/// accept latency is bounded by the peer's own `connect` instead of by a timer
/// grid.
///
/// Any error, including `EINTR`, simply returns: the caller re-checks `shutdown`
/// and retries `accept`, so a spurious wake costs one extra loop turn and never
/// a missed connection.
fn wait_for_pending_connection(fd: std::os::unix::io::RawFd, timeout: Duration) {
    let mut pending = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
    // SAFETY: `pending` is a single initialized `pollfd` owned by this frame and
    // `fd` is the listener's, valid for the duration of the call.
    unsafe {
        libc::poll(&raw mut pending, 1, timeout_ms);
    }
}

fn is_fd_exhaustion_accept_error(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(code) if code == libc::EMFILE || code == libc::ENFILE
    )
}

/// Ceiling on the accept-loop backoff while descriptors stay exhausted.
///
/// The first retry waits [`ACCEPT_POLL_INTERVAL`] and each further one doubles
/// up to this bound, so a minute-long episode costs a few dozen wakeups instead
/// of twenty a second — while the listener still returns to service within a
/// second of descriptors freeing up.
const FD_EXHAUSTION_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// How often a *continuing* fd-exhaustion episode restates itself in the log.
const FD_EXHAUSTION_SUMMARY_INTERVAL: Duration = Duration::from_secs(30);

/// What one failed accept should put in the log.
#[derive(Debug, PartialEq, Eq)]
enum FdExhaustionLog {
    /// First failure of a new episode — report it at ERROR.
    Onset,
    /// The episode is still running and the summary interval has elapsed —
    /// restate it at WARN.
    Continuing { attempts: u64, elapsed: Duration },
    /// Same episode, inside the summary interval — say nothing.
    Suppressed,
}

/// Shape of a closed episode, for the one line that reports recovery.
#[derive(Debug, PartialEq, Eq)]
struct FdExhaustionRecovery {
    attempts: u64,
    elapsed: Duration,
}

/// Log de-duplication and escalating backoff for accept-time fd exhaustion.
///
/// `accept` keeps returning `EMFILE`/`ENFILE` for as long as the process is out
/// of descriptors — the pending connection stays queued, so every retry fails
/// the same way. Reporting each retry at ERROR turned one ~95-second episode
/// into 492 identical Sentry issues (`7641868650`): the same fact, captured
/// twenty times a second, drowning the issue stream it was supposed to raise.
///
/// So an episode is reported on its *edges* instead — ERROR once at onset, WARN
/// at most once per [`FD_EXHAUSTION_SUMMARY_INTERVAL`] while it lasts, and one
/// line when accept succeeds again. An operator still learns that the node ran
/// out of descriptors, how long it lasted, and how many accepts it cost; Sentry
/// gets one issue per episode rather than one per retry.
#[derive(Debug, Default)]
struct FdExhaustionEpisode {
    /// When this episode's first failed accept landed. `None` between episodes.
    started_at: Option<Instant>,
    /// When this episode last put a line in the log.
    last_reported_at: Option<Instant>,
    /// Failed accepts so far in this episode.
    attempts: u64,
}

impl FdExhaustionEpisode {
    /// Record one failed accept: decide what to log, and how long to wait
    /// before retrying.
    fn record(&mut self, now: Instant) -> (FdExhaustionLog, Duration) {
        self.attempts = self.attempts.saturating_add(1);
        let started_at = *self.started_at.get_or_insert(now);
        let backoff = Self::backoff_for_attempt(self.attempts);

        if self.attempts == 1 {
            self.last_reported_at = Some(now);
            return (FdExhaustionLog::Onset, backoff);
        }

        let quiet_long_enough = match self.last_reported_at {
            Some(last) => now.duration_since(last) >= FD_EXHAUSTION_SUMMARY_INTERVAL,
            None => true,
        };
        if !quiet_long_enough {
            return (FdExhaustionLog::Suppressed, backoff);
        }

        self.last_reported_at = Some(now);
        let report = FdExhaustionLog::Continuing {
            attempts: self.attempts,
            elapsed: now.duration_since(started_at),
        };
        (report, backoff)
    }

    /// Close the episode after an accept succeeds. Returns the episode's shape
    /// for one recovery line, or `None` when no episode was open.
    ///
    /// Only a successful accept clears the state. `WouldBlock` does not: an
    /// exhausted process whose peers all gave up waiting reports an empty
    /// backlog too, and treating that as recovery would report the episode over
    /// while it is still running.
    fn recovered(&mut self, now: Instant) -> Option<FdExhaustionRecovery> {
        let started_at = self.started_at.take()?;
        let attempts = std::mem::take(&mut self.attempts);
        self.last_reported_at = None;
        Some(FdExhaustionRecovery {
            attempts,
            elapsed: now.duration_since(started_at),
        })
    }

    /// Backoff before retrying the `attempts`-th consecutive failed accept:
    /// [`ACCEPT_POLL_INTERVAL`] doubled `attempts - 1` times, capped at
    /// [`FD_EXHAUSTION_MAX_BACKOFF`].
    fn backoff_for_attempt(attempts: u64) -> Duration {
        let doublings = u32::try_from(attempts.saturating_sub(1))
            .unwrap_or(u32::MAX)
            .min(u32::BITS - 1);
        ACCEPT_POLL_INTERVAL
            .saturating_mul(1_u32 << doublings)
            .min(FD_EXHAUSTION_MAX_BACKOFF)
    }
}

/// Milliseconds of `d` as a `u64`, the widest integer `tracing` records.
///
/// Every duration reported here is bounded by the summary interval or the
/// backoff cap, so the saturation arm is unreachable in practice.
fn millis_for_log(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Report one fd-exhaustion accept failure according to the episode state.
fn log_fd_exhaustion(error: &io::Error, report: &FdExhaustionLog, backoff: Duration) {
    match *report {
        FdExhaustionLog::Onset => tracing::error!(
            error = %error,
            backoff_ms = millis_for_log(backoff),
            "control-socket accept hit fd exhaustion; backing off and keeping listener alive"
        ),
        FdExhaustionLog::Continuing { attempts, elapsed } => tracing::warn!(
            error = %error,
            failed_accepts = attempts,
            elapsed_ms = millis_for_log(elapsed),
            backoff_ms = millis_for_log(backoff),
            "control-socket accept still out of file descriptors; listener alive"
        ),
        FdExhaustionLog::Suppressed => {}
    }
}

/// Close a rejected connection. Closing the stream is the drop; log numeric
/// ids only so the audit trail can't leak namespace data.
fn drop_rejected(stream: std::os::unix::net::UnixStream, verdict: &UdsConnVerdict) {
    if let UdsConnVerdict::Rejected {
        peer_uid,
        owner_uid,
    } = *verdict
    {
        tracing::warn!(
            peer_uid,
            owner_uid,
            "dropping control-socket connection from a foreign OS user"
        );
    }
    drop(stream);
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl UdsSocket {
    /// Run the control-socket accept loop until `shutdown` is set.
    ///
    /// For each connection it reads the peer credential and applies the
    /// same-user gate ([`evaluate_connection`]). A [`UdsConnVerdict::Rejected`]
    /// connection is dropped (its stream closed) and logged with numeric ids
    /// only — never any namespace data (the I4 concern). A
    /// [`UdsConnVerdict::Accepted`] one is handed to `on_accept` together with
    /// the [`CallerTransport`] / [`CallerVerification`] it contributes to an
    /// [`AccessContext`](fold_db::access::AccessContext). The injected verifier
    /// can upgrade the base `Unverified` posture after the same-user gate.
    /// A connection whose peer-credential read errors is dropped and the
    /// loop continues — one bad connection never tears the loop down.
    ///
    /// The listener is switched to non-blocking so the loop observes `shutdown`
    /// promptly instead of parking in `accept` until the next client arrives.
    /// Each accepted stream is restored to blocking mode before dispatch, the
    /// mode a synchronous handler expects.
    pub fn serve<F, V>(
        &self,
        owner_uid: u32,
        shutdown: &AtomicBool,
        verify_handle: V,
        mut on_accept: F,
    ) -> io::Result<()>
    where
        F: FnMut(std::os::unix::net::UnixStream, CallerTransport, CallerVerification),
        V: Fn(Option<CallerHandle>) -> CallerVerification,
    {
        use std::os::unix::io::AsRawFd;

        self.listener.set_nonblocking(true)?;
        let mut fd_exhaustion = FdExhaustionEpisode::default();
        while !shutdown.load(Ordering::Relaxed) {
            let (stream, _addr) = match self.listener.accept() {
                Ok(conn) => {
                    if let Some(recovery) = fd_exhaustion.recovered(Instant::now()) {
                        tracing::warn!(
                            failed_accepts = recovery.attempts,
                            elapsed_ms = millis_for_log(recovery.elapsed),
                            "control-socket accept succeeded again after fd exhaustion"
                        );
                    }
                    conn
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Wait on the fd, not the clock: a peer that connects during
                    // this call wakes it immediately. Sleeping here instead is
                    // what made the loop a poll grid and put ~186ms in front of
                    // every request (see `ACCEPT_POLL_INTERVAL`).
                    wait_for_pending_connection(self.listener.as_raw_fd(), ACCEPT_POLL_INTERVAL);
                    continue;
                }
                Err(e) if is_fd_exhaustion_accept_error(&e) => {
                    // One episode, not one report per retry: every retry fails
                    // identically while descriptors are out, and the ERROR layer
                    // turns each one into its own Sentry issue.
                    let (report, backoff) = fd_exhaustion.record(Instant::now());
                    log_fd_exhaustion(&e, &report, backoff);
                    // Deliberately a blind sleep, unlike the WouldBlock arm: the
                    // connection stays pending while fds are exhausted, so the fd
                    // is readable and polling it would spin at 100% CPU. Time is
                    // the only thing worth waiting on here.
                    std::thread::sleep(backoff);
                    continue;
                }
                Err(e) => return Err(e),
            };
            // A handler reads/writes the stream synchronously; restore blocking
            // mode in case the non-blocking listener propagated its flag. A
            // failure here taints only THIS connection — drop it and continue,
            // never `?`-propagate (that would exit the loop and unlink the
            // socket on `UdsSocket::drop`, taking the whole control plane down
            // for every other app over one racing/expired peer). This mirrors
            // the peer-cred-read error arm below. (sec review 2026-06-15.)
            if let Err(e) = stream.set_nonblocking(false) {
                tracing::warn!(
                    error = %e,
                    "dropping control-socket connection: could not restore blocking mode"
                );
                drop(stream);
                continue;
            }
            match evaluate_connection(&stream, owner_uid) {
                Ok(verdict) => {
                    if let Some((transport, _base)) = verdict.access_posture() {
                        // I3b: upgrade the base `Unverified` posture by running the
                        // code-signature check on the accepted caller handle — the
                        // audit token when reported (blocker B3: its pidversion
                        // invalidates on PID reuse), else the pid. The verifier is
                        // OS-/feature-injected — the real macOS `SecCode` check in
                        // production, `Unverified` on platforms/builds without it.
                        let verification = verify_handle(verdict.candidate_handle());
                        on_accept(stream, transport, verification);
                    } else {
                        drop_rejected(stream, &verdict);
                    }
                }
                Err(e) => {
                    // A failed peer-credential read taints only this connection.
                    tracing::warn!(
                        error = %e,
                        "dropping control-socket connection: peer-credential read failed"
                    );
                    drop(stream);
                }
            }
        }
        Ok(())
    }
}
