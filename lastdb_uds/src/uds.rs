//! Unix-domain-socket bind and accept path (invariant **I3a** in
//! `exemem-workspace/docs/designs/app_security_model.md`).
//!
//! The default Unix socket serves the node's data routes. Kernel peer
//! credentials establish the same-user gate; a client header does not establish
//! caller identity. The optional `app-isolation` feature adds the macOS
//! code-signature check and per-app jailed sockets.
//!
//! This module owns the socket path, bind, owner-only permissions, stale-socket
//! removal, and cleanup on drop. [`UdsSocket::serve`] accepts connections,
//! checks peer credentials, and passes accepted streams with their access
//! posture to a callback. The injected verifier can upgrade that posture when
//! app isolation is enabled. [`super::uds_http`] reads and serves each request;
//! this module does not parse HTTP.
//!
//! The transport is Unix-only. The default owner socket uses the same-user
//! device-trust posture without requiring a code signature.

use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};

/// File name of the node's Unix-domain control socket within its data dir.
pub const SOCKET_FILE_NAME: &str = "folddb.sock";

/// File name of the node's full-surface setup socket within its data dir.
///
/// Named here (not just at its bind site) so [`preflight_data_dir`] can reject a
/// data dir up front for EVERY socket the node will bind, not only the first.
pub const FULL_SOCKET_FILE_NAME: &str = "folddb-full.sock";

/// Suffix of the temporary sibling [`UdsSocket::bind_at`] binds before the
/// atomic rename into place.
///
/// It is part of the length budget: the temp path is longer than the final one,
/// so a socket path that fits `sockaddr_un` on its own can still fail to bind.
const SOCKET_TMP_SUFFIX: &str = ".tmp";

/// Owner-only permission bits applied to the bound socket file (`rw-------`).
///
/// The same-user peer-credential gate (I3a) is the authoritative check; these
/// filesystem permissions are a cheap second layer that keeps another OS user
/// from even opening the socket. Group/other are denied.
const SOCKET_MODE: u32 = 0o600;

/// Conservative upper bound on the socket path length, in bytes.
///
/// `sockaddr_un::sun_path` is 108 bytes on Linux and 104 on macOS (both
/// NUL-terminated). We validate against the smaller (macOS) limit on every
/// platform so a socket path that binds on Linux but not on macOS is rejected
/// uniformly and early, rather than failing deep in `bind` on one OS only.
/// The usable length is one less than the buffer to leave room for the NUL.
const MAX_SOCKET_PATH_LEN: usize = 104 - 1;

/// Derive the node's control-socket path from its data directory.
///
/// Pure: `<data_dir>/folddb.sock`. Does not touch the filesystem.
pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SOCKET_FILE_NAME)
}

/// Reject a socket path too long for the platform's `sockaddr_un`.
///
/// Binding a `UnixListener` at an over-long path fails inside `bind` with a
/// platform-specific error; checking up front yields a clear, portable error
/// and keeps the failure off the bind path.
fn validate_socket_path_len(path: &Path) -> io::Result<()> {
    let len = path.as_os_str().len();
    if len > MAX_SOCKET_PATH_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "socket path is {len} bytes, exceeds the {MAX_SOCKET_PATH_LEN}-byte \
                 sockaddr_un limit",
            ),
        ));
    }
    Ok(())
}

/// Temp sibling path [`UdsSocket::bind_at`] binds before renaming into place.
///
/// Shared with [`preflight_data_dir`] so the up-front check measures exactly the
/// path bind will use, instead of a re-derivation that could drift from it.
fn tmp_socket_path(path: &Path) -> PathBuf {
    let tmp_file_name = format!(
        "{}{SOCKET_TMP_SUFFIX}",
        path.file_name().map_or_else(
            || SOCKET_FILE_NAME.to_string(),
            |n| n.to_string_lossy().into_owned()
        )
    );
    path.with_file_name(tmp_file_name)
}

/// Reject a data dir whose sockets cannot be bound, BEFORE any boot work runs.
///
/// Pure and I/O-free — it measures path lengths only — so the node can call it
/// the moment the data dir is resolved. That ordering matters for three reasons,
/// all of which bit a real run (`repair-dangling-tips` CoW proof, 2026-08-03):
///
/// 1. **Cost.** `bind` currently happens after `Host::boot`, so a data dir that
///    can never serve still pays a full open + decrypt proof + executor build.
/// 2. **False crash attribution.** The session ledger records a session start
///    before the bind, so a bind refusal leaves no clean-shutdown record and the
///    NEXT boot reports "previous session ended UNCLEANLY" — promoting a static
///    configuration error to Sentry as a crash.
/// 3. **Partial serving.** The full-surface socket is bound later still, so a
///    data dir in the narrow band where only the shorter name fits would serve
///    the narrow socket and then die — a half-up node.
///
/// Checks every socket the node binds under `data_dir`, and each one's temporary
/// rename sibling, reporting the LONGEST offender with the budget a caller can
/// actually act on.
pub fn preflight_data_dir(data_dir: &Path) -> io::Result<()> {
    // The temp sibling is the binding constraint, so measure it — but report the
    // real socket path, which is the thing an operator can shorten.
    let worst = [SOCKET_FILE_NAME, FULL_SOCKET_FILE_NAME]
        .into_iter()
        .map(|name| data_dir.join(name))
        .max_by_key(|p| tmp_socket_path(p).as_os_str().len())
        .expect("socket name list is non-empty");
    if tmp_socket_path(&worst).as_os_str().len() <= MAX_SOCKET_PATH_LEN {
        return Ok(());
    }
    // Budget for the data dir itself: the limit, less the longest socket file
    // name, its temp suffix, and the path separator joining them.
    let overhead = FULL_SOCKET_FILE_NAME.len() + SOCKET_TMP_SUFFIX.len() + 1;
    let max_data_dir = MAX_SOCKET_PATH_LEN.saturating_sub(overhead);
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "data dir {} is too deep to host a Unix control socket: its socket path {} \
             is {} bytes, and binding needs {} more for the atomic temp sibling, over the \
             {MAX_SOCKET_PATH_LEN}-byte sockaddr_un limit. Use a data dir at most \
             {max_data_dir} bytes long.",
            data_dir.display(),
            worst.display(),
            worst.as_os_str().len(),
            SOCKET_TMP_SUFFIX.len(),
        ),
    ))
}

/// Remove a stale socket file left at `path` by a crashed prior run.
///
/// Only a *socket* file is removed — never a regular file or directory — so a
/// mistyped data dir pointing at real data can't be clobbered. A missing path
/// is fine (nothing to clean). On a single-user, single-node device an existing
/// socket here is always our own stale leftover; removing it lets `bind`
/// succeed instead of failing with `AddrInUse`.
fn remove_stale_socket(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            use std::os::unix::fs::FileTypeExt;
            if meta.file_type().is_socket() {
                std::fs::remove_file(path)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "refusing to remove non-socket file at socket path {}",
                        path.display()
                    ),
                ))
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Whether a *live* node is currently listening on the socket at `path`.
///
/// [`remove_stale_socket`] only inspects the inode TYPE — it cannot tell a
/// crashed run's leftover socket file (safe to remove) from one a live node is
/// still serving (must NOT be removed). A successful `connect()` proves a peer
/// is accepting on the path, i.e. a live owner; `ConnectionRefused` (the inode
/// exists but nothing is accepting) and `NotFound` (no inode) both mean the path
/// is free to claim. This is the single-instance guard at the socket layer: it
/// turns a would-be silent double-bind — two processes serving the same data dir,
/// the inconsistent-writes failure mode — into a clean refusal at bind time.
fn socket_has_live_listener(path: &Path) -> bool {
    use std::os::unix::net::UnixStream;
    UnixStream::connect(path).is_ok()
}

/// An owned, bound Unix-domain control socket with managed lifecycle.
///
/// Binding removes any stale socket first and restricts the file to `0o600`;
/// dropping removes the socket file so the next start can rebind cleanly. The
/// accept loop in [`Self::serve`] uses the bound listener.
#[derive(Debug)]
pub struct UdsSocket {
    path: PathBuf,
    listener: UnixListener,
}

impl UdsSocket {
    /// Bind the node's owner control socket under `data_dir`
    /// (`<data_dir>/folddb.sock`).
    ///
    /// Validates the path length, clears a stale socket, binds a
    /// [`UnixListener`], and tightens the socket file to owner-only. Returns the
    /// owned socket; the file is removed when it is dropped.
    pub fn bind(data_dir: &Path) -> io::Result<Self> {
        Self::bind_at(socket_path(data_dir))
    }

    /// Bind a control socket at an explicit `path`.
    ///
    /// Same lifecycle as [`bind`](Self::bind) — length validation, stale-socket
    /// removal, owner-only (`0o600`) permissions, removal on drop — but at a
    /// caller-chosen path rather than the data-dir-derived one. This is how the
    /// **app data-plane socket** is bound OUTSIDE the node data dir (Option B):
    /// the data dir is denied to a jailed app, so the app socket must live at a
    /// path the launcher controls and the jail leaves reachable. The owner-only
    /// mode still applies — the same-user peer-credential gate is the
    /// authoritative check, and the app's confinement is enforced by its verified
    /// `AccessContext`, not by the socket file's visibility.
    pub fn bind_at(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        validate_socket_path_len(&path)?;
        // Bind at a temp path, tighten to owner-only, then atomically rename
        // into place — so the FINAL socket path never exists at the process
        // umask mode. `UnixListener::bind` creates the inode at `0o777 & ~umask`
        // (commonly 0o755); tightening it with a *separate* `set_permissions`
        // afterward leaves a window where another OS user could `connect()`.
        // The same-user peer-cred gate (I3a) is still the authoritative check,
        // but binding owner-only and revealing the path atomically closes the
        // filesystem window at the source without mutating the process-global
        // umask. A listening AF_UNIX socket survives a rename of its path —
        // the fd, not the path, carries the listening state; the `serve` tests
        // exercise a connect after bind. (sec review 2026-06-15.)
        // Derive the temp name from the REAL target file name, not the hardcoded
        // owner-socket constant: `bind_at` also binds app-runtime sockets, and a
        // constant `folddb.sock.tmp` would mislabel those binds.
        let tmp_path = tmp_socket_path(&path);
        validate_socket_path_len(&tmp_path)?;
        // Single-instance guard: if a live node is already listening here, REFUSE
        // rather than unlink-and-rebind. `remove_stale_socket` only checks the
        // inode type, so without this a second node would silently delete the live
        // socket file and rename its own in — leaving two processes serving the
        // same data dir (the inconsistent-writes failure mode). The desktop app's
        // last-wins takeover stops the prior owner BEFORE binding, so in the happy
        // path the socket is already free; this is the safety net for when that
        // didn't run or didn't complete.
        if socket_has_live_listener(&path) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!(
                    "a live node is already listening on {} — refusing to take over its socket",
                    path.display()
                ),
            ));
        }
        remove_stale_socket(&tmp_path)?;
        remove_stale_socket(&path)?;
        let listener = UnixListener::bind(&tmp_path)?;
        if let Err(e) =
            std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(SOCKET_MODE))
                .and_then(|()| std::fs::rename(&tmp_path, &path))
        {
            // Don't leak the temp socket on a failed tighten/rename.
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
        Ok(Self { path, listener })
    }

    /// Path of the bound socket file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Borrow the underlying listener (for the accept loop).
    pub fn listener(&self) -> &UnixListener {
        &self.listener
    }
}

impl Drop for UdsSocket {
    fn drop(&mut self) {
        // Best-effort: a failed unlink on shutdown is non-fatal — the next
        // `bind` removes a stale socket anyway. We only skip the error.
        let _ = std::fs::remove_file(&self.path);
    }
}

// --- Accept-side peer-credential evaluation (I3a) --------------------------
//
// Builds on the bound listener above: for each accepted connection the node
// reads the kernel-set peer credential and applies the same-user gate
// (`fold_db::access`), turning a raw connection into a verdict the access
// boundary can act on. This is the *decision* layer of the accept loop. Two
// further steps build on it:
//   - the loop that spawns on the listener and dispatches accepted connections
//     into the HTTP handler stack, threading the posture below into each
//     request's `AccessContext`, and
//   - the macOS code-signature check (I3b / B4) that promotes an accepted
//     connection from `Unverified` to `CodeSignatureVerified`.
// Until that code-signature check runs, an accepted same-user peer is
// `UnixSocket` transport but still `Unverified` — the UDS transport establishes
// *same-user*, never *app identity*.

use fold_db::access::{AuditToken, CallerHandle, CallerTransport, CallerVerification};

/// Verdict for a connection accepted on the control socket (invariant **I3a**).
///
/// Carries only numeric ids (uid/pid) and the opaque audit-token words — never
/// any namespace data — so logging or auditing a verdict cannot leak
/// isolated-namespace content (the I4 concern). It mirrors the pure
/// [`PeerCredVerdict`](fold_db::access::PeerCredVerdict) one transport layer up,
/// adding the access posture the accept loop attaches to a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdsConnVerdict {
    /// The peer runs as the node owner's OS user. `pid` is the legacy handle and
    /// `token` the kernel audit token (macOS) the I3b code-signature check
    /// prefers — its `pidversion` invalidates on PID reuse (blocker **B3**).
    /// Both `None` when the platform reported neither, which I3b treats as
    /// unverifiable. The connection may be dispatched, but only as `Unverified`
    /// until the code-signature check runs.
    Accepted {
        pid: Option<i32>,
        token: Option<AuditToken>,
    },
    /// The peer runs as a *different* OS user. The connection must be dropped —
    /// never dispatched — and a code-signature check is never attempted.
    Rejected { peer_uid: u32, owner_uid: u32 },
}

impl UdsConnVerdict {
    /// The strongest handle to hand the I3b code-signature check — the audit
    /// token when the platform reported one (blocker **B3**: its `pidversion`
    /// invalidates on PID reuse), else the pid. `None` for a rejected
    /// connection, or an accepted one with neither reported.
    pub fn candidate_handle(&self) -> Option<CallerHandle> {
        match self {
            Self::Accepted { pid, token } => CallerHandle::best(*pid, *token),
            Self::Rejected { .. } => None,
        }
    }

    /// The transport + verification this connection contributes to an
    /// [`AccessContext`](fold_db::access::AccessContext).
    ///
    /// An accepted same-user peer is [`CallerTransport::UnixSocket`] but remains
    /// [`CallerVerification::Unverified`] — the UDS transport establishes
    /// *same-user*, not *app identity*; only the I3b code-signature check
    /// upgrades it to `CodeSignatureVerified`. A rejected connection yields
    /// `None`: it must be dropped, never turned into a request context.
    pub fn access_posture(&self) -> Option<(CallerTransport, CallerVerification)> {
        match self {
            Self::Accepted { .. } => {
                Some((CallerTransport::UnixSocket, CallerVerification::Unverified))
            }
            Self::Rejected { .. } => None,
        }
    }
}

/// Read the peer credential off a connected control-socket stream and apply the
/// same-user gate against `owner_uid` (invariant **I3a**).
///
/// `owner_uid` is the uid the node process runs as. A peer with a matching uid
/// is [`UdsConnVerdict::Accepted`] (carrying its pid for the downstream I3b
/// check); any other uid is [`UdsConnVerdict::Rejected`]. Errors only if the
/// underlying `getsockopt` peer-credential read fails.
///
/// Limited to the platforms whose peer-credential FFI exists
/// (`fold_db::access::read_peer_credential` is Linux/macOS only).
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn evaluate_connection(
    stream: &std::os::unix::net::UnixStream,
    owner_uid: u32,
) -> io::Result<UdsConnVerdict> {
    use fold_db::access::peer_cred::read_peer_credential;
    use fold_db::access::PeerCredVerdict;
    use std::os::unix::io::AsRawFd;

    let cred = read_peer_credential(stream.as_raw_fd())?;
    Ok(match cred.evaluate(owner_uid) {
        PeerCredVerdict::SameUser { pid, token } => UdsConnVerdict::Accepted { pid, token },
        PeerCredVerdict::ForeignUser {
            peer_uid,
            owner_uid,
        } => UdsConnVerdict::Rejected {
            peer_uid,
            owner_uid,
        },
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl UdsSocket {
    /// Accept one pending connection and evaluate its peer credential against
    /// `owner_uid` (the node's own uid).
    ///
    /// Returns the accepted stream paired with its [`UdsConnVerdict`]. The
    /// accept loop drops the stream on a [`UdsConnVerdict::Rejected`] verdict
    /// and dispatches it — as the posture from
    /// [`UdsConnVerdict::access_posture`] — on an [`UdsConnVerdict::Accepted`]
    /// one. Blocks until a connection arrives.
    pub fn accept_and_evaluate(
        &self,
        owner_uid: u32,
    ) -> io::Result<(std::os::unix::net::UnixStream, UdsConnVerdict)> {
        let (stream, _addr) = self.listener.accept()?;
        let verdict = evaluate_connection(&stream, owner_uid)?;
        Ok((stream, verdict))
    }
}

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
                    match report {
                        FdExhaustionLog::Onset => tracing::error!(
                            error = %e,
                            backoff_ms = millis_for_log(backoff),
                            "control-socket accept hit fd exhaustion; backing off and keeping listener alive"
                        ),
                        FdExhaustionLog::Continuing { attempts, elapsed } => tracing::warn!(
                            error = %e,
                            failed_accepts = attempts,
                            elapsed_ms = millis_for_log(elapsed),
                            backoff_ms = millis_for_log(backoff),
                            "control-socket accept still out of file descriptors; listener alive"
                        ),
                        FdExhaustionLog::Suppressed => {}
                    }
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
                        // Rejected: closing the stream is the drop; log numeric
                        // ids only so the audit trail can't leak namespace data.
                        if let UdsConnVerdict::Rejected {
                            peer_uid,
                            owner_uid,
                        } = verdict
                        {
                            tracing::warn!(
                                peer_uid,
                                owner_uid,
                                "dropping control-socket connection from a foreign OS user"
                            );
                        }
                        drop(stream);
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
