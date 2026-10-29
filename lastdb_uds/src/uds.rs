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
/// all of which bit a real run (a copy-on-write proof, 2026-08-03):
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
    /// Same-user peer. Dispatched only as `Unverified` until code-signature check.
    Accepted {
        /// Legacy pid. `None` if unreported; a missing pid and token is unverifiable.
        pid: Option<i32>,
        /// macOS audit token. `pidversion` invalidates on PID reuse (blocker **B3**).
        token: Option<AuditToken>,
    },
    /// Different OS user. Drop the connection; do not attempt a code-signature check.
    Rejected {
        /// OS user id of the connecting peer.
        peer_uid: u32,
        /// OS user id of the node owner.
        owner_uid: u32,
    },
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

mod accept;
