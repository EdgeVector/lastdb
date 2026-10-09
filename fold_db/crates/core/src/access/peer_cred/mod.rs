//! Peer credentials for the Unix-domain-socket transport.
//!
//! The local trust boundary is the OS user plus one-time app consent. A
//! Unix-domain socket can report kernel-set peer credentials (uid/pid, read via
//! `getsockopt`), which the node uses for same-user owner-socket admission and
//! audit metadata.
//!
//! **Scope — do NOT read this as "verified identity on *every* read path"**
//! (a previous version of this docstring overclaimed exactly that). Verified
//! identity gates a *granted non-owner app*; it does not yet gate *owner*
//! access. A same-uid caller on the UDS control socket is treated as the local
//! owner for the Mini node.
//!
//! This module is the *peer-credential* half (I3a). It does two things:
//!
//! 1. The always-compiled **same-user gate** — the pure security decision. The
//!    node runs as one OS user (FoldDB is single-user, device-local). A socket
//!    peer whose uid matches the owner's is a *candidate* local process; its
//!    audit token (preferred) or `pid` identifies the local process for
//!    diagnostics. A peer with a *different* uid is a foreign user and is
//!    rejected outright. Tokens don't establish this; the kernel-set uid does.
//! 2. The **OS read** — [`read_peer_credential`], the thin
//!    `getsockopt` FFI that produces a [`PeerCredential`] from a connected
//!    socket fd (`SO_PEERCRED` on Linux; `LOCAL_PEERCRED` + `LOCAL_PEERPID` +
//!    `LOCAL_PEERTOKEN` on macOS).
//!
//! The *transport itself* — binding a `UnixListener` alongside the TCP listener
//! and threading the resulting [`PeerCredential`] into the access boundary — is
//! the integration step (B3-wire) and is deliberately **not** in this module.
//! Here we establish only the credential type, the decision, and the read.

use serde::{Deserialize, Serialize};

/// The kernel **audit token** of a socket peer (invariant **I3b**, blocker
/// **B3**) — the macOS `audit_token_t`, eight opaque `u32` words.
///
/// Unlike a bare pid, the audit token embeds a **`pidversion`** that the kernel
/// bumps on every `exec`, so a token captured at `accept` is permanently bound
/// to the *exact* process image that opened the socket. Resolving the
/// process identity from this token closes the PID-reuse TOCTOU: if the
/// original peer `exec`s another binary after the peer-credential read, the
/// stale token no longer resolves to the process now holding the pid.
///
/// This type is a **pure data carrier** with no FFI: the words are filled by
/// the `getsockopt(LOCAL_PEERTOKEN)` read on macOS (the only platform with an
/// audit token). CI (Linux) carries `None` everywhere and exercises the
/// plumbing — extracting, threading, and round-tripping the token — without the
/// macOS FFI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuditToken {
    /// The eight `u32` words of the macOS `audit_token_t`, verbatim.
    words: [u32; 8],
}

impl AuditToken {
    /// Build an audit token from the eight raw kernel words.
    pub fn from_words(words: [u32; 8]) -> Self {
        Self { words }
    }

    /// The eight raw words, for re-materializing an `audit_token_t` at the FFI
    /// boundary.
    pub fn words(&self) -> [u32; 8] {
        self.words
    }
}

/// Process handle captured from a same-user UDS peer.
///
/// The audit token is preferred when available because it remains bound to the
/// accepted process image across pid reuse. The pid fallback is retained for
/// platforms that do not expose audit tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CallerHandle {
    Pid(i32),
    Token(AuditToken),
}

impl CallerHandle {
    pub fn best(pid: Option<i32>, token: Option<AuditToken>) -> Option<Self> {
        token.map(Self::Token).or_else(|| pid.map(Self::Pid))
    }
}

/// OS peer credentials read from a connected Unix-domain socket.
///
/// The kernel fills these in for the process on the other end of the socket;
/// the peer cannot forge them. `uid` is always present; `gid`, `pid`, and the
/// audit `token` are optional because the platforms differ in what they report
/// (`SO_PEERCRED` gives uid/gid/pid on Linux and no audit token; macOS
/// `LOCAL_PEERCRED` gives uid + groups, `LOCAL_PEERPID` the pid, and
/// `LOCAL_PEERTOKEN` the audit token — each a separate, best-effort lookup).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerCredential {
    /// Effective uid of the peer process. The basis of the same-user gate.
    uid: u32,
    /// Primary gid of the peer process, when the platform reports it.
    gid: Option<u32>,
    /// Pid of the peer process, when the platform reports it.
    pid: Option<i32>,
    /// Kernel audit token of the peer (macOS `LOCAL_PEERTOKEN`), when the
    /// platform reports one. Preferred over `pid`: its embedded `pidversion`
    /// invalidates on PID reuse. `None` on Linux and on platforms without the
    /// audit-token read.
    token: Option<AuditToken>,
}

impl PeerCredential {
    /// Construct a peer credential from raw kernel-reported fields, with no
    /// audit token (the Linux shape, and the pre-B3 macOS shape).
    ///
    /// This is the seam between the (untestable) `getsockopt` FFI and the pure,
    /// unit-tested decision logic: the FFI's only job is to fill these fields,
    /// and every decision is taken on the resulting value.
    pub fn new(uid: u32, gid: Option<u32>, pid: Option<i32>) -> Self {
        Self {
            uid,
            gid,
            pid,
            token: None,
        }
    }

    /// Construct a peer credential carrying a kernel audit token (the macOS
    /// shape). The token's `pidversion` invalidates on PID reuse.
    pub fn with_token(
        uid: u32,
        gid: Option<u32>,
        pid: Option<i32>,
        token: Option<AuditToken>,
    ) -> Self {
        Self {
            uid,
            gid,
            pid,
            token,
        }
    }

    /// Effective uid of the peer process.
    pub fn uid(&self) -> u32 {
        self.uid
    }

    /// Primary gid of the peer process, if the platform reported one.
    pub fn gid(&self) -> Option<u32> {
        self.gid
    }

    /// Pid of the peer process, if the platform reported one.
    pub fn pid(&self) -> Option<i32> {
        self.pid
    }

    /// Kernel audit token of the peer, if the platform reported one.
    pub fn token(&self) -> Option<AuditToken> {
        self.token
    }

    /// Apply the **same-user gate** (invariant **I3a**) against the node
    /// owner's uid.
    ///
    /// A peer whose uid equals `owner_uid` is the node owner's own OS user, so
    /// it is a [`PeerCredVerdict::SameUser`] candidate carrying its `pid` for
    /// the downstream code-signature check. Any other uid is a
    /// [`PeerCredVerdict::ForeignUser`] and is rejected here — the node never
    /// attempts code-signature verification for a different OS user.
    pub fn evaluate(&self, owner_uid: u32) -> PeerCredVerdict {
        if self.uid == owner_uid {
            PeerCredVerdict::SameUser {
                pid: self.pid,
                token: self.token,
            }
        } else {
            PeerCredVerdict::ForeignUser {
                peer_uid: self.uid,
                owner_uid,
            }
        }
    }

    /// Convenience: `true` iff the peer is the node owner's OS user.
    pub fn is_same_user(&self, owner_uid: u32) -> bool {
        self.uid == owner_uid
    }
}

/// Best-effort short process name for a peer pid (ops attribution only).
///
/// Returns `None` when the process is gone, the platform cannot resolve a name,
/// or the pid is non-positive. Not a security boundary — labels are diagnostic.
#[must_use]
pub fn process_name(pid: i32) -> Option<String> {
    if pid <= 0 {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        let raw = std::fs::read_to_string(format!("/proc/{pid}/comm")).ok()?;
        let name = raw.trim();
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }
    #[cfg(target_os = "macos")]
    {
        // `proc_name` returns the short executable name (not a full path).
        let mut buf = [0u8; 256];
        let n = unsafe {
            libc::proc_name(
                pid,
                buf.as_mut_ptr().cast::<libc::c_void>(),
                buf.len() as u32,
            )
        };
        if n <= 0 {
            return None;
        }
        let capped = (n as usize).min(buf.len());
        let end = buf[..capped].iter().position(|&b| b == 0).unwrap_or(capped);
        let name = std::str::from_utf8(&buf[..end]).ok()?.trim();
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = pid;
        None
    }
}

/// Outcome of the [`PeerCredential::evaluate`] same-user gate (invariant
/// **I3a**).
///
/// Carries only numeric ids (uid/pid) — never any namespace data — so logging
/// or auditing a verdict cannot leak isolated-namespace content (the I4
/// concern). It is the input to I3b: only a [`Self::SameUser`] with a known
/// `pid` can proceed to a code-signature check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "verdict")]
pub enum PeerCredVerdict {
    /// The peer runs as the node owner's OS user — a candidate for I3b
    /// code-signature verification. `pid` is the process the kernel attributed
    /// the socket to; `token` is its kernel audit token when the platform
    /// reported one (macOS `LOCAL_PEERTOKEN`). The code-signature check prefers
    /// the `token` (its `pidversion` invalidates on PID reuse — blocker **B3**)
    /// and falls back to the `pid` only when no token is present. Both `None`
    /// means the platform reported no handle, which I3b treats as unverifiable.
    SameUser {
        pid: Option<i32>,
        token: Option<AuditToken>,
    },
    /// The peer runs as a *different* OS user. Rejected at the peer-credential
    /// layer; code-signature verification is never attempted.
    ForeignUser { peer_uid: u32, owner_uid: u32 },
}

impl PeerCredVerdict {
    /// `true` iff the peer is the node owner's OS user.
    pub fn is_same_user(&self) -> bool {
        matches!(self, Self::SameUser { .. })
    }

    /// The pid to hand to the I3b code-signature check: `Some` only for a
    /// same-user peer whose pid the kernel reported. A foreign user, or a
    /// same-user peer with no reported pid, yields `None` — there is nothing
    /// safe to verify.
    pub fn candidate_pid(&self) -> Option<i32> {
        match self {
            Self::SameUser { pid, .. } => *pid,
            Self::ForeignUser { .. } => None,
        }
    }

    /// The audit token to hand to the I3b code-signature check (blocker
    /// **B3**): `Some` only for a same-user peer whose platform reported one.
    /// When present it is preferred over [`candidate_pid`](Self::candidate_pid)
    /// — resolving the `SecCode` guest from the token closes the PID-reuse
    /// TOCTOU. A foreign user, or a same-user peer with no reported token,
    /// yields `None`.
    pub fn candidate_token(&self) -> Option<AuditToken> {
        match self {
            Self::SameUser { token, .. } => *token,
            Self::ForeignUser { .. } => None,
        }
    }
}

// --- OS peer-credential read -----------------------------------------------
//
// The actual `getsockopt` FFI produces a [`PeerCredential`]; every decision on
// that credential is the pure, always-compiled logic above.

/// Read kernel-reported peer credentials from a connected Unix-domain socket.
///
/// `fd` must be a connected `AF_UNIX` stream socket. Returns the peer's
/// [`PeerCredential`] (uid always; gid/pid where the platform reports them) or
/// the underlying `getsockopt` error.
///
/// Compiled on all Unix targets: reading a connected socket's kernel peer
/// credential is a generic primitive consumed by the dev node's UDS control
/// socket.
#[cfg(target_os = "linux")]
pub fn read_peer_credential(fd: std::os::unix::io::RawFd) -> std::io::Result<PeerCredential> {
    // Linux: a single SO_PEERCRED read yields pid, uid and gid together.
    let mut cred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: `cred`/`len` are valid for writes of the sizes passed; the kernel
    // writes at most `len` bytes and updates `len` to the bytes written.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast::<libc::c_void>(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(PeerCredential::new(
        cred.uid,
        Some(cred.gid),
        Some(cred.pid),
    ))
}

/// Read kernel-reported peer credentials from a connected Unix-domain socket
/// (macOS). See the Linux variant for contract.
#[cfg(target_os = "macos")]
pub fn read_peer_credential(fd: std::os::unix::io::RawFd) -> std::io::Result<PeerCredential> {
    // macOS: LOCAL_PEERCRED yields uid (+ groups) via `struct xucred`; the pid
    // is a separate, best-effort LOCAL_PEERPID lookup.
    // SAFETY: `xucred` is plain-old-data; zeroing it is a valid initial value.
    let mut cred: libc::xucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::xucred>() as libc::socklen_t;
    // SAFETY: `cred`/`len` are valid for writes of the sizes passed.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERCRED,
            (&mut cred as *mut libc::xucred).cast::<libc::c_void>(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if cred.cr_version != libc::XUCRED_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected xucred version from LOCAL_PEERCRED",
        ));
    }
    // Prefer the audit token (blocker B3): its embedded `pidversion` binds the
    // identity to the exact process image that opened the socket, so a later
    // `exec` cannot make the code-signature check resolve to another binary. The
    // pid is still captured as a best-effort fallback for the diagnostic /
    // legacy path; the verifier uses the token when present.
    let token = read_peer_token(fd).ok();
    let pid = read_peer_pid(fd).ok();
    Ok(PeerCredential::with_token(cred.cr_uid, None, pid, token))
}

/// Best-effort `LOCAL_PEERTOKEN` lookup (macOS only): the peer's kernel
/// `audit_token_t`, captured at the same point as the peer credential so the
/// downstream code-signature check (blocker **B3**) can resolve the `SecCode`
/// guest from a `pidversion`-bearing token instead of a reusable pid. A failure
/// here is non-fatal — the caller records `token = None` and the verifier falls
/// back to the pid (the pre-B3 behavior).
#[cfg(target_os = "macos")]
fn read_peer_token(fd: std::os::unix::io::RawFd) -> std::io::Result<AuditToken> {
    // `audit_token_t` is `struct { unsigned int val[8]; }` — eight u32 words.
    // It is not in the `libc` crate, so read it as a raw [u32; 8] of the exact
    // size the kernel returns via LOCAL_PEERTOKEN.
    const AUDIT_TOKEN_WORDS: usize = 8;
    // LOCAL_PEERTOKEN is defined in <sys/un.h> as 0x006; not surfaced by `libc`.
    const LOCAL_PEERTOKEN: libc::c_int = 0x006;

    let mut words = [0u32; AUDIT_TOKEN_WORDS];
    let mut len = std::mem::size_of::<[u32; AUDIT_TOKEN_WORDS]>() as libc::socklen_t;
    // SAFETY: `words`/`len` are valid for writes of the sizes passed; the kernel
    // writes at most `len` bytes and updates `len` to the bytes written.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            LOCAL_PEERTOKEN,
            words.as_mut_ptr().cast::<libc::c_void>(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if len as usize != std::mem::size_of::<[u32; AUDIT_TOKEN_WORDS]>() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected audit_token_t size from LOCAL_PEERTOKEN",
        ));
    }
    Ok(AuditToken::from_words(words))
}

/// Best-effort LOCAL_PEERPID lookup (macOS only). A failure here is non-fatal:
/// the caller records `pid = None`, which the I3b check treats as unverifiable.
#[cfg(target_os = "macos")]
fn read_peer_pid(fd: std::os::unix::io::RawFd) -> std::io::Result<i32> {
    let mut pid: libc::pid_t = 0;
    let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    // SAFETY: `pid`/`len` are valid for writes of the sizes passed.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast::<libc::c_void>(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(pid)
}
