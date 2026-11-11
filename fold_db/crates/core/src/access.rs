//! Local caller identity and Unix-domain-socket peer credentials.

use serde::{Deserialize, Serialize};

pub use peer_cred::{AuditToken, CallerHandle, PeerCredVerdict, PeerCredential};

/// Request identity context for local callers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessContext {
    pub user_id: String,
    pub is_owner: bool,
}

impl AccessContext {
    pub fn owner(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            is_owner: true,
        }
    }
}

pub mod peer_cred {
    use serde::{Deserialize, Serialize};

    /// Kernel audit token of a macOS socket peer (`audit_token_t`, eight `u32` words).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct AuditToken {
        words: [u32; 8],
    }

    impl AuditToken {
        pub fn from_words(words: [u32; 8]) -> Self {
            Self { words }
        }

        pub fn words(&self) -> [u32; 8] {
            self.words
        }
    }

    /// Process handle captured from a same-user UDS peer.
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
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct PeerCredential {
        uid: u32,
        gid: Option<u32>,
        pid: Option<i32>,
        token: Option<AuditToken>,
    }

    impl PeerCredential {
        pub fn new(uid: u32, gid: Option<u32>, pid: Option<i32>) -> Self {
            Self {
                uid,
                gid,
                pid,
                token: None,
            }
        }

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

        pub fn uid(&self) -> u32 {
            self.uid
        }

        pub fn pid(&self) -> Option<i32> {
            self.pid
        }

        pub fn token(&self) -> Option<AuditToken> {
            self.token
        }

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
    }

    /// Outcome of the [`PeerCredential::evaluate`] same-user gate.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case", tag = "verdict")]
    pub enum PeerCredVerdict {
        SameUser {
            pid: Option<i32>,
            token: Option<AuditToken>,
        },
        ForeignUser {
            peer_uid: u32,
            owner_uid: u32,
        },
    }

    #[cfg(target_os = "linux")]
    pub fn read_peer_credential(fd: std::os::unix::io::RawFd) -> std::io::Result<PeerCredential> {
        let mut cred = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
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

    #[cfg(target_os = "macos")]
    pub fn read_peer_credential(fd: std::os::unix::io::RawFd) -> std::io::Result<PeerCredential> {
        let mut cred: libc::xucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::xucred>() as libc::socklen_t;
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
        let token = read_peer_token(fd).ok();
        let pid = read_peer_pid(fd).ok();
        Ok(PeerCredential::with_token(cred.cr_uid, None, pid, token))
    }

    #[cfg(target_os = "macos")]
    fn read_peer_token(fd: std::os::unix::io::RawFd) -> std::io::Result<AuditToken> {
        const AUDIT_TOKEN_WORDS: usize = 8;
        const LOCAL_PEERTOKEN: libc::c_int = 0x006;
        let mut words = [0u32; AUDIT_TOKEN_WORDS];
        let mut len = std::mem::size_of::<[u32; AUDIT_TOKEN_WORDS]>() as libc::socklen_t;
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

    #[cfg(target_os = "macos")]
    fn read_peer_pid(fd: std::os::unix::io::RawFd) -> std::io::Result<i32> {
        let mut pid: libc::pid_t = 0;
        let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
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
}
