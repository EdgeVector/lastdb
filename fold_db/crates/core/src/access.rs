//! Local caller identity and Unix-domain-socket peer credentials.

use serde::{Deserialize, Serialize};

pub use peer_cred::{PeerCredVerdict, PeerCredential};

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

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct PeerCredential {
        uid: u32,
        pid: Option<i32>,
    }

    impl PeerCredential {
        pub fn evaluate(&self, owner_uid: u32) -> PeerCredVerdict {
            if self.uid == owner_uid {
                PeerCredVerdict::SameUser { pid: self.pid }
            } else {
                PeerCredVerdict::ForeignUser {
                    peer_uid: self.uid,
                    owner_uid,
                }
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case", tag = "verdict")]
    pub enum PeerCredVerdict {
        SameUser { pid: Option<i32> },
        ForeignUser { peer_uid: u32, owner_uid: u32 },
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn getsockopt<T>(
        fd: std::os::unix::io::RawFd,
        level: libc::c_int,
        opt: libc::c_int,
    ) -> std::io::Result<T> {
        let mut val: T = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<T>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                level,
                opt,
                (&raw mut val).cast::<libc::c_void>(),
                &mut len,
            )
        };
        (rc == 0)
            .then_some(val)
            .ok_or_else(std::io::Error::last_os_error)
    }

    #[cfg(target_os = "linux")]
    pub fn read_peer_credential(fd: std::os::unix::io::RawFd) -> std::io::Result<PeerCredential> {
        let cred: libc::ucred = getsockopt(fd, libc::SOL_SOCKET, libc::SO_PEERCRED)?;
        Ok(PeerCredential {
            uid: cred.uid,
            pid: Some(cred.pid),
        })
    }

    #[cfg(target_os = "macos")]
    pub fn read_peer_credential(fd: std::os::unix::io::RawFd) -> std::io::Result<PeerCredential> {
        let cred: libc::xucred = getsockopt(fd, libc::SOL_LOCAL, libc::LOCAL_PEERCRED)?;
        if cred.cr_version != libc::XUCRED_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected xucred version from LOCAL_PEERCRED",
            ));
        }
        let pid = getsockopt::<libc::pid_t>(fd, libc::SOL_LOCAL, libc::LOCAL_PEERPID).ok();
        Ok(PeerCredential {
            uid: cred.cr_uid,
            pid,
        })
    }
}
