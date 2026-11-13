//! Unified FoldDB error type, caller peer credentials, content limits, and manifest retention.

use crate::schema::types::SchemaError;
use std::{fmt, io};

#[derive(Debug)]
pub enum FoldDbError {
    Schema(SchemaError),
    Database(String),
    Config(String),
    Io(io::Error),
    Serialization(String),
    SecurityError(String),
    InProgress(String),
}

impl fmt::Display for FoldDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(err) => write!(f, "Schema error: {err}"),
            Self::Database(msg) => write!(f, "Database error: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::SecurityError(msg) => write!(f, "Security error: {msg}"),
            Self::InProgress(msg) => write!(f, "operation in progress: {msg}"),
        }
    }
}

impl std::error::Error for FoldDbError {}

impl From<SchemaError> for FoldDbError {
    fn from(e: SchemaError) -> Self {
        Self::Schema(e)
    }
}

impl From<io::Error> for FoldDbError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for FoldDbError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

impl From<crate::storage::StorageError> for FoldDbError {
    fn from(error: crate::storage::StorageError) -> Self {
        match error {
            crate::storage::StorageError::MutationLimit { detail } => {
                Self::Schema(SchemaError::WriteResourceLimit { detail })
            }
            other => Self::Database(other.to_string()),
        }
    }
}

pub type FoldDbResult<T> = Result<T, FoldDbError>;

pub mod access {
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
        pub fn read_peer_credential(
            fd: std::os::unix::io::RawFd,
        ) -> std::io::Result<PeerCredential> {
            let cred: libc::ucred = getsockopt(fd, libc::SOL_SOCKET, libc::SO_PEERCRED)?;
            Ok(PeerCredential {
                uid: cred.uid,
                pid: Some(cred.pid),
            })
        }

        #[cfg(target_os = "macos")]
        pub fn read_peer_credential(
            fd: std::os::unix::io::RawFd,
        ) -> std::io::Result<PeerCredential> {
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
}

pub mod content_limit {
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub use lastdb_model::content_limit::{
        ABSOLUTE_MAX_FIELD_VALUE_BYTES, DEFAULT_MAX_FIELD_VALUE_BYTES, MAX_FIELD_VALUE_BYTES_ENV,
        MIN_MAX_FIELD_VALUE_BYTES,
    };

    static EFFECTIVE_LIMIT: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FIELD_VALUE_BYTES);

    pub fn set_max_field_value_bytes(limit: usize) {
        let prev = EFFECTIVE_LIMIT.swap(limit, Ordering::AcqRel);
        if prev != limit && limit != DEFAULT_MAX_FIELD_VALUE_BYTES {
            tracing::info!(
                target: "fold_db::content_limit",
                limit,
                default = DEFAULT_MAX_FIELD_VALUE_BYTES,
                absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES,
                env = MAX_FIELD_VALUE_BYTES_ENV,
                "peer field value size limit overridden from env"
            );
        }
    }

    #[must_use]
    pub fn max_field_value_bytes() -> usize {
        EFFECTIVE_LIMIT.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn parse_max_field_value_bytes(raw: Option<String>) -> usize {
        let Some(raw) = raw else {
            return DEFAULT_MAX_FIELD_VALUE_BYTES;
        };
        let s = raw.trim();
        if s.is_empty() {
            return DEFAULT_MAX_FIELD_VALUE_BYTES;
        }
        let lower = s.to_ascii_lowercase();
        let (num, mult) = [
            ("kib", 1024usize),
            ("mib", 1024 * 1024),
            ("kb", 1024),
            ("mb", 1024 * 1024),
            ("k", 1024),
            ("m", 1024 * 1024),
        ]
        .into_iter()
        .find_map(|(u, m)| lower.strip_suffix(u).map(|r| (r, m)))
        .unwrap_or((lower.as_str(), 1));
        let Some(parsed) = num
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|n| n.checked_mul(mult))
        else {
            tracing::warn!(
                raw = %s, env = MAX_FIELD_VALUE_BYTES_ENV,
                "invalid LASTDB_MAX_FIELD_VALUE_BYTES; using default {}",
                DEFAULT_MAX_FIELD_VALUE_BYTES
            );
            return DEFAULT_MAX_FIELD_VALUE_BYTES;
        };
        if parsed < MIN_MAX_FIELD_VALUE_BYTES {
            tracing::warn!(
                requested = parsed,
                min = MIN_MAX_FIELD_VALUE_BYTES,
                env = MAX_FIELD_VALUE_BYTES_ENV,
                "field value limit below minimum; clamping"
            );
            MIN_MAX_FIELD_VALUE_BYTES
        } else if parsed > ABSOLUTE_MAX_FIELD_VALUE_BYTES {
            tracing::warn!(
                requested = parsed, absolute_max = ABSOLUTE_MAX_FIELD_VALUE_BYTES, env = MAX_FIELD_VALUE_BYTES_ENV,
                "field value limit above absolute max; clamping (use the record's file for larger payloads)"
            );
            ABSOLUTE_MAX_FIELD_VALUE_BYTES
        } else {
            parsed
        }
    }
}

pub mod manifest_retention {
    use crate::schema::types::schema_types::SchemaRetentionPolicy;
    use serde::{Deserialize, Serialize};

    const SECONDS_PER_DAY: u64 = 86_400;
    const MAX_MANIFEST_TTL_SECONDS: u64 = 3650 * SECONDS_PER_DAY;
    const MIN_MANIFEST_TTL_SECONDS: u64 = 3600;
    const MIN_MANIFEST_TTL_ENV: &str = "LASTDB_MIN_MANIFEST_TTL_SECONDS";

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ManifestRetention {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub ttl_days: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub ttl_seconds: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub confirm_expire_existing: Option<bool>,
    }

    impl ManifestRetention {
        pub fn confirmed(&self) -> bool {
            self.confirm_expire_existing == Some(true)
        }

        pub fn ttl(&self) -> Result<u64, String> {
            let seconds = match (self.ttl_days, self.ttl_seconds) {
                (Some(d), None) => d.checked_mul(SECONDS_PER_DAY),
                (None, Some(s)) => Some(s),
                (None, None) => return Err("retention needs ttl_days or ttl_seconds".into()),
                (Some(_), Some(_)) => {
                    return Err("retention takes ttl_days or ttl_seconds, not both".into());
                }
            };
            let floor = crate::crypto::env_flag::var_parsed::<u64>(MIN_MANIFEST_TTL_ENV)
                .filter(|&s| s > 0)
                .unwrap_or(MIN_MANIFEST_TTL_SECONDS);
            match seconds {
                Some(0) => Err("retention ttl must be greater than zero".into()),
                Some(s) if s < floor => Err(format!(
                    "retention ttl of {s} seconds is below the floor of {floor} seconds. \
                     A TTL deletes records for good, and a value this small is usually a unit \
                     slip (ttl_seconds: 90 meant as days). Use ttl_days, or set \
                     {MIN_MANIFEST_TTL_ENV} to lower the floor"
                )),
                Some(s) if s <= MAX_MANIFEST_TTL_SECONDS => Ok(s),
                _ => Err(format!(
                    "retention ttl is too large: the limit is {MAX_MANIFEST_TTL_SECONDS} seconds (3650 days)"
                )),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ManifestRetentionStatus {
        Applied,
        Updated,
        Unchanged,
        OperatorOverride,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ManifestRetentionPlan {
        pub anchor: String,
        pub current: Option<SchemaRetentionPolicy>,
        pub next: Option<SchemaRetentionPolicy>,
        pub status: ManifestRetentionStatus,
        pub shortened: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ManifestRetentionOutcome {
        pub anchor: String,
        pub status: ManifestRetentionStatus,
        pub previous: Option<SchemaRetentionPolicy>,
        pub effective: SchemaRetentionPolicy,
        pub shortened: bool,
    }
}
