//! Local native restore validation and install support for the V2 byte client.
//! No V1 cloud selection, read grant or downloader is admitted here.
mod context;
mod download;
mod install;
mod keys;
mod session;

pub(crate) use context::{
    bounded_bytes, check_capacity, verify_file, NativeFileProof, NativeInventory,
    VerifiedNativeContext,
};
pub use keys::RecoveryKeys;
pub use session::RestoreReport;

/// Static errors never contain keys, record values, provider URLs, or local secrets.
#[derive(Debug)]
pub enum RestoreError {
    Invalid(&'static str),
    Limit(&'static str),
    Unsupported(&'static str),
    Io(std::io::Error),
    Protocol(cloud_backup_protocol::ProtocolError),
    Source(&'static str),
}
pub type Result<T> = std::result::Result<T, RestoreError>;

impl From<std::io::Error> for RestoreError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<cloud_backup_protocol::ProtocolError> for RestoreError {
    fn from(value: cloud_backup_protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
impl std::fmt::Display for RestoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(field) => write!(f, "invalid restore {field}"),
            Self::Limit(field) => write!(f, "restore limit: {field}"),
            Self::Unsupported(field) => write!(f, "unsupported restore: {field}"),
            Self::Io(_) => f.write_str("restore disk operation failed"),
            Self::Protocol(_) => f.write_str("restore protocol validation failed"),
            Self::Source(field) => write!(f, "restore source unavailable: {field}"),
        }
    }
}
impl std::error::Error for RestoreError {}
