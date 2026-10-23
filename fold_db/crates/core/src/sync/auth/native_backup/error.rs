use cloud_backup_protocol::ProtocolError;

/// Transport failures never establish that a saved mutation failed remotely.
#[derive(Clone)]
pub enum NativeBackupError {
    Configuration(&'static str),
    Protocol(ProtocolError),
    Authentication,
    Timeout,
    Transport,
    Http(u16),
    InvalidResponse(&'static str),
    Integrity(&'static str),
    UnsupportedFileScope,
    Source,
    Task,
}
impl From<ProtocolError> for NativeBackupError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
impl std::fmt::Display for NativeBackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration(field) => write!(f, "native backup configuration: {field}"),
            Self::Protocol(error) => write!(f, "native backup service error: {:?}", error.code),
            Self::Authentication => f.write_str("native backup authentication refused"),
            Self::Timeout => f.write_str("native backup request deadline expired"),
            Self::Transport => f.write_str("native backup transport failed"),
            Self::Http(status) => write!(f, "native backup HTTP status {status}"),
            Self::InvalidResponse(field) => write!(f, "native backup invalid response: {field}"),
            Self::Integrity(field) => write!(f, "native backup byte integrity: {field}"),
            Self::UnsupportedFileScope => {
                f.write_str("native backup file owner scope needs a read-only source grant")
            }
            Self::Source => f.write_str("native backup source read failed"),
            Self::Task => f.write_str("native backup source task failed"),
        }
    }
}
impl std::fmt::Debug for NativeBackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for NativeBackupError {}
