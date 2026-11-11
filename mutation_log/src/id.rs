use crate::error::LogError;

fn checked_label(value: &str, error: LogError) -> Result<String, LogError> {
    if (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Ok(value.to_owned())
    } else {
        Err(error)
    }
}

macro_rules! label_id {
    ($name:ident, $error:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Reject an empty label, a label longer than 64 bytes, or a byte
            /// outside the node database-selector alphabet.
            pub fn new(value: &str) -> Result<Self, LogError> {
                checked_label(value, LogError::$error).map(Self)
            }

            /// Borrow the label.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

label_id!(
    DatabaseId,
    InvalidId,
    "Identity of one database. One log is keyed by this value."
);
label_id!(
    DeviceId,
    InvalidId,
    "Identity of one device of a node key. A 64-character hex label fits."
);
label_id!(
    PrincipalId,
    InvalidId,
    "Node key. The same key is used on every device of that node."
);

/// Opaque pointer to snapshot bytes held by the shell.
///
/// The core stores the pointer and returns it on reset. It does not store
/// the bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotRef(String);

impl SnapshotRef {
    /// Reject an empty pointer or one longer than 256 bytes.
    pub fn new(value: &str) -> Result<Self, LogError> {
        if (1..=256).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_graphic()) {
            Ok(Self(value.to_owned()))
        } else {
            Err(LogError::InvalidId)
        }
    }

    /// Borrow the pointer.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Schema namespace, the part before `/` in a name such as `hearth/PhotoMeta`.
///
/// The schema name stays in the entry payload. This core classifies the
/// namespace only.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace(String);

impl Namespace {
    /// Reject an empty namespace or one that already includes the schema name.
    pub fn new(value: &str) -> Result<Self, LogError> {
        checked_label(value, LogError::InvalidNamespace).map(Self)
    }

    /// Borrow the namespace.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
