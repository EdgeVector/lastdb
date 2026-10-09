use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("docstore corrupt: {0}")]
    Corrupt(String),
    #[error("docstore manifest: {0}")]
    Manifest(String),
    /// AEAD authentication failed while decrypting a frame.
    ///
    /// Deliberately **not** folded into [`Error::Corrupt`]. Corruption means the
    /// on-disk bytes are truncated or inconsistent; an AEAD failure means the
    /// framing was intact enough to reach the decrypt step and the tag did not
    /// verify — i.e. the wrong key, or the bytes were altered. Those demand
    /// opposite operator responses (re-check key provenance vs. restore from
    /// backup), and collapsing them is how a wrong-key event gets misdiagnosed
    /// as data loss.
    #[error("aead authentication failed")]
    AeadAuthFail,
    /// A sealed encrypted chunk failed verification and was moved aside by the
    /// store.
    ///
    /// `chunk_uuid` and `path` are kept as fields rather than formatted into a
    /// string: they are the only handle an operator has on the suspect bytes,
    /// and a caller that wants to report or re-fetch that chunk should not have
    /// to parse a message to find them.
    #[error("chunk {chunk_uuid} quarantined at {}", path.display())]
    ChunkQuarantined {
        /// Chunk UUID that failed verification.
        chunk_uuid: uuid::Uuid,
        /// Quarantine path holding the suspect bytes.
        path: PathBuf,
    },
    /// A product read did not name one partition.
    ///
    /// This is a caller shape error, not corruption. Keep it distinct so a
    /// missing `\0` never looks like damaged bytes or a config/manifest
    /// mistake. Key bytes stay out of the variant; only the operation name
    /// and bound length travel with the error.
    #[error(
        "unanchored read: {operation} requires one partition (bound length {prefix_bytes} bytes)"
    )]
    UnanchoredRead {
        /// The rejected read operation; never caller-provided key text.
        operation: &'static str,
        /// Length of the rejected bound, without retaining its contents.
        prefix_bytes: usize,
    },
}

impl From<laststore::Error> for Error {
    /// Deliberately exhaustive: there is no `_` arm, and there must not be one.
    ///
    /// A wildcard is what would have let `AeadAuthFail` and `ChunkQuarantined`
    /// be added to `laststore::Error` without anyone noticing this mapping
    /// needed them — it would have silently folded two integrity failures into
    /// whatever bucket it named. `laststore::Error` is not `#[non_exhaustive]`,
    /// so listing every variant makes the compiler the gate: the next variant
    /// added upstream breaks this build until someone decides what it means to
    /// a docstore caller.
    fn from(e: laststore::Error) -> Self {
        match e {
            laststore::Error::Io(err) => Self::Io(err),
            laststore::Error::Corrupt(msg) => Self::Corrupt(msg),
            laststore::Error::Config(msg) => Self::Manifest(msg),
            laststore::Error::AeadAuthFail => Self::AeadAuthFail,
            laststore::Error::ChunkQuarantined { chunk_uuid, path } => {
                Self::ChunkQuarantined { chunk_uuid, path }
            }
            laststore::Error::UnanchoredRead {
                operation,
                prefix_bytes,
            } => Self::UnanchoredRead {
                operation,
                prefix_bytes,
            },
            // A refused cold-group load is a store-side resource decision, not
            // corruption: the bytes on disk are fine, the process declined to
            // read them whole. To a docstore caller it is a config/layout
            // condition an operator resolves (drop or compact the group), so it
            // maps like `Config` rather than `Corrupt`.
            laststore::Error::ColdGroupTooLarge {
                collection,
                shard,
                group,
                bytes,
                cap,
            } => Self::Manifest(format!(
                "cold group too large to load: {collection} shard {shard} group {group} \
                 is {bytes} bytes, cap {cap}"
            )),
        }
    }
}
