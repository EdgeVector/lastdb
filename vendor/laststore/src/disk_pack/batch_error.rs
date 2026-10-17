//! Preserve typed per-item errors when several records share one failed extent.

use crate::Error;

pub(super) fn copy(error: &Error) -> Error {
    match error {
        Error::Io(error) => Error::Io(match error.raw_os_error() {
            Some(code) => std::io::Error::from_raw_os_error(code),
            None => std::io::Error::new(error.kind(), error.to_string()),
        }),
        Error::Corrupt(message) => Error::Corrupt(message.clone()),
        Error::Config(message) => Error::Config(message.clone()),
        Error::WriteCapacity(message) => Error::WriteCapacity(message.clone()),
        Error::ReadBudgetExceeded => Error::ReadBudgetExceeded,
        Error::AeadAuthFail => Error::AeadAuthFail,
        Error::ChunkQuarantined { chunk_uuid, path } => Error::ChunkQuarantined {
            chunk_uuid: *chunk_uuid,
            path: path.clone(),
        },
        Error::ColdGroupTooLarge {
            collection,
            shard,
            group,
            bytes,
            cap,
        } => Error::ColdGroupTooLarge {
            collection: collection.clone(),
            shard: *shard,
            group: *group,
            bytes: *bytes,
            cap: *cap,
        },
        Error::UnanchoredRead {
            operation,
            prefix_bytes,
        } => Error::UnanchoredRead {
            operation,
            prefix_bytes: *prefix_bytes,
        },
    }
}
