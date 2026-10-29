//! Count JSON bytes before allocating a canonical event buffer.

use super::*;
use std::io::{self, Write};

struct Counter {
    bytes: usize,
    exceeded: bool,
}

impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(total) = self
            .bytes
            .checked_add(bytes.len())
            .filter(|total| *total <= MAX_EVENT_BYTES - 2)
        else {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "capture byte cap",
            ));
        };
        self.bytes = total;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn serialized_size<T: Serialize + ?Sized>(value: &T) -> StorageResult<usize> {
    let mut count = Counter {
        bytes: 0,
        exceeded: false,
    };
    if serde_json::to_writer(&mut count, value).is_err() {
        return Err(if count.exceeded {
            StorageError::MutationLimit {
                detail: "logical capture event exceeds 1 MiB".into(),
            }
        } else {
            StorageError::SerializationError("cannot serialize logical capture".into())
        });
    }
    Ok(count.bytes)
}

/// A collector must admit each outcome before it retains another owned copy.
pub(crate) fn preflight_outcome_size<T: Serialize + ?Sized>(outcome: &T) -> StorageResult<usize> {
    serialized_size(outcome)
}
