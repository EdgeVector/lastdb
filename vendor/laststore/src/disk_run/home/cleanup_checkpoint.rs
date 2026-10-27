//! One bounded authenticated node-cleanup checkpoint, separate from CURRENT.

use super::RunHome;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{decode_frame_bounded, encode_frame, FrameHeader};
use crate::{Error, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use uuid::Uuid;

const NAME: &str = "NODE_CLEANUP";
const MAGIC: &[u8; 8] = b"LDRCLN1\0";
const MAX_BYTES: usize = 16 * 1024;

impl RunHome {
    /// Read the node's private cleanup checkpoint. This is not a data record
    /// or a snapshot selector; the authenticated payload binds it to this home.
    pub fn load_cleanup_checkpoint(&self) -> Result<Option<Vec<u8>>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(self.root.join(NAME))
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let meta = file.metadata()?;
        if !meta.is_file() || meta.mode() & 0o077 != 0 || meta.len() > (MAX_BYTES + 4096) as u64 {
            return Err(invalid("invalid cleanup checkpoint file"));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((MAX_BYTES + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let frame = decode_frame_bounded(&self.key, &bytes, MAX_BYTES + 24)?;
        if frame.payload.get(..8) != Some(MAGIC.as_slice())
            || frame.payload.get(8..24) != Some(self.identity.as_bytes().as_slice())
        {
            return Err(invalid(
                "cleanup checkpoint belongs to another role or home",
            ));
        }
        Ok(Some(frame.payload[24..].to_vec()))
    }

    /// Durably replace the bounded node-cleanup checkpoint. A fresh frame id
    /// prevents nonce reuse. Failure never changes CURRENT or user records.
    pub fn save_cleanup_checkpoint(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_BYTES {
            return Err(invalid("cleanup checkpoint exceeds its byte bound"));
        }
        let _publish = self
            .publish
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.refuse_if_poisoned()?;
        let header = FrameHeader {
            chunk_uuid: Uuid::new_v4(),
            shard: 0,
            start_csn: 0,
            counter: 0,
        };
        let mut payload = MAGIC.to_vec();
        payload.extend_from_slice(self.identity.as_bytes());
        payload.extend_from_slice(bytes);
        let frame = encode_frame(&self.key, header, &payload)?;
        let pending = self.root.join(format!(".{NAME}.{}.tmp", header.chunk_uuid));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&pending)?;
            file.write_all(&frame)?;
            sync_dirty_file(&file)?;
            fs::rename(&pending, self.root.join(NAME))?;
            sync_directory(&File::open(&self.root)?)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&pending);
        }
        result
    }
}

fn invalid(message: &str) -> Error {
    Error::Corrupt(message.into())
}
