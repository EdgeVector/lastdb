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
const CAS_NAME: &str = "CAS_DELETE";
const CAS_MAGIC: &[u8; 8] = b"LDRCAS1\0";
/// Maximum encrypted cross-home deletion intent payload.
pub const MAX_CAS_DELETE_CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;

impl RunHome {
    /// Read the node's private cleanup checkpoint. This is not a data record
    /// or a snapshot selector; the authenticated payload binds it to this home.
    pub fn load_cleanup_checkpoint(&self) -> Result<Option<Vec<u8>>> {
        self.load_checkpoint(NAME, MAGIC, MAX_BYTES)
    }

    /// Read the authenticated cross-home deletion intent, if one remains.
    pub fn load_cas_delete_checkpoint(&self) -> Result<Option<Vec<u8>>> {
        self.load_checkpoint(CAS_NAME, CAS_MAGIC, MAX_CAS_DELETE_CHECKPOINT_BYTES)
    }

    fn load_checkpoint(
        &self,
        name: &str,
        magic: &[u8; 8],
        maximum: usize,
    ) -> Result<Option<Vec<u8>>> {
        let mut file = match OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(self.root.join(name))
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.mode() & 0o077 != 0
            || meta.nlink() != 1
            || meta.uid() != rustix::process::geteuid().as_raw()
            || meta.len() > (maximum + 4096) as u64
        {
            return Err(invalid("invalid cleanup checkpoint file"));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((maximum + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let frame = decode_frame_bounded(&self.key, &bytes, maximum + 24)?;
        if frame.payload.get(..8) != Some(magic.as_slice())
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
        self.save_checkpoint(NAME, MAGIC, MAX_BYTES, bytes)
    }

    /// Save intent before either home changes. No body or encryption key enters it.
    pub fn save_cas_delete_checkpoint(&self, bytes: &[u8]) -> Result<()> {
        self.save_checkpoint(CAS_NAME, CAS_MAGIC, MAX_CAS_DELETE_CHECKPOINT_BYTES, bytes)
    }

    /// Remove only the authenticated intent after its exact frontier completes.
    pub fn clear_cas_delete_checkpoint(&self) -> Result<()> {
        if self.load_cas_delete_checkpoint()?.is_some() {
            fs::remove_file(self.root.join(CAS_NAME))?;
            sync_directory(&File::open(&self.root)?)?;
        }
        Ok(())
    }

    fn save_checkpoint(
        &self,
        name: &str,
        magic: &[u8; 8],
        maximum: usize,
        bytes: &[u8],
    ) -> Result<()> {
        if bytes.len() > maximum {
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
        let mut payload = magic.to_vec();
        payload.extend_from_slice(self.identity.as_bytes());
        payload.extend_from_slice(bytes);
        let frame = encode_frame(&self.key, header, &payload)?;
        let pending = self.root.join(format!(".{name}.{}.tmp", header.chunk_uuid));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&pending)?;
            file.write_all(&frame)?;
            sync_dirty_file(&file)?;
            fs::rename(&pending, self.root.join(name))?;
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
