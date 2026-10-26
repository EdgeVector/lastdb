use super::super::{decode_header, FrameHeader, HEADER_SIZE, TAG_SIZE};
use crate::{read_receipt::Timer, Error, Result};
use std::fs::{File, Metadata};
use std::io;
use std::os::unix::{fs::FileExt, fs::MetadataExt};

pub(super) struct Source {
    file: File,
    identity: Metadata,
    pub header: FrameHeader,
    pub header_bytes: [u8; HEADER_SIZE],
    pub tag: [u8; TAG_SIZE],
    pub flags: u8,
    pub offset: u64,
    pub stored_bytes: u64,
}

impl Source {
    pub fn open(file: &File, offset: u64, encoded_bytes: u64) -> Result<Self> {
        let identity = crate::read_receipt_io::status(|| file.metadata())?;
        if !identity.is_file()
            || offset
                .checked_add(encoded_bytes)
                .map_or(true, |end| end > identity.len())
        {
            return Err(Error::Corrupt("streamed frame file extent differs".into()));
        }
        if encoded_bytes < (HEADER_SIZE + TAG_SIZE) as u64 {
            return Err(Error::Corrupt("streamed frame is too short".into()));
        }
        let file = file.try_clone()?;
        let mut header_bytes = [0; HEADER_SIZE];
        read_at(&file, &mut header_bytes, offset, true)?;
        let (header, stored_bytes, flags) = decode_header(&header_bytes)?;
        let stored_bytes = stored_bytes as u64;
        if stored_bytes.checked_add((HEADER_SIZE + TAG_SIZE) as u64) != Some(encoded_bytes) {
            return Err(Error::Corrupt("streamed frame length differs".into()));
        }
        let mut tag = [0; TAG_SIZE];
        read_at(
            &file,
            &mut tag,
            offset + HEADER_SIZE as u64 + stored_bytes,
            true,
        )?;
        let source = Self {
            file,
            identity,
            header,
            header_bytes,
            tag,
            flags,
            offset,
            stored_bytes,
        };
        source.check()?;
        Ok(source)
    }

    pub fn check(&self) -> Result<()> {
        let current = crate::read_receipt_io::status(|| self.file.metadata())?;
        // A retained original can lose its path/link during compaction. Its bytes cannot change.
        if !current.is_file()
            || current.dev() != self.identity.dev()
            || current.ino() != self.identity.ino()
            || current.len() != self.identity.len()
            || current.mtime() != self.identity.mtime()
            || current.mtime_nsec() != self.identity.mtime_nsec()
        {
            return Err(Error::Corrupt("streamed frame file changed".into()));
        }
        Ok(())
    }

    pub fn read_payload(&self, bytes: &mut [u8], position: u64) -> Result<()> {
        self.check()?;
        let offset = self
            .offset
            .checked_add(HEADER_SIZE as u64)
            .and_then(|start| start.checked_add(position))
            .ok_or_else(|| Error::Corrupt("streamed frame offset overflow".into()))?;
        read_at(&self.file, bytes, offset, false)?;
        self.check()
    }
}

fn read_at(file: &File, mut bytes: &mut [u8], mut offset: u64, probe: bool) -> io::Result<()> {
    while !bytes.is_empty() {
        let timer = Timer::start();
        let result = file.read_at(bytes, offset);
        timer.finish(|receipt, nanos| {
            receipt.read_operations = receipt.read_operations.saturating_add(1);
            receipt.read_nanos = receipt.read_nanos.saturating_add(nanos);
            let count = result.as_ref().copied().unwrap_or(0) as u64;
            let target = if probe {
                &mut receipt.format_probe_bytes
            } else {
                &mut receipt.payload_bytes
            };
            *target = target.saturating_add(count);
        });
        let count = match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "streamed frame is truncated",
            ));
        }
        offset = offset.checked_add(count as u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "streamed frame offset overflow")
        })?;
        bytes = &mut bytes[count..];
    }
    Ok(())
}
