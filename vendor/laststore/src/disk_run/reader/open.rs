//! Adjacent footer and top bytes share one bounded read on ordinary opens.

use super::*;
use std::path::Path;

const TAIL_BYTES: usize = 512;

impl RunReader {
    /// Opens a run and checks its footer and top block. The file must be
    /// named `<run id>.ldrun` for the run id its footer and blocks carry, so
    /// a valid older run copied over another name does not open.
    pub fn open(path: &Path, data_key: &[u8; 32]) -> Result<Self> {
        Self::open_inner(path, data_key, None, 0)
    }

    /// Open metadata under a caller's total decode-memory admission.
    /// Bounded readers retain their exact footer read and decode admission.
    pub fn open_bounded(path: &Path, data_key: &[u8; 32], bytes: u64) -> Result<Self> {
        Self::open_inner(path, data_key, Some(bytes), 0)
    }
    pub(crate) fn open_bounded_with_reserve(
        path: &Path,
        data_key: &[u8; 32],
        bytes: u64,
        reserved: u64,
    ) -> Result<Self> {
        Self::open_inner(path, data_key, Some(bytes), reserved)
    }

    fn open_inner(
        path: &Path,
        data_key: &[u8; 32],
        bytes: Option<u64>,
        reserved: u64,
    ) -> Result<Self> {
        let named = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(RUN_SUFFIX))
            .and_then(|stem| Uuid::parse_str(stem).ok());
        let Some(named) = named else {
            return Err(corrupt("run file name is not <run id>.ldrun"));
        };
        let file = measured::open(path)?;
        let len = file.metadata()?.len();
        if len < FOOTER_BYTES as u64 {
            return Err(corrupt("file shorter than a footer"));
        }
        let _class = ReadClass::Footer.enter();
        let (footer, tail) = read_footer(&file, len, bytes.is_none())?;
        let _tail_charge = tail
            .as_ref()
            .map(|tail| crate::read_receipt::TemporaryBytes::new(tail.capacity()));
        if bytes.is_some_and(|n| u64::from(footer.top_len).saturating_mul(4) > n) {
            return Err(crate::Error::ReadBudgetExceeded);
        }
        if footer.run_id != named {
            return Err(corrupt("run file name and run id disagree"));
        }
        let end = footer
            .top_offset
            .checked_add(u64::from(footer.top_len))
            .and_then(|end| end.checked_add(FOOTER_BYTES as u64));
        if end != Some(len) {
            return Err(corrupt("top block is not before the footer"));
        }
        let mut reader = Self {
            reserved_memory: reserved,
            file,
            data_key: *data_key,
            // Bounded backup readers retain their original allocation path.
            frame_key: if bytes.is_none() {
                Some(Box::new(PreparedFrameKey::new(data_key, footer.run_id)?))
            } else {
                None
            },
            footer,
            top: Top {
                records: 0,
                data_blocks: 0,
                max_payload: 0,
                indexes: Vec::new(),
                last_key: Vec::new(),
            },
        };
        reader.top = reader.read_top(bytes, tail.as_deref(), len)?;
        if !reader.top.agrees_with(&footer) {
            return Err(corrupt("footer and top block disagree"));
        }
        Ok(reader)
    }
    fn read_top(&self, admission: Option<u64>, tail: Option<&[u8]>, len: u64) -> Result<Top> {
        let footer = self.footer;
        let payload = match admission {
            Some(admission) => self.read_metadata_frame(
                footer.top_offset,
                footer.top_len,
                TOP_COUNTER,
                MAX_TOP_BYTES,
                admission,
            )?,
            None => match cached_top(tail, len, &footer) {
                Some(frame) => {
                    crate::read_receipt::update(|r| {
                        r.coalesced_metadata_reads = r.coalesced_metadata_reads.saturating_add(1);
                    });
                    self.decode_checked_frame(frame, TOP_COUNTER, MAX_TOP_BYTES, None)?
                }
                None => self.read_frame(
                    footer.top_offset,
                    footer.top_len,
                    TOP_COUNTER,
                    MAX_TOP_BYTES,
                )?,
            },
        };
        match admission {
            Some(admission) => Top::decode_bounded(&payload, admission),
            None => Top::decode(&payload),
        }
    }
}

fn read_footer(file: &MeasuredFile, len: u64, ordinary: bool) -> Result<(Footer, Option<Vec<u8>>)> {
    let mut raw = [0u8; FOOTER_BYTES];
    let tail = if ordinary {
        let count = len.min(TAIL_BYTES as u64) as usize;
        let mut tail = vec![0u8; count];
        measured::read_exact_at(file, &mut tail, len - count as u64)?;
        raw.copy_from_slice(&tail[count - FOOTER_BYTES..]);
        Some(tail)
    } else {
        measured::read_exact_at(file, &mut raw, len - FOOTER_BYTES as u64)?;
        None
    };
    Ok((Footer::decode(&raw)?, tail))
}

fn cached_top<'a>(tail: Option<&'a [u8]>, len: u64, footer: &Footer) -> Option<&'a [u8]> {
    let tail = tail?;
    let start = len.checked_sub(tail.len() as u64)?;
    let offset = usize::try_from(footer.top_offset.checked_sub(start)?).ok()?;
    let end = offset.checked_add(footer.top_len as usize)?;
    tail.get(offset..end)
}
