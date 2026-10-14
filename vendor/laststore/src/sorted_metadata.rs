//! Header-only allocation bounds before opening a sorted footer.

use super::sorted::*;
use crate::{sorted_file, Result};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub(crate) fn opening_bound(path: &Path) -> Result<Option<(bool, u64)>> {
    let _read_class = crate::read_receipt_io::ReadClass::FormatProbe.enter();
    if !Segment::recognizes(path)? {
        return Ok(None);
    }
    let mut file = sorted_file::Input::open(path)?;
    let mut bytes = [0; HEADER_LEN];
    file.read_exact(&mut bytes)?;
    let header = Header::parse(&bytes)?;
    file.seek(SeekFrom::End(-(TRAILER_LEN as i64)))?;
    let mut trailer = [0; TRAILER_LEN];
    file.read_exact(&mut trailer)?;
    let offset = u64::from_le_bytes(trailer[8..16].try_into().unwrap());
    let length = u64::from_le_bytes(trailer[16..24].try_into().unwrap());
    if &trailer[..8] != END_MAGIC
        || length > MAX_FOOTER_BYTES + 128
        || offset
            .checked_add(length)
            .and_then(|n| n.checked_add(TRAILER_LEN as u64))
            != Some(file.len())
    {
        return Err(corrupt("metadata footer bounds are invalid"));
    }
    let mut decoded = length;
    if header.encrypted {
        file.seek(SeekFrom::Start(offset))?;
        let mut frame_header = [0; 48];
        file.read_exact(&mut frame_header)?;
        if frame_header[5] & 1 != 0 {
            decoded = MAX_FOOTER_BYTES;
        }
    }
    let bound = decoded
        .max(length)
        .checked_mul(8)
        .and_then(|n| n.checked_add(4 * 1024 * 1024))
        .ok_or_else(|| corrupt("metadata footer allocation overflows"))?;
    Ok(Some((header.complete, bound)))
}
