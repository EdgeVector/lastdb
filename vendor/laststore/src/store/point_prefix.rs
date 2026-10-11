//! At most five value bytes identify the outer value codec before allocation.

use super::*;
use std::io::{Read, Seek, SeekFrom};

pub(super) fn may_inflate(shard: &Shard, id: &str, location: Loc, size: u64) -> Result<bool> {
    let take = usize::try_from(size.min(5)).expect("five bytes");
    let mut prefix = [0; 5];
    match location {
        Loc::Sorted {
            segment, offset, ..
        } => {
            let segment = shard
                .sorted_segments
                .get(segment)
                .ok_or_else(|| Error::Corrupt("missing metadata segment".into()))?;
            segment
                .cursor(shard.data_key.as_ref(), b"")?
                .read_into(offset, &mut prefix[..take])?;
        }
        Loc::Legacy { seg, offset, .. } => {
            if let Some(bytes) = shard.values.get(id) {
                prefix[..take].copy_from_slice(&bytes[..take]);
            } else if let Some(start) = offset
                .checked_sub(shard.open_buf_base)
                .and_then(|start| usize::try_from(start).ok())
                .filter(|_| shard.segments.last() == Some(&seg))
            {
                let start = start
                    .checked_add(7 + id.len())
                    .ok_or_else(|| Error::Corrupt("metadata prefix overflows".into()))?;
                if let Some(bytes) = shard.open_buf.get(start..start + take) {
                    prefix[..take].copy_from_slice(bytes);
                } else {
                    return read_legacy(&shard.dir, seg, offset, id, take);
                }
            } else {
                return read_legacy(&shard.dir, seg, offset, id, take);
            }
        }
        // Legacy encrypted chunks use a bounded frame decoder. Without an
        // outer-codec prefix, reserve the existing inflate ceiling.
        Loc::Chunk { .. } => return Ok(true),
    }
    Ok(compressed(&prefix))
}

fn read_legacy(directory: &Path, seg: u64, offset: u64, id: &str, take: usize) -> Result<bool> {
    let mut file = File::open(directory.join(format!("{seg:010}.seg")))?;
    file.seek(SeekFrom::Start(
        offset
            .checked_add(7 + id.len() as u64)
            .ok_or_else(|| Error::Corrupt("metadata prefix offset overflows".into()))?,
    ))?;
    let mut prefix = [0; 5];
    file.read_exact(&mut prefix[..take])?;
    Ok(compressed(&prefix))
}

fn compressed(prefix: &[u8; 5]) -> bool {
    prefix.starts_with(b"ENZ:") || (prefix.starts_with(b"ENB:") && prefix[4] & 1 != 0)
}
