use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::{artifact, corrupt, Current, DirectoryEntry, Location, Source, PAGE_BYTES};
use crate::Result;
use std::path::Path;

/// Worst-case uncompressed output, calculated from exact live locator entries.
/// Compression can never reduce this reservation and then enlarge its output beyond it.
/// Stream the selected trees with per-path I/O budgets and checked byte arithmetic.
pub(super) fn replacement_bytes(root: &Path, current: &Current) -> Result<u64> {
    quote(root, current, true)
}

pub(super) fn lookup_replacement_bytes(root: &Path, current: &Current) -> Result<u64> {
    quote(root, current, false)
}

fn quote(root: &Path, current: &Current, copy_payload: bool) -> Result<u64> {
    // Complete receipt, its private publication, empty-tree floors, and checkpoint slack.
    let mut bytes = 8 * 1024 * 1024u64;
    let mut call = artifact::metadata_call(root, current.clone());
    let mut directory = TreeCursor::seek(&mut call, current.directory.clone(), b"")?;
    while let Some((key, value)) = directory.next(&mut call)? {
        let entry = DirectoryEntry::decode(&value)?;
        super::compact::collection(&key, entry.group)?;
        let mut cursor = TreeCursor::seek(&mut call, entry.locator, b"")?;
        // Include directory long-key overflow, initial tree pages, part headers, map tails.
        add(&mut bytes, 128 * 1024 + key.len() as u64)?;
        while let Some((key, value)) = cursor.next(&mut call)? {
            let location = Location::decode_for_current(&value, current)?;
            if location.version.batch > current.frontier {
                return Err(corrupt("compaction quote frontier"));
            }
            add(&mut bytes, locator_bytes(key.len()))?;
            if copy_payload && location.source != Source::Tombstone {
                // Each record can straddle one initial block. Include frames and block map entries.
                let blocks = location.record_bytes.div_ceil(4096) + 1;
                add(&mut bytes, location.record_bytes)?;
                add(
                    &mut bytes,
                    blocks
                        .checked_mul(104)
                        .ok_or_else(|| corrupt("compaction block reserve overflow"))?,
                )?;
            }
            call = artifact::metadata_call(root, current.clone());
        }
        call = artifact::metadata_call(root, current.clone());
    }
    Ok(bytes)
}

fn add(total: &mut u64, bytes: u64) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| corrupt("compaction reserve overflow"))?;
    Ok(())
}

fn locator_bytes(key_bytes: usize) -> u64 {
    let external = key_bytes > crate::disk_pack::key_layout::INLINE_KEY_BYTES;
    let descriptor = if external { 184 } else { 40 + key_bytes } as u64;
    let overflow = if external {
        key_bytes.saturating_sub(128).div_ceil(PAGE_BYTES - 128) as u64
    } else {
        0
    };
    // Twice the leaf and separator widths covers byte packing and the seven-
    // separator minimum. The group floor reserves one partial page per level.
    overflow * PAGE_BYTES as u64 + 2 * ((descriptor + 132) + (descriptor + 12))
}
