use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::{artifact, corrupt, Current, DirectoryEntry, Location, Source, PAGE_BYTES};
use crate::Result;
use std::path::Path;

/// Worst-case uncompressed output, calculated from exact live locator entries.
/// Compression can never reduce this reservation and then enlarge its output beyond it.
/// Stream the complete selected trees; record and group counts are not format limits.
/// Each cursor path retains its own I/O budget, and output retains its space reservation.
pub(super) fn replacement_bytes(root: &Path, current: &Current) -> Result<u64> {
    // Complete receipt, its private publication, empty-tree floors, and publication slack.
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
            if location.source != Source::Tombstone {
                // Each record can straddle one initial block. Include frames and block map entries.
                let blocks = location
                    .record_bytes
                    .div_ceil(crate::disk_pack::BLOCK_BYTES as u64)
                    + 1;
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

/// A recent tier quotes recent descriptors and payload bounds, never old subtrees.
pub(super) fn tier_bytes(root: &Path, current: &Current, boundary: u64) -> Result<u64> {
    let receipt = crate::disk_pack::generation::read_receipt(root, current)?;
    let mut bytes = 8 * 1024 * 1024u64;
    add(
        &mut bytes,
        super::tier_quote::tree_bytes(root, current, &receipt, boundary)?,
    )?;
    let mut payloads = std::collections::BTreeSet::new();
    for (coordinate, record) in receipt.coordinates.iter().zip(&receipt.records) {
        if crate::disk_pack::bytes::u64_at(record, 72) <= boundary {
            continue;
        }
        match coordinate.kind {
            // Keep the existing metadata/part slack in addition to the exact
            // descriptor-based output bound. Old overflow chains can be referenced
            // by recent pages, so recent part lengths alone cannot quote them.
            1 | 2 => add(
                &mut bytes,
                crate::disk_pack::bytes::u64_at(record, 32).saturating_mul(16),
            )?,
            5 => {
                payloads.insert(coordinate.object);
            }
            _ => {}
        }
    }
    for object in payloads {
        let mut call = artifact::metadata_call(root, current.clone());
        let page = call.map_page(&root.join("packs").join(object.to_string()), object, 0)?;
        let (logical, _) = page
            .object_bounds()
            .ok_or_else(|| corrupt("tier quote has no payload bounds"))?;
        // A copy cannot emit more plaintext frames than its source objects.
        // The second copy covers worst-case block framing, maps and part tails.
        add(
            &mut bytes,
            logical
                .checked_mul(2)
                .ok_or_else(|| corrupt("tier quote overflow"))?,
        )?;
    }
    Ok(bytes)
}
