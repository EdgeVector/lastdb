//! Bounded receipt packs with exact sequence offsets and unchanged JSON bytes.

use super::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub(super) const HEADER_BYTES: u64 = 44;
pub(super) const SLOT_BYTES: u64 = 12;
pub(super) const BLOCK_SLOTS: u32 = 256;
pub(super) const MAX_SLOTS: u32 = 1_048_576;
pub(super) const MAX_RECORD_BYTES: u32 = 64 * 1024;
pub(super) const MAX_PACK_BYTES: u64 = 512 * 1024 * 1024;
const MAGIC: [u8; 8] = *b"LDBGCP01";

#[derive(Clone, Copy)]
pub(super) struct Header {
    pub first: u64,
    pub slots: u32,
    pub records: u32,
    pub file_bytes: u64,
}

fn word<const N: usize>(file: &mut File) -> SyncResult<[u8; N]> {
    let mut bytes = [0; N];
    file.read_exact(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

pub(super) fn open(path: &Path, id: &str) -> SyncResult<(File, Header)> {
    let info = std::fs::symlink_metadata(path).map_err(io_error)?;
    if !info.is_file() || !(HEADER_BYTES..=MAX_PACK_BYTES).contains(&info.len()) {
        return Err(io_error("invalid receipt pack file"));
    }
    let mut file = File::open(path).map_err(io_error)?;
    // A writer can replace a block between pathname metadata and open. The
    // bounds belong to this immutable opened file, not the previous inode.
    let info = file.metadata().map_err(io_error)?;
    if !info.is_file() || !(HEADER_BYTES..=MAX_PACK_BYTES).contains(&info.len()) {
        return Err(io_error("invalid opened receipt pack file"));
    }
    if word::<8>(&mut file)? != MAGIC || u32::from_le_bytes(word(&mut file)?) != 1 {
        return Err(io_error("unsupported receipt pack format"));
    }
    let uuid = uuid::Uuid::parse_str(id).map_err(|_| io_error("invalid receipt job UUID"))?;
    if word::<16>(&mut file)? != *uuid.as_bytes() {
        return Err(io_error("receipt pack belongs to another job"));
    }
    let header = Header {
        first: u64::from_le_bytes(word(&mut file)?),
        slots: u32::from_le_bytes(word(&mut file)?),
        records: u32::from_le_bytes(word(&mut file)?),
        file_bytes: info.len(),
    };
    if header.first == 0
        || header.slots == 0
        || header.slots > MAX_SLOTS
        || header.records > header.slots
        || header
            .first
            .checked_add(u64::from(header.slots) - 1)
            .is_none_or(|last| last > u64::from(MAX_SLOTS))
        || HEADER_BYTES + u64::from(header.slots) * SLOT_BYTES > info.len()
    {
        return Err(io_error("invalid receipt pack bounds"));
    }
    Ok((file, header))
}

pub(super) fn read_raw(
    file: &mut File,
    header: Header,
    sequence: u64,
) -> SyncResult<Option<Vec<u8>>> {
    let Some(slot) = sequence.checked_sub(header.first) else {
        return Ok(None);
    };
    if slot >= u64::from(header.slots) {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(HEADER_BYTES + slot * SLOT_BYTES))
        .map_err(io_error)?;
    let offset = u64::from_le_bytes(word(file)?);
    let length = u32::from_le_bytes(word(file)?);
    if length == 0 {
        if offset != 0 {
            return Err(io_error("invalid empty receipt slot"));
        }
        return Ok(None);
    }
    let data_start = HEADER_BYTES + u64::from(header.slots) * SLOT_BYTES;
    if length > MAX_RECORD_BYTES
        || offset < data_start
        || offset
            .checked_add(u64::from(length))
            .is_none_or(|end| end > header.file_bytes)
    {
        return Err(io_error("receipt slot exceeds pack bounds"));
    }
    file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
    let mut bytes = vec![0; length as usize];
    file.read_exact(&mut bytes).map_err(io_error)?;
    Ok(Some(bytes))
}

pub(super) fn decode(raw: &[u8], id: &str, sequence: u64) -> SyncResult<GcObjectReceipt> {
    if raw.len() > MAX_RECORD_BYTES as usize {
        return Err(io_error("receipt exceeds pack record limit"));
    }
    let value: serde_json::Value = serde_json::from_slice(raw).map_err(io_error)?;
    check_receipt_version(value.get("version").and_then(serde_json::Value::as_u64))?;
    let receipt: GcObjectReceipt = serde_json::from_value(value).map_err(io_error)?;
    if receipt.job_id != id || receipt.sequence != sequence {
        return Err(io_error("packed receipt identity differs"));
    }
    Ok(receipt)
}

pub(super) fn publish<I>(
    path: &Path,
    id: &str,
    first: u64,
    slots: u32,
    records: I,
) -> SyncResult<()>
where
    I: IntoIterator<Item = SyncResult<(u64, Vec<u8>)>>,
{
    if first == 0 || slots == 0 || slots > MAX_SLOTS {
        return Err(io_error("invalid receipt pack range"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| io_error("pack has no parent"))?;
    std::fs::create_dir_all(parent).map_err(io_error)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
    let file = temp.as_file_mut();
    file.write_all(&MAGIC).map_err(io_error)?;
    file.write_all(&1_u32.to_le_bytes()).map_err(io_error)?;
    file.write_all(uuid::Uuid::parse_str(id).map_err(io_error)?.as_bytes())
        .map_err(io_error)?;
    file.write_all(&first.to_le_bytes()).map_err(io_error)?;
    file.write_all(&slots.to_le_bytes()).map_err(io_error)?;
    file.write_all(&0_u32.to_le_bytes()).map_err(io_error)?;
    let mut end = HEADER_BYTES + u64::from(slots) * SLOT_BYTES;
    file.set_len(end).map_err(io_error)?;
    let mut seen = HashSet::new();
    for item in records {
        let (sequence, bytes) = item?;
        let slot = sequence
            .checked_sub(first)
            .filter(|slot| *slot < u64::from(slots))
            .ok_or_else(|| io_error("receipt sequence exceeds pack range"))?;
        if !seen.insert(sequence) {
            return Err(io_error("duplicate receipt sequence"));
        }
        decode(&bytes, id, sequence)?;
        let next = end
            .checked_add(bytes.len() as u64)
            .filter(|next| *next <= MAX_PACK_BYTES)
            .ok_or_else(|| io_error("receipt pack capacity exceeded"))?;
        file.seek(SeekFrom::Start(end)).map_err(io_error)?;
        file.write_all(&bytes).map_err(io_error)?;
        file.seek(SeekFrom::Start(HEADER_BYTES + slot * SLOT_BYTES))
            .map_err(io_error)?;
        file.write_all(&end.to_le_bytes()).map_err(io_error)?;
        file.write_all(&(bytes.len() as u32).to_le_bytes())
            .map_err(io_error)?;
        end = next;
    }
    file.seek(SeekFrom::Start(40)).map_err(io_error)?;
    file.write_all(&(seen.len() as u32).to_le_bytes())
        .map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    temp.persist(path).map_err(io_error)?;
    sync_parent(path)
}
