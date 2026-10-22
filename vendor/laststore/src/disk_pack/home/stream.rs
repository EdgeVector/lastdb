//! Exact-key body copies with bounded block and metadata ownership.

use crate::disk_pack::{corrupt, resource, Location, ReadBudget, ReadCall, ReadCosts, BLOCK_BYTES};
use crate::Result;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::io::Write;

const RUN_BLOCKS: u32 = lastdb_storage_policy::native_read::STREAM_RUN_BLOCKS;

/// Positive evidence only after the complete stored record identity validates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamedRecord {
    /// Exact stored body bytes, excluding the native record header and key.
    pub body_bytes: u64,
    /// SHA-256 of those exact body bytes.
    pub body_sha256: [u8; 32],
}

pub(super) fn copy_body_to(
    call: &mut ReadCall,
    collection: &str,
    group: u16,
    key: &str,
    output: &mut dyn Write,
) -> Result<Option<StreamedRecord>> {
    let Some(location) = call.locate(collection, group, key)? else {
        return Ok(None);
    };
    let mut stream = stream_call(call, &location)?;
    // No old decoded payload or locator cache coexists with the copy buffers.
    clear(call);
    // Only one descriptor pool is live during payload copy. The parent's
    // generation hold remains in place; max(old peak, stream peak) stays exact.
    call.files = super::super::call_io::CallFiles::default();
    let result = copy_record(&mut stream, collection, key, &location, output);
    merge_costs(&mut call.costs, stream.costs);
    result.map(Some)
}

fn stream_call(parent: &ReadCall, location: &Location) -> Result<ReadCall> {
    let blocks = u64::from(location.blocks);
    let pages = blocks
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(resource)?;
    let per_block = 2 * BLOCK_BYTES as u64 + 256 + 2 * super::super::PAGE_BYTES as u64;
    let bytes = blocks
        .checked_mul(per_block)
        .and_then(|value| value.checked_add(location.record_bytes))
        .and_then(|value| value.checked_add(super::super::PAGE_BYTES as u64))
        .ok_or_else(resource)?;
    // Only this validated exact record can use the larger cumulative allowance.
    // The parent retains its generation hold and its ordinary read limits.
    Ok(ReadCall {
        root: parent.root.clone(),
        current: parent.current.clone(),
        budget: ReadBudget {
            pages,
            blocks,
            bytes,
            keys: 1,
        },
        costs: ReadCosts::default(),
        pages: HashMap::new(),
        blocks: HashMap::new(),
        parts: HashMap::new(),
        files: super::super::call_io::CallFiles::default(),
        _generation_hold: None,
        payload_class: parent.payload_class,
        native_descriptor: parent.native_descriptor.clone(),
    })
}

fn copy_record(
    call: &mut ReadCall,
    collection: &str,
    key: &str,
    location: &Location,
    output: &mut dyn Write,
) -> Result<StreamedRecord> {
    if let Some(object) = call.object_metadata(location)? {
        let end = location
            .first_block
            .checked_mul(BLOCK_BYTES as u64)
            .and_then(|value| value.checked_add(u64::from(location.first_position)))
            .and_then(|value| value.checked_add(location.record_bytes))
            .ok_or_else(|| corrupt("streamed record object offset overflow"))?;
        if end > object.logical_bytes {
            return Err(corrupt("streamed record outside published object"));
        }
    }
    let body_bytes = location
        .record_bytes
        .checked_sub(96 + key.len() as u64)
        .ok_or_else(|| corrupt("streamed record shorter than complete key"))?;
    let header =
        super::super::record::record_header(collection, key, body_bytes, location.version)?;
    call.charge(location.record_bytes)?;
    let mut copy = RecordCopy {
        header: &header,
        key: key.as_bytes(),
        output,
        record_hash: Sha256::new(),
        body_hash: Sha256::new(),
        seen: 0,
    };
    if RUN_BLOCKS == 0 {
        return Err(resource());
    }
    let mut first = 0;
    while first < location.blocks {
        let count = (location.blocks - first).min(RUN_BLOCKS);
        copy_run(call, location, first, count, &mut copy)?;
        clear(call);
        first += count;
    }
    let record_hash: [u8; 32] = copy.record_hash.finalize().into();
    if copy.seen != location.record_bytes || record_hash != location.digest {
        return Err(corrupt("streamed record extent length or digest mismatch"));
    }
    Ok(StreamedRecord {
        body_bytes,
        body_sha256: copy.body_hash.finalize().into(),
    })
}

fn copy_run(
    call: &mut ReadCall,
    location: &Location,
    first: u32,
    count: u32,
    copy: &mut RecordCopy<'_>,
) -> Result<()> {
    let mut plans = Vec::with_capacity(count as usize);
    for index in first..first + count {
        let ordinal = location
            .first_block
            .checked_add(u64::from(index))
            .ok_or_else(|| corrupt("streamed record block overflow"))?;
        plans.push(call.plan_block(location, ordinal)?);
    }
    let identities: Vec<_> = plans.iter().map(|plan| plan.identity).collect();
    let mut failures = BTreeMap::new();
    call.read_planned_blocks(plans, &mut failures);
    if let Some((_, error)) = failures.into_iter().next() {
        return Err(error);
    }
    for (at, identity) in identities.into_iter().enumerate() {
        let block = call
            .blocks
            .get(&identity)
            .ok_or_else(|| corrupt("streamed decoded block is absent"))?;
        let start = if first == 0 && at == 0 {
            location.first_position as usize
        } else {
            0
        };
        if start >= block.bytes.len() {
            return Err(corrupt("streamed record starts outside decoded block"));
        }
        let remaining = location.record_bytes - copy.seen;
        let take = remaining.min((block.bytes.len() - start) as u64) as usize;
        if take as u64 != remaining && block.bytes.len() != BLOCK_BYTES {
            return Err(corrupt("short streamed block before record end"));
        }
        copy.write(&block.bytes[start..start + take])?;
    }
    Ok(())
}

struct RecordCopy<'a> {
    header: &'a [u8; 96],
    key: &'a [u8],
    output: &'a mut dyn Write,
    record_hash: Sha256,
    body_hash: Sha256,
    seen: u64,
}

impl RecordCopy<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.record_hash.update(bytes);
        let mut offset = 0;
        if self.seen < 96 {
            let start = self.seen as usize;
            let take = bytes.len().min(96 - start);
            if bytes[..take] != self.header[start..start + take] {
                return Err(corrupt("streamed record header or mutation version"));
            }
            offset = take;
        }
        let key_end = 96 + self.key.len() as u64;
        let position = self.seen + offset as u64;
        if position < key_end && offset < bytes.len() {
            let start = (position - 96) as usize;
            let take = (bytes.len() - offset).min(self.key.len() - start);
            if bytes[offset..offset + take] != self.key[start..start + take] {
                return Err(corrupt("streamed record complete key"));
            }
            offset += take;
        }
        if offset < bytes.len() {
            self.output.write_all(&bytes[offset..])?;
            self.body_hash.update(&bytes[offset..]);
        }
        self.seen += bytes.len() as u64;
        Ok(())
    }
}

fn clear(call: &mut ReadCall) {
    call.blocks.clear();
    call.pages.clear();
    call.parts.clear();
}

fn merge_costs(total: &mut ReadCosts, copy: ReadCosts) {
    total.directory_pages = total.directory_pages.saturating_add(copy.directory_pages);
    total.locator_pages = total.locator_pages.saturating_add(copy.locator_pages);
    total.overflow_pages = total.overflow_pages.saturating_add(copy.overflow_pages);
    total.map_pages = total.map_pages.saturating_add(copy.map_pages);
    total.payload_blocks = total.payload_blocks.saturating_add(copy.payload_blocks);
    total.shared_pages = total.shared_pages.saturating_add(copy.shared_pages);
    total.shared_blocks = total.shared_blocks.saturating_add(copy.shared_blocks);
    total.file_bytes = total.file_bytes.saturating_add(copy.file_bytes);
    total.budget_bytes = total.budget_bytes.saturating_add(copy.budget_bytes);
    total.examined_keys = total.examined_keys.saturating_add(copy.examined_keys);
    total.file_handle_reuses = total
        .file_handle_reuses
        .saturating_add(copy.file_handle_reuses);
    total.batch_read_runs = total.batch_read_runs.saturating_add(copy.batch_read_runs);
    total.file_handles_peak = total.file_handles_peak.max(copy.file_handles_peak);
}
