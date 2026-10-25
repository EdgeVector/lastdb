//! Direct bounded body-range copies. The source hold belongs to the parent call.
use super::{clear, stream_call, StreamedRecord, RUN_BLOCKS};
use crate::disk_pack::{corrupt, record, Location, ReadCall, Source, BLOCK_BYTES};
use crate::Result;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::ops::Range;

pub(in crate::disk_pack::home) fn copy_body_range_to(
    call: &mut ReadCall,
    collection: &str,
    group: u16,
    key: &str,
    range: Range<u64>,
    output: &mut dyn Write,
) -> Result<Option<StreamedRecord>> {
    let Some(location) = call.locate(collection, group, key)? else {
        return Ok(None);
    };
    if location.source != Source::Pack {
        return Err(corrupt("body range does not select a stored pack"));
    }
    let prefix = 96u64 + key.len() as u64;
    let body_bytes = location
        .record_bytes
        .checked_sub(prefix)
        .ok_or_else(|| corrupt("body range record prefix"))?;
    if range.start > range.end || range.end > body_bytes {
        return Err(corrupt("body range outside stored record"));
    }
    let mut stream = stream_call(call, &location)?;
    // The prefix and selected body can share a boundary block. Charge that
    // second bounded read without widening retained-memory admission.
    stream.budget.pages = stream.budget.pages.saturating_mul(2);
    stream.budget.blocks = stream.budget.blocks.saturating_mul(2);
    stream.budget.bytes = stream.budget.bytes.saturating_mul(2);
    clear(call);
    call.files = crate::disk_pack::call_io::CallFiles::default();
    let result = copy(
        &mut stream,
        collection,
        key,
        &location,
        prefix,
        range,
        output,
    );
    super::merge_costs(&mut call.costs, stream.costs);
    result.map(Some)
}

fn copy(
    call: &mut ReadCall,
    collection: &str,
    key: &str,
    location: &Location,
    prefix: u64,
    range: Range<u64>,
    output: &mut dyn Write,
) -> Result<StreamedRecord> {
    let header = record::record_header(
        collection,
        key,
        location.record_bytes - prefix,
        location.version,
        call.current.required_features,
    )?;
    let mut seen = 0usize;
    span(call, location, 0..prefix, |bytes| {
        for byte in bytes {
            let expected = if seen < header.len() {
                header[seen]
            } else {
                key.as_bytes()[seen - header.len()]
            };
            if *byte != expected {
                return Err(corrupt("body range key or mutation version differs"));
            }
            seen += 1;
        }
        Ok(())
    })?;
    let mut hash = Sha256::new();
    let bytes = range.end - range.start;
    span(
        call,
        location,
        prefix + range.start..prefix + range.end,
        |value| {
            output.write_all(value)?;
            hash.update(value);
            Ok(())
        },
    )?;
    Ok(StreamedRecord {
        body_bytes: bytes,
        body_sha256: hash.finalize().into(),
    })
}

fn span(
    call: &mut ReadCall,
    location: &Location,
    range: Range<u64>,
    mut output: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut position = range.start;
    while position < range.end {
        let absolute = u64::from(location.first_position) + position;
        let first = location.first_block + absolute / BLOCK_BYTES as u64;
        let last_absolute = u64::from(location.first_position) + range.end - 1;
        let last = location.first_block + last_absolute / BLOCK_BYTES as u64;
        let count = (last - first + 1).min(u64::from(RUN_BLOCKS));
        let mut plans = Vec::with_capacity(count as usize);
        for ordinal in first..first + count {
            plans.push(call.plan_block(location, ordinal)?);
        }
        let identities: Vec<_> = plans.iter().map(|plan| plan.identity).collect();
        let mut failures = BTreeMap::new();
        call.read_planned_blocks(plans, &mut failures);
        if let Some((_, failure)) = failures.into_iter().next() {
            return Err(failure);
        }
        for identity in identities {
            let block = call
                .blocks
                .get(&identity)
                .ok_or_else(|| corrupt("body range block is absent"))?;
            let start =
                ((u64::from(location.first_position) + position) % BLOCK_BYTES as u64) as usize;
            if start >= block.bytes.len() {
                return Err(corrupt("body range starts outside decoded block"));
            }
            let take = (range.end - position).min((block.bytes.len() - start) as u64) as usize;
            if position + (take as u64) < range.end && block.bytes.len() != BLOCK_BYTES {
                return Err(corrupt("body range crosses a short block"));
            }
            output(&block.bytes[start..start + take])?;
            position += take as u64;
        }
        clear(call);
    }
    Ok(())
}
