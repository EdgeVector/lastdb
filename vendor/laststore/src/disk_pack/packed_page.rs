//! Packed locator slots: an offset/length array followed by contiguous entries.

use super::bytes::{put16, u16_at};
use super::key_layout;
use super::page::Page;
use super::page_write::PageFields;
use super::tree_output::TreePages;
use super::{corrupt, PAGE_PAYLOAD};
use crate::Result;

pub(super) const LEAF: u8 = 12;
pub(super) const INTERNAL: u8 = 13;

pub(super) fn is_packed(kind: u8) -> bool {
    kind == LEAF || kind == INTERNAL
}

pub(super) fn entry(payload: &[u8], index: usize) -> &[u8] {
    let offset = u16_at(payload, index * 4) as usize;
    let length = u16_at(payload, index * 4 + 2) as usize;
    &payload[offset..offset + length]
}

pub(super) fn validate(page: &Page) -> Result<()> {
    let value_bytes = if page.kind() == LEAF { 128 } else { 8 };
    let payload = page.payload();
    let mut offset = page.count() * 4;
    if offset > payload.len() {
        return Err(corrupt("packed slot array exceeds page"));
    }
    let mut previous: Option<&[u8]> = None;
    for index in 0..page.count() {
        let start = u16_at(payload, index * 4) as usize;
        let length = u16_at(payload, index * 4 + 2) as usize;
        if start != offset || start + length > payload.len() {
            return Err(corrupt("packed slots overlap or exceed page"));
        }
        let entry = &payload[start..start + length];
        key_layout::validate_packed(entry, value_bytes)?;
        let prefix = key_layout::inline(entry);
        if previous.is_some_and(|last| {
            let common = last.len().min(prefix.len());
            last[..common] > prefix[..common]
        }) || (page.kind() == INTERNAL && key_layout::child(entry) == 0)
        {
            return Err(corrupt("packed key order or child"));
        }
        previous = Some(prefix);
        offset += length;
    }
    if offset != payload.len() {
        return Err(corrupt("packed page has unreferenced bytes"));
    }
    Ok(())
}

pub(super) fn append<'a>(
    pages: &mut impl TreePages,
    mut fields: PageFields,
    entries: impl ExactSizeIterator<Item = (&'a [u8], &'a [u8])>,
) -> Result<u64> {
    let payload_build = crate::commit_timing::stage("metadata_packed_payload_build_us");
    let count = entries.len();
    let mut payload = vec![0; count * 4];
    for (index, (key, value)) in entries.enumerate() {
        let length = key.len() + value.len();
        let offset = payload.len();
        if offset + length > PAGE_PAYLOAD {
            return Err(corrupt("packed output exceeds page"));
        }
        put16(&mut payload, index * 4, offset as u16);
        put16(&mut payload, index * 4 + 2, length as u16);
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);
    }
    fields.count = count as u16;
    fields.width = 0;
    drop(payload_build);
    pages.append(fields, &payload)
}
