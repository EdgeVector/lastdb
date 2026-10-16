use super::bytes::*;
use super::page_write::{PageFields, PageWriter};
use super::{corrupt, TreeKind, INTERNAL_WIDTH, KEY_DESCRIPTOR, LEAF_WIDTH, PAGE_PAYLOAD};
use crate::Result;

pub(super) type Descriptor = [u8; KEY_DESCRIPTOR];
pub(super) type LeafEntry = (Descriptor, [u8; 128]);

pub(super) trait TreePages {
    fn reserve(&mut self, count: usize) -> Result<u64>;
    fn append(&mut self, fields: PageFields, payload: &[u8]) -> Result<u64>;
}

impl TreePages for PageWriter {
    fn reserve(&mut self, _count: usize) -> Result<u64> {
        Ok(self.next_page())
    }
    fn append(&mut self, fields: PageFields, payload: &[u8]) -> Result<u64> {
        self.append(fields, payload)
    }
}

pub(super) struct Child {
    // The left edge of the whole tree needs no separator or minimum lookup.
    pub(super) minimum: Option<Descriptor>,
    pub(super) number: u64,
}

pub(super) fn key_descriptor(pages: &mut impl TreePages, key: &[u8]) -> Result<Descriptor> {
    if key.len() > super::KEY_BYTES {
        return Err(corrupt("tree key exceeds format limit"));
    }
    let mut entry = [0; KEY_DESCRIPTOR];
    let inline = key.len().min(128);
    put32(&mut entry, 0, key.len() as u32);
    put16(&mut entry, 4, inline as u16);
    entry[8..8 + inline].copy_from_slice(&key[..inline]);
    entry[144..176].copy_from_slice(&digest(key));
    if key.len() > inline {
        let chunks = key[inline..].chunks(PAGE_PAYLOAD);
        let count = chunks.len();
        let first = pages.reserve(count)?;
        put16(&mut entry, 6, 1);
        put64(&mut entry, 136, first);
        for (index, suffix) in chunks.enumerate() {
            pages.append(
                PageFields {
                    kind: 4,
                    level: 0,
                    count: 0,
                    width: 0,
                    left: 0,
                    next: if index + 1 == count {
                        0
                    } else {
                        first + index as u64 + 1
                    },
                    auxiliary: (key.len() - inline) as u64,
                },
                suffix,
            )?;
        }
    }
    Ok(entry)
}

pub(super) fn leaf_fields(kind: TreeKind, count: usize) -> PageFields {
    PageFields {
        kind: if kind == TreeKind::Directory { 3 } else { 2 },
        level: 0,
        count: count as u16,
        width: LEAF_WIDTH as u16,
        left: 0,
        next: 0,
        auxiliary: 0,
    }
}

pub(super) fn leaf(
    pages: &mut impl TreePages,
    kind: TreeKind,
    entries: &[LeafEntry],
) -> Result<Child> {
    if entries.is_empty() || entries.len() > super::LEAF_ENTRIES {
        return Err(corrupt("tree leaf output count"));
    }
    let mut payload = Vec::with_capacity(entries.len() * LEAF_WIDTH);
    for (key, value) in entries {
        payload.extend_from_slice(key);
        payload.extend_from_slice(value);
    }
    let number = pages.append(leaf_fields(kind, entries.len()), &payload)?;
    Ok(Child {
        minimum: Some(entries[0].0),
        number,
    })
}

pub(super) fn parent(
    pages: &mut impl TreePages,
    level: usize,
    children: &[Child],
) -> Result<Child> {
    if level >= 8 || !(2..=89).contains(&children.len()) {
        return Err(corrupt("internal page height or child count"));
    }
    let mut payload = Vec::with_capacity((children.len() - 1) * INTERNAL_WIDTH);
    for child in &children[1..] {
        payload.extend_from_slice(
            child
                .minimum
                .as_ref()
                .ok_or_else(|| corrupt("missing separator"))?,
        );
        payload.extend_from_slice(&child.number.to_le_bytes());
    }
    let number = pages.append(
        PageFields {
            kind: 1,
            level: level as u8,
            count: (children.len() - 1) as u16,
            width: INTERNAL_WIDTH as u16,
            left: children[0].number,
            next: 0,
            auxiliary: 0,
        },
        &payload,
    )?;
    Ok(Child {
        minimum: children[0].minimum,
        number,
    })
}
