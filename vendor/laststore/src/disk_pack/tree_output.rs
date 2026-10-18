use super::bytes::*;
use super::page_write::{PageFields, PageWriter};
use super::{corrupt, TreeKind, INTERNAL_WIDTH, KEY_DESCRIPTOR, LEAF_WIDTH, PAGE_PAYLOAD};
use crate::Result;

pub(super) type Descriptor = Vec<u8>;
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

pub(super) fn key_descriptor(
    pages: &mut impl TreePages,
    kind: TreeKind,
    key: &[u8],
) -> Result<Descriptor> {
    if key.len() > super::KEY_BYTES {
        return Err(corrupt("tree key exceeds format limit"));
    }
    let packed = kind == TreeKind::Locator;
    let external = key.len()
        > if packed {
            super::key_layout::INLINE_KEY_BYTES
        } else {
            128
        };
    let inline = if external { 128 } else { key.len() };
    let mut entry = vec![
        0;
        if packed {
            if external {
                184
            } else {
                40 + inline
            }
        } else {
            KEY_DESCRIPTOR
        }
    ];
    put32(&mut entry, 0, key.len() as u32);
    put16(&mut entry, 4, inline as u16);
    let offset = if packed { 40 } else { 8 };
    entry[offset..offset + inline].copy_from_slice(&key[..inline]);
    let digest_offset = if packed { 8 } else { 144 };
    entry[digest_offset..digest_offset + 32].copy_from_slice(&digest(key));
    if packed {
        put16(
            &mut entry,
            6,
            if external {
                super::key_layout::PACKED_EXTERNAL
            } else {
                super::key_layout::PACKED_INLINE
            },
        );
    }
    if external {
        let chunks = key[inline..].chunks(PAGE_PAYLOAD);
        let count = chunks.len();
        let first = pages.reserve(count)?;
        if !packed {
            put16(&mut entry, 6, 1);
        }
        put64(&mut entry, if packed { 168 } else { 136 }, first);
        if packed {
            put64(&mut entry, 176, (key.len() - inline) as u64);
        }
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
        kind: if kind == TreeKind::Directory {
            3
        } else {
            super::packed_page::LEAF
        },
        level: 0,
        count: count as u16,
        width: if kind == TreeKind::Directory {
            LEAF_WIDTH as u16
        } else {
            0
        },
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
    if entries.is_empty() || (kind == TreeKind::Directory && entries.len() > super::LEAF_ENTRIES) {
        return Err(corrupt("tree leaf output count"));
    }
    let number = if kind == TreeKind::Locator {
        super::packed_page::append(
            pages,
            leaf_fields(kind, entries.len()),
            entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice())),
        )?
    } else {
        let mut payload = Vec::with_capacity(entries.len() * LEAF_WIDTH);
        for (key, value) in entries {
            payload.extend_from_slice(key);
            payload.extend_from_slice(value);
        }
        pages.append(leaf_fields(kind, entries.len()), &payload)?
    };
    Ok(Child {
        minimum: Some(entries[0].0.clone()),
        number,
    })
}

pub(super) fn parent(
    pages: &mut impl TreePages,
    kind: TreeKind,
    level: usize,
    children: &[Child],
) -> Result<Child> {
    if level >= 8 || children.len() < 2 || (kind == TreeKind::Directory && children.len() > 89) {
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
    let fields = PageFields {
        kind: if kind == TreeKind::Locator {
            super::packed_page::INTERNAL
        } else {
            1
        },
        level: level as u8,
        count: (children.len() - 1) as u16,
        width: INTERNAL_WIDTH as u16,
        left: children[0].number,
        next: 0,
        auxiliary: 0,
    };
    let number = if kind == TreeKind::Locator {
        let values: Vec<_> = children[1..]
            .iter()
            .map(|child| child.number.to_le_bytes())
            .collect();
        super::packed_page::append(
            pages,
            fields,
            children[1..].iter().zip(&values).map(|(child, value)| {
                (
                    child
                        .minimum
                        .as_ref()
                        .expect("validated separator")
                        .as_slice(),
                    value.as_slice(),
                )
            }),
        )?
    } else {
        pages.append(fields, &payload)?
    };
    Ok(Child {
        minimum: children[0].minimum.clone(),
        number,
    })
}
