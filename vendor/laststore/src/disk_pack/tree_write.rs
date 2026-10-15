use super::bytes::*;
use super::page_write::{PageFields, PageWriter};
use super::{
    corrupt, TreeKind, TreeRoot, INTERNAL_WIDTH, KEY_BYTES, KEY_DESCRIPTOR, LEAF_ENTRIES,
    LEAF_WIDTH, PAGE_PAYLOAD,
};
use crate::Result;
use std::path::Path;
use uuid::Uuid;

struct Child {
    minimum: [u8; KEY_DESCRIPTOR],
    number: u64,
}

pub(super) struct TreeWriter {
    pages: PageWriter,
    object: Uuid,
    kind: TreeKind,
    last_key: Option<Vec<u8>>,
    leaf: Vec<([u8; KEY_DESCRIPTOR], [u8; 128])>,
    levels: Vec<Vec<Child>>,
}

impl TreeWriter {
    pub(super) fn new(root: &Path, kind: TreeKind, generation: u64) -> Result<Self> {
        let object = Uuid::new_v4();
        let mut pages = PageWriter::new(
            root.join(kind.directory()).join(object.to_string()),
            object,
            generation,
        )?;
        pages.append(Self::leaf_fields(kind, 0), &[])?;
        Ok(Self {
            pages,
            object,
            kind,
            last_key: None,
            leaf: Vec::new(),
            levels: vec![Vec::new()],
        })
    }

    pub(super) fn add(&mut self, key: &[u8], value: [u8; 128]) -> Result<()> {
        if key.len() > KEY_BYTES
            || self
                .last_key
                .as_deref()
                .is_some_and(|previous| previous >= key)
        {
            return Err(corrupt(
                "tree input keys must be strictly ordered and representable",
            ));
        }
        let descriptor = self.key_descriptor(key)?;
        self.leaf.push((descriptor, value));
        self.last_key = Some(key.to_vec());
        if self.leaf.len() == LEAF_ENTRIES {
            self.flush_leaf()?;
        }
        Ok(())
    }

    fn key_descriptor(&mut self, key: &[u8]) -> Result<[u8; KEY_DESCRIPTOR]> {
        let mut entry = [0; KEY_DESCRIPTOR];
        let inline = key.len().min(128);
        put32(&mut entry, 0, key.len() as u32);
        put16(&mut entry, 4, inline as u16);
        entry[8..8 + inline].copy_from_slice(&key[..inline]);
        entry[144..176].copy_from_slice(&digest(key));
        if key.len() > inline {
            put16(&mut entry, 6, 1);
            put64(&mut entry, 136, self.pages.next_page());
            let chunks = key[inline..].chunks(PAGE_PAYLOAD);
            let count = chunks.len();
            for (index, suffix) in chunks.enumerate() {
                let next = if index + 1 == count {
                    0
                } else {
                    self.pages.next_page() + 1
                };
                self.pages.append(
                    PageFields {
                        kind: 4,
                        level: 0,
                        count: 0,
                        width: 0,
                        left: 0,
                        next,
                        auxiliary: (key.len() - inline) as u64,
                    },
                    suffix,
                )?;
            }
        }
        Ok(entry)
    }

    fn leaf_fields(kind: TreeKind, count: usize) -> PageFields {
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

    fn flush_leaf(&mut self) -> Result<()> {
        if self.leaf.is_empty() {
            return Ok(());
        }
        let minimum = self.leaf[0].0;
        let mut payload = Vec::with_capacity(self.leaf.len() * LEAF_WIDTH);
        for (key, value) in &self.leaf {
            payload.extend_from_slice(key);
            payload.extend_from_slice(value);
        }
        let number = self
            .pages
            .append(Self::leaf_fields(self.kind, self.leaf.len()), &payload)?;
        self.leaf.clear();
        self.push_child(0, Child { minimum, number })
    }

    fn push_child(&mut self, level: usize, child: Child) -> Result<()> {
        if level >= 8 {
            return Err(corrupt("tree height admission limit"));
        }
        if self.levels.len() <= level {
            self.levels.push(Vec::new());
        }
        self.levels[level].push(child);
        // Retain at least 89 children until the final balancing decision.
        if self.levels[level].len() >= 178 {
            let children = self.levels[level].drain(..89).collect::<Vec<_>>();
            let parent = self.parent(level + 1, &children)?;
            self.push_child(level + 1, parent)?;
        }
        Ok(())
    }

    fn parent(&mut self, level: usize, children: &[Child]) -> Result<Child> {
        if level >= 8 || !(2..=89).contains(&children.len()) {
            return Err(corrupt("internal page height or child count"));
        }
        let mut payload = Vec::with_capacity((children.len() - 1) * INTERNAL_WIDTH);
        for child in &children[1..] {
            payload.extend_from_slice(&child.minimum);
            payload.extend_from_slice(&child.number.to_le_bytes());
        }
        let number = self.pages.append(
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

    pub(super) fn finish(mut self) -> Result<TreeRoot> {
        self.flush_leaf()?;
        if self.levels[0].is_empty() && self.last_key.is_none() {
            self.pages.finish()?;
            return Ok(TreeRoot {
                object: self.object,
                page: 0,
                height: 1,
                kind: self.kind,
            });
        }
        for level in 0..8 {
            let children = std::mem::take(&mut self.levels[level]);
            let higher = self
                .levels
                .iter()
                .skip(level + 1)
                .any(|children| !children.is_empty());
            if children.len() == 1 && !higher {
                let page = children[0].number;
                self.pages.finish()?;
                return Ok(TreeRoot {
                    object: self.object,
                    page,
                    height: (level + 1) as u8,
                    kind: self.kind,
                });
            }
            if children.is_empty() {
                continue;
            }
            let pages = children.len().div_ceil(89);
            let chunk = children.len().div_ceil(pages);
            for children in children.chunks(chunk) {
                let parent = self.parent(level + 1, children)?;
                self.push_child(level + 1, parent)?;
            }
        }
        Err(corrupt("tree height exhausted before publication"))
    }
}
