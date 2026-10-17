use super::page_write::PageWriter;
use super::tree_output::{self, Child, LeafEntry};
use super::{corrupt, TreeKind, TreeRoot, KEY_BYTES, LEAF_ENTRIES};
use crate::Result;
use std::path::Path;
use uuid::Uuid;

pub(super) struct TreeWriter {
    pages: PageWriter,
    object: Uuid,
    kind: TreeKind,
    last_key: Option<Vec<u8>>,
    leaf: Vec<LeafEntry>,
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
        pages.append(tree_output::leaf_fields(kind, 0), &[])?;
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
        let descriptor = tree_output::key_descriptor(&mut self.pages, key)?;
        self.leaf.push((descriptor, value));
        self.last_key = Some(key.to_vec());
        if self.leaf.len() == LEAF_ENTRIES {
            self.flush_leaf()?;
        }
        Ok(())
    }

    fn flush_leaf(&mut self) -> Result<()> {
        if self.leaf.is_empty() {
            return Ok(());
        }
        let child = tree_output::leaf(&mut self.pages, self.kind, &self.leaf)?;
        self.leaf.clear();
        self.push_child(0, child)
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
            let parent = tree_output::parent(&mut self.pages, level + 1, &children)?;
            self.push_child(level + 1, parent)?;
        }
        Ok(())
    }

    pub(super) fn finish_with_parts(mut self) -> Result<(TreeRoot, u64)> {
        self.flush_leaf()?;
        if self.levels[0].is_empty() && self.last_key.is_none() {
            return self.finish_root(0, 1);
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
                return self.finish_root(page, (level + 1) as u8);
            }
            if children.is_empty() {
                continue;
            }
            let pages = children.len().div_ceil(89);
            let chunk = children.len().div_ceil(pages);
            for children in children.chunks(chunk) {
                let parent = tree_output::parent(&mut self.pages, level + 1, children)?;
                self.push_child(level + 1, parent)?;
            }
        }
        Err(corrupt("tree height exhausted before publication"))
    }

    fn finish_root(self, page: u64, height: u8) -> Result<(TreeRoot, u64)> {
        let parts = self.pages.next_page().div_ceil(512);
        self.pages.finish()?;
        Ok((
            TreeRoot {
                object: self.object,
                page,
                height,
                kind: self.kind,
            },
            parts,
        ))
    }
}
