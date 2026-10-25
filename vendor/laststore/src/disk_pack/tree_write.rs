use super::page_write::PageWriter;
use super::tree_output::{self, Child, LeafEntry};
use super::{corrupt, TreeKind, TreeRoot, LEAF_ENTRIES};
use crate::Result;
use std::path::Path;
use uuid::Uuid;

pub(super) struct TreeWriter {
    pages: PageWriter,
    object: Uuid,
    kind: TreeKind,
    key_limit: usize,
    last_key: Option<Vec<u8>>,
    leaf: Vec<LeafEntry>,
    levels: Vec<Vec<Child>>,
}

impl TreeWriter {
    pub(super) fn new(root: &Path, kind: TreeKind, generation: u64) -> Result<Self> {
        Self::protected(root, kind, generation, Default::default())
    }

    pub(super) fn protected(
        root: &Path,
        kind: TreeKind,
        generation: u64,
        protection: super::private_files::OutputFiles,
    ) -> Result<Self> {
        let object = Uuid::new_v4();
        let mut pages = PageWriter::protected(
            root.join(kind.directory()).join(object.to_string()),
            object,
            generation,
            protection,
        )?;
        pages.append(tree_output::leaf_fields(kind, 0), &[])?;
        Self::from_pages(pages, kind)
    }

    /// Start an independent tree in an allocator whose page zero is an empty leaf.
    /// Every child and overflow address uses the allocator's final object identity.
    pub(super) fn from_pages(pages: PageWriter, kind: TreeKind) -> Result<Self> {
        if pages.next_page() == 0 {
            return Err(corrupt("tree allocator has no empty root"));
        }
        let object = pages.object();
        Ok(Self {
            pages,
            object,
            kind,
            key_limit: super::LEGACY_STORAGE_KEY_BYTES,
            last_key: None,
            leaf: Vec::new(),
            levels: vec![Vec::new()],
        })
    }

    pub(super) fn for_features(mut self, features: u64) -> Self {
        self.key_limit = super::key_limits::key_byte_limit(features);
        self
    }

    pub(super) fn add(&mut self, key: &[u8], value: [u8; 128]) -> Result<()> {
        if key.len() > self.key_limit
            || self
                .last_key
                .as_deref()
                .is_some_and(|previous| previous >= key)
        {
            return Err(corrupt(
                "tree input keys must be strictly ordered and representable",
            ));
        }
        let descriptor =
            tree_output::key_descriptor(&mut self.pages, self.kind, key, self.key_limit)?;
        if self.kind == TreeKind::Locator
            && !self.leaf.is_empty()
            && self
                .leaf
                .iter()
                .map(|(key, _)| key.len() + 132)
                .sum::<usize>()
                + descriptor.len()
                + 132
                > super::PAGE_PAYLOAD
        {
            self.flush_leaf()?;
        }
        self.leaf.push((descriptor, value));
        self.last_key = Some(key.to_vec());
        if self.kind == TreeKind::Directory && self.leaf.len() == LEAF_ENTRIES {
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
        let split = if self.kind == TreeKind::Directory {
            (self.levels[level].len() >= 178).then_some(89)
        } else if self.levels[level].len() > 1 {
            let ranges = super::tree_partition::parents(self.kind, &self.levels[level])?;
            // Keep two byte-bounded groups for the final minimum-occupancy split.
            (ranges.len() > 2).then(|| ranges[0].end)
        } else {
            None
        };
        if let Some(split) = split {
            let children = self.levels[level].drain(..split).collect::<Vec<_>>();
            let parent = tree_output::parent(&mut self.pages, self.kind, level + 1, &children)?;
            self.push_child(level + 1, parent)?;
        }
        Ok(())
    }

    pub(super) fn finish_with_parts(self) -> Result<(TreeRoot, u64)> {
        let (tree, pages) = self.finish_tree()?;
        let parts = pages.next_page().div_ceil(crate::disk_pack::PAGES_PER_PART);
        pages.finish()?;
        Ok((tree, parts))
    }

    /// Return the private allocator without a barrier so the next tree can share it.
    pub(super) fn finish_tree(mut self) -> Result<(TreeRoot, PageWriter)> {
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
            for range in super::tree_partition::parents(self.kind, &children)? {
                let parent =
                    tree_output::parent(&mut self.pages, self.kind, level + 1, &children[range])?;
                self.push_child(level + 1, parent)?;
            }
        }
        Err(corrupt("tree height exhausted before publication"))
    }

    fn finish_root(self, page: u64, height: u8) -> Result<(TreeRoot, PageWriter)> {
        Ok((
            TreeRoot {
                object: self.object,
                page,
                height,
                kind: self.kind,
            },
            self.pages,
        ))
    }
}
