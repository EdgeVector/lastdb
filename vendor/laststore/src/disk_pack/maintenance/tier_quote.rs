//! Bound rewritten pages and key overflow without fetching old key contents.
use crate::disk_pack::bytes::{u32_at, u64_at};
use crate::disk_pack::dependency::{Coordinate, MAX_DEPENDENCIES};
use crate::disk_pack::generation::Generation;
use crate::disk_pack::{
    artifact, corrupt, key_layout, Current, DirectoryEntry, TreeKind, TreeRoot, PAGE_BYTES,
    PAGE_PAYLOAD,
};
use crate::Result;
use std::path::Path;

pub(super) fn tree_bytes(
    root: &Path,
    current: &Current,
    receipt: &Generation,
    boundary: u64,
) -> Result<u64> {
    let mut quote = TreeQuote {
        root,
        current,
        receipt,
        boundary,
        pages: 0,
        bytes: 0,
    };
    quote.tree(&current.directory)?;
    Ok(quote.bytes)
}

struct TreeQuote<'a> {
    root: &'a Path,
    current: &'a Current,
    receipt: &'a Generation,
    boundary: u64,
    pages: u64,
    bytes: u64,
}

impl TreeQuote<'_> {
    fn tree(&mut self, tree: &TreeRoot) -> Result<()> {
        tree.validate()?;
        self.page(tree, tree.page, tree.height - 1, self.current.generation)
    }

    fn page(&mut self, tree: &TreeRoot, number: u64, level: u8, parent_age: u64) -> Result<()> {
        let coordinate = Coordinate {
            kind: if tree.kind == TreeKind::Directory {
                1
            } else {
                2
            },
            object: tree.object,
            part: u32::try_from(number / 512).map_err(|_| corrupt("tier quote page ordinal"))?,
        };
        let at = self
            .receipt
            .coordinates
            .binary_search(&coordinate)
            .map_err(|_| corrupt("tier quote page is absent from its receipt"))?;
        let age = u64_at(&self.receipt.records[at], 72);
        if age > parent_age {
            return Err(corrupt("tier quote child is newer than its parent"));
        }
        if age <= self.boundary {
            return Ok(());
        }
        self.pages += 1;
        if self.pages > MAX_DEPENDENCIES as u64 * 512 {
            return Err(crate::disk_pack::resource());
        }
        self.add(PAGE_BYTES as u64)?;
        let page =
            artifact::metadata_call(self.root, self.current.clone()).tree_page(tree, number)?;
        if page.generation() != age || page.level() != level {
            return Err(corrupt("tier quote page age or height differs"));
        }
        let inline_limit = if tree.kind == TreeKind::Directory {
            128
        } else {
            key_layout::INLINE_KEY_BYTES
        };
        for index in 0..page.count() {
            let length = u32_at(page.entry(index), 0) as usize;
            if length > inline_limit {
                // A recent descriptor may still point at an old chain. TierCopy
                // rewrites that chain, including every internal separator copy.
                self.add((length - 128).div_ceil(PAGE_PAYLOAD) as u64 * PAGE_BYTES as u64)?;
            }
        }
        if level > 0 {
            self.page(tree, page.left(), level - 1, age)?;
            for index in 0..page.count() {
                self.page(tree, key_layout::child(page.entry(index)), level - 1, age)?;
            }
        } else if tree.kind == TreeKind::Directory {
            for index in 0..page.count() {
                let value: &[u8; 128] = key_layout::value(page.entry(index))
                    .try_into()
                    .map_err(|_| corrupt("tier quote directory value width"))?;
                let entry = DirectoryEntry::decode(value)?;
                self.tree(&entry.locator)?;
            }
        }
        Ok(())
    }

    fn add(&mut self, bytes: u64) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| corrupt("tier descriptor quote overflow"))?;
        Ok(())
    }
}
