use super::bytes::*;
use super::page::Page;
use super::{corrupt, ReadCall, KEY_DESCRIPTOR};
use crate::Result;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// Tree leaf type and exact path namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TreeKind {
    /// Maps exact collection/group coordinates to locator roots.
    Directory,
    /// Maps full storage keys to exact payload extents.
    Locator,
}

impl TreeKind {
    pub(super) fn directory(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::Locator => "locator",
        }
    }
}

/// Immutable root identity. Page numbers never repeat under one object UUID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeRoot {
    /// Exact immutable tree identity.
    pub object: Uuid,
    /// Root page number, including page zero for the initial empty root.
    pub page: u64,
    /// Number of pages in a root-to-leaf path, one through eight.
    pub height: u8,
    /// Leaf format and physical namespace.
    pub kind: TreeKind,
}

impl TreeRoot {
    pub(super) fn validate(&self) -> Result<()> {
        nonnil(self.object)?;
        if !(1..=8).contains(&self.height) || self.page / 512 > u64::from(u32::MAX) {
            return Err(corrupt("tree root outside format limits"));
        }
        Ok(())
    }
}

impl ReadCall {
    pub(super) fn find(&mut self, tree: &TreeRoot, key: &[u8]) -> Result<Option<[u8; 128]>> {
        tree.validate()?;
        let mut number = tree.page;
        let mut ancestors = HashSet::new();
        let mut lower: Option<Vec<u8>> = None;
        let mut upper: Option<Vec<u8>> = None;
        for depth in 0..tree.height {
            if !ancestors.insert(number) {
                return Err(corrupt("tree ancestor cycle"));
            }
            let page = self.tree_page(tree, number)?;
            if page.level() != tree.height - depth - 1
                || (depth > 0 && page.kind() == 1 && page.count() < 7)
                || (depth > 0 && page.level() == 0 && page.count() == 0)
            {
                return Err(corrupt("tree level or internal occupancy"));
            }
            let (position, equal) = self.search_page(tree, &page, key)?;
            self.check_boundaries(tree, &page, position, lower.as_deref(), upper.as_deref())?;
            if page.level() == 0 {
                return Ok(equal.then(|| {
                    page.entry(position)[KEY_DESCRIPTOR..]
                        .try_into()
                        .expect("leaf value")
                }));
            }
            let child = position + usize::from(equal);
            if child > 0 {
                lower = Some(self.full_key(tree, page.entry(child - 1))?);
            }
            if child < page.count() {
                upper = Some(self.full_key(tree, page.entry(child))?);
            }
            number = if child == 0 {
                page.left()
            } else {
                u64_at(page.entry(child - 1), KEY_DESCRIPTOR)
            };
        }
        Err(corrupt("tree path failed to reach leaf"))
    }

    pub(super) fn search_page(
        &mut self,
        tree: &TreeRoot,
        page: &Page,
        key: &[u8],
    ) -> Result<(usize, bool)> {
        let (mut lo, mut hi) = (0, page.count());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.compare_key(tree, page.entry(mid), key)? {
                Ordering::Less => lo = mid + 1,
                Ordering::Equal => return Ok((mid, true)),
                Ordering::Greater => hi = mid,
            }
        }
        Ok((lo, false))
    }

    fn compare_key(&mut self, tree: &TreeRoot, entry: &[u8], key: &[u8]) -> Result<Ordering> {
        let prefix = &entry[8..8 + u16_at(entry, 4) as usize];
        let common = prefix.len().min(key.len());
        let order = prefix[..common].cmp(&key[..common]);
        if order != Ordering::Equal {
            return Ok(order);
        }
        if key.len() < prefix.len() {
            return Ok(Ordering::Greater);
        }
        Ok(self.full_key(tree, entry)?.as_slice().cmp(key))
    }

    pub(super) fn full_key(&mut self, tree: &TreeRoot, entry: &[u8]) -> Result<Vec<u8>> {
        let length = u32_at(entry, 0) as usize;
        let inline = u16_at(entry, 4) as usize;
        self.charge(length as u64)?;
        let mut key = Vec::new();
        key.try_reserve_exact(length)
            .map_err(|_| super::resource())?;
        key.extend_from_slice(&entry[8..8 + inline]);
        let mut next = u64_at(entry, 136);
        let mut seen = HashSet::new();
        let directory = self
            .root
            .join(tree.kind.directory())
            .join(tree.object.to_string());
        while next != 0 {
            if seen.len() >= 5 || !seen.insert(next) {
                return Err(corrupt("key overflow cycle or limit"));
            }
            let page = self.page(&directory, tree.object, next)?;
            if page.kind() != 4
                || page.auxiliary() != (length - inline) as u64
                || key.len() + page.payload().len() > length
            {
                return Err(corrupt("key overflow identity or length"));
            }
            key.extend_from_slice(page.payload());
            next = page.next();
        }
        if key.len() != length || digest(&key) != entry[144..176] {
            return Err(corrupt("full key length or digest"));
        }
        Ok(key)
    }

    pub(super) fn check_boundaries(
        &mut self,
        tree: &TreeRoot,
        page: &Arc<Page>,
        position: usize,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<()> {
        let before = position
            .checked_sub(1)
            .map(|index| self.full_key(tree, page.entry(index)))
            .transpose()?;
        let after = (position < page.count())
            .then(|| self.full_key(tree, page.entry(position)))
            .transpose()?;
        if before
            .as_deref()
            .zip(after.as_deref())
            .is_some_and(|(a, b)| a >= b)
        {
            return Err(corrupt("selected tree boundary is not ordered"));
        }
        for key in [before.as_deref(), after.as_deref()].into_iter().flatten() {
            if lower.is_some_and(|bound| key < bound) || upper.is_some_and(|bound| key >= bound) {
                return Err(corrupt("selected key outside parent interval"));
            }
        }
        Ok(())
    }
}
