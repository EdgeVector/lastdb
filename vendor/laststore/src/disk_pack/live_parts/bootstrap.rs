use super::{Changes, LiveParts, Work};
use crate::disk_pack::bytes::u16_at;
use crate::disk_pack::dependency::{Coordinate, MAX_DEPENDENCIES};
use crate::disk_pack::page::Page;
use crate::disk_pack::{
    artifact, corrupt, key_layout, Current, DirectoryEntry, ReadCall, TreeKind, TreeRoot,
};
use crate::Result;
use std::path::Path;

struct Collector<'a> {
    root: &'a Path,
    current: &'a Current,
    allowed: &'a [Coordinate],
    live: LiveParts,
    work: Work,
}

struct TreePath {
    number: u64,
    level: u8,
    is_root: bool,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
}

impl TreePath {
    fn root(tree: &TreeRoot) -> Self {
        Self {
            number: tree.page,
            level: tree.height - 1,
            is_root: true,
            lower: None,
            upper: None,
        }
    }
}

/// One round shares a work ceiling and count ledger across all imported trees.
/// Directory edits account allocator templates separately from these subtrees.
pub(in crate::disk_pack) struct LocatorImports<'a>(Collector<'a>);

impl<'a> LocatorImports<'a> {
    pub(in crate::disk_pack) fn new(
        root: &'a Path,
        current: &'a Current,
        allowed: &'a [Coordinate],
    ) -> Result<Self> {
        Ok(Self(Collector::new(root, current, allowed)?))
    }

    pub(in crate::disk_pack) fn add(&mut self, tree: &TreeRoot) -> Result<()> {
        if tree.kind != TreeKind::Locator {
            return Err(corrupt("imported live tree is not a locator"));
        }
        self.0.tree(tree, TreePath::root(tree))
    }

    pub(in crate::disk_pack) fn finish(self) -> Result<Changes> {
        let mut changes = Changes::default();
        for (coordinate, count) in self.0.live.counts {
            let count = i64::try_from(count)
                .map_err(|_| corrupt("imported live-part count exceeds signed delta"))?;
            changes.add_part(coordinate, count)?;
        }
        Ok(changes)
    }
}

pub(super) fn collect(root: &Path, current: &Current, allowed: &[Coordinate]) -> Result<LiveParts> {
    let mut collector = Collector::new(root, current, allowed)?;
    let tree = current.directory.clone();
    collector.tree(&tree, TreePath::root(&tree))?;
    let mut empty = Changes::default();
    super::add_directory_empty_root(root, current, 1, &mut empty)?;
    collector.include(empty)?;
    Ok(collector.live)
}

impl<'a> Collector<'a> {
    fn new(root: &'a Path, current: &'a Current, allowed: &'a [Coordinate]) -> Result<Self> {
        if allowed.len() > MAX_DEPENDENCIES || allowed.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(corrupt(
                "bootstrap part coordinates are not bounded and ordered",
            ));
        }
        current.encode()?;
        Ok(Self {
            root,
            current,
            allowed,
            live: LiveParts::new(),
            work: Work::seed(),
        })
    }

    fn include(&mut self, changes: Changes) -> Result<()> {
        self.live.apply_checked(
            self.root,
            self.current,
            changes,
            Some(self.allowed),
            &mut self.work,
        )
    }

    fn tree(&mut self, tree: &TreeRoot, path: TreePath) -> Result<()> {
        self.work.charge()?;
        tree.validate()?;
        let coordinate = Coordinate {
            kind: if tree.kind == TreeKind::Directory {
                1
            } else {
                2
            },
            object: tree.object,
            part: u32::try_from(path.number / crate::disk_pack::PAGES_PER_PART)
                .map_err(|_| corrupt("bootstrap tree part exceeds format"))?,
        };
        if self.allowed.binary_search(&coordinate).is_err() {
            return Err(corrupt("bootstrap tree page lies outside the receipt"));
        }
        let mut call = artifact::metadata_call(self.root, self.current.clone());
        let page = call.tree_page(tree, path.number)?;
        if page.level() != path.level
            || (!path.is_root && path.level > 0 && page.count() < 7)
            || (!path.is_root && path.level == 0 && page.count() == 0)
            || (tree.kind == TreeKind::Locator && page.count() == 0 && path.number != 0)
        {
            return Err(corrupt(
                "bootstrap tree level, occupancy, or empty-root address",
            ));
        }
        let mut changes = Changes::default();
        changes.observe_complete_page(
            tree.kind,
            tree.object,
            path.number,
            &page,
            self.current,
            1,
        )?;
        // The held page owns its bytes. Release this path's descriptors before
        // a resolver or child path opens its own bounded descriptor pool.
        call = artifact::metadata_call(self.root, self.current.clone());
        self.include(changes)?;
        self.entries(tree, &path, &page, &mut call)?;
        if path.level > 0 {
            self.children(tree, &path, &page, &mut call)?;
        }
        Ok(())
    }

    fn entries(
        &mut self,
        tree: &TreeRoot,
        path: &TreePath,
        page: &Page,
        call: &mut ReadCall,
    ) -> Result<()> {
        let mut previous: Option<Vec<u8>> = None;
        for at in 0..page.count() {
            self.work.charge()?;
            self.reset(call);
            let key = call.full_key(tree, page.entry(at))?;
            if previous
                .as_deref()
                .is_some_and(|previous| previous >= key.as_slice())
                || path
                    .lower
                    .as_deref()
                    .is_some_and(|lower| lower > key.as_slice())
                || path
                    .upper
                    .as_deref()
                    .is_some_and(|upper| upper <= key.as_slice())
            {
                return Err(corrupt("bootstrap key order or parent interval"));
            }
            if path.level == 0 && tree.kind == TreeKind::Directory {
                *call = artifact::metadata_call(self.root, self.current.clone());
                self.directory_entry(&key, key_layout::value(page.entry(at)))?;
            }
            previous = Some(key);
        }
        Ok(())
    }

    fn directory_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = DirectoryEntry::decode(value.try_into().expect("directory value"))?;
        if key.len() < 4
            || usize::from(u16_at(key, 0)) + 4 != key.len()
            || u16_at(key, key.len() - 2) != entry.group
            || std::str::from_utf8(&key[2..key.len() - 2]).is_err()
        {
            return Err(corrupt("bootstrap directory key differs from its value"));
        }
        self.tree(&entry.locator, TreePath::root(&entry.locator))
    }

    fn children(
        &mut self,
        tree: &TreeRoot,
        path: &TreePath,
        page: &Page,
        call: &mut ReadCall,
    ) -> Result<()> {
        let mut lower = path.lower.clone();
        for at in 0..=page.count() {
            self.reset(call);
            let upper = if at == page.count() {
                path.upper.clone()
            } else {
                Some(call.full_key(tree, page.entry(at))?)
            };
            let number = if at == 0 {
                page.left()
            } else {
                key_layout::child(page.entry(at - 1))
            };
            *call = artifact::metadata_call(self.root, self.current.clone());
            self.tree(
                tree,
                TreePath {
                    number,
                    level: path.level - 1,
                    is_root: false,
                    lower,
                    upper: upper.clone(),
                },
            )?;
            lower = upper;
        }
        Ok(())
    }

    fn reset(&self, call: &mut ReadCall) {
        if call.pages.len() >= 24 {
            *call = artifact::metadata_call(self.root, self.current.clone());
        }
    }
}
