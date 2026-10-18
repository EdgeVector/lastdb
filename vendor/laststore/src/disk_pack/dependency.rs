use super::bytes::*;
use super::page::Page;
use super::{artifact, corrupt, Current, TreeKind, TreeRoot, PAGE_BYTES, PART_BYTES};
use crate::read_receipt_io::{self as measured, ReadClass};
use crate::Result;
use std::io::Read;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub(super) const MAX_DEPENDENCIES: usize = 65_536;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Coordinate {
    pub(super) kind: u8,
    pub(super) object: Uuid,
    pub(super) batch: u64,
    pub(super) part: u32,
}

impl Coordinate {
    pub(super) fn path(&self, root: &Path) -> Result<PathBuf> {
        let base = match self.kind {
            1 => root.join("directory").join(self.object.to_string()),
            2 => root.join("locator").join(self.object.to_string()),
            3 | 4 if self.batch > 0 => root
                .join("journal")
                .join(self.object.to_string())
                .join(format!("batch-{:016x}", self.batch)),
            3 | 5 if self.batch == 0 => root.join("packs").join(self.object.to_string()),
            _ => return Err(corrupt("unsupported local dependency kind")),
        };
        let base = if self.kind == 3 {
            base.join("map")
        } else {
            base
        };
        Ok(base.join(format!("part-{:08x}", self.part)))
    }

    pub(super) fn capture(&self, root: &Path, current: &Current) -> Result<[u8; 80]> {
        let path = self.path(root)?;
        let mut file = measured::open(&path)?;
        let length = file.metadata()?.len();
        let data = self.kind == 4 || self.kind == 5;
        if length == 0 || length > PART_BYTES || (!data && length % PAGE_BYTES as u64 != 0) {
            return Err(corrupt("generation dependency length"));
        }
        let generation = if data {
            let mut bytes = [0; 128];
            let _class = ReadClass::FormatProbe.enter();
            file.read_exact(&mut bytes)?;
            verify_checksum(&bytes, 80)?;
            zero(&bytes[112..])?;
            if &bytes[..8] != b"LDPART1\0"
                || u16_at(&bytes, 8) != 1
                || bytes[10] != if self.kind == 4 { 2 } else { 1 }
                || bytes[11] != 0
                || u32_at(&bytes, 12) != 128
                || uuid_at(&bytes, 16) != self.object
                || u64_at(&bytes, 32) != self.batch
                || u32_at(&bytes, 40) != self.part
                || u64::from(u32_at(&bytes, 44)) != length
                || u64_at(&bytes, 64) != 0
                || u64_at(&bytes, 56) == 0
            {
                return Err(corrupt("generation data dependency identity"));
            }
            u64_at(&bytes, 72)
        } else {
            let mut call = artifact::metadata_call(root, current.clone());
            let buffer = call.read_extent(&path, 0, PAGE_BYTES, true, ReadClass::FormatProbe)?;
            let page = Page::decode(
                buffer,
                self.object,
                u64::from(self.part) * 512,
                current.generation,
                current.required_features,
            )?;
            if !match self.kind {
                1 => matches!(page.kind(), 1 | 3 | 4),
                2 => matches!(
                    page.kind(),
                    4 | super::packed_page::LEAF | super::packed_page::INTERNAL
                ),
                3 => page.kind() == 5,
                _ => false,
            } {
                return Err(corrupt("generation page dependency kind"));
            }
            page.generation()
        };
        if generation == 0 || generation > current.generation {
            return Err(corrupt("generation dependency creation boundary"));
        }
        let mut entry = [0; 80];
        entry[0] = self.kind;
        // Whole-part checksum is absent. Page/frame checksums remain mandatory.
        put32(&mut entry, 4, self.part);
        entry[8..24].copy_from_slice(self.object.as_bytes());
        put64(&mut entry, 24, self.batch);
        put64(&mut entry, 32, length);
        put64(&mut entry, 72, generation);
        Ok(entry)
    }
}

pub(super) struct Dependencies {
    entries: Vec<Coordinate>,
}

impl Dependencies {
    pub(super) fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub(super) fn from_coordinates(entries: Vec<Coordinate>) -> Result<Self> {
        if entries.len() > MAX_DEPENDENCIES {
            return Err(super::resource());
        }
        Ok(Self { entries })
    }

    pub(super) fn add_tree(
        &mut self,
        tree: &TreeRoot,
        parts: impl IntoIterator<Item = u32>,
    ) -> Result<()> {
        for part in parts {
            self.add(Coordinate {
                kind: if tree.kind == TreeKind::Directory {
                    1
                } else {
                    2
                },
                object: tree.object,
                batch: 0,
                part,
            })?;
        }
        Ok(())
    }

    pub(super) fn add_contiguous_tree(&mut self, tree: &TreeRoot, parts: u64) -> Result<()> {
        if parts > MAX_DEPENDENCIES as u64 {
            return Err(super::resource());
        }
        self.add_tree(tree, (0..parts).map(|part| part as u32))
    }

    pub(super) fn add_payload(
        &mut self,
        object: Uuid,
        batch: u64,
        data_parts: u64,
        map_pages: u64,
    ) -> Result<()> {
        let map_parts = map_pages.div_ceil(512);
        if data_parts
            .checked_add(map_parts)
            .map_or(true, |count| count > MAX_DEPENDENCIES as u64)
        {
            return Err(super::resource());
        }
        for part in 0..map_parts {
            self.add(Coordinate {
                kind: 3,
                object,
                batch,
                part: part as u32,
            })?;
        }
        for part in 0..data_parts {
            self.add(Coordinate {
                kind: if batch > 0 { 4 } else { 5 },
                object,
                batch,
                part: part as u32,
            })?;
        }
        Ok(())
    }

    pub(super) fn add(&mut self, coordinate: Coordinate) -> Result<()> {
        if self.entries.len() == MAX_DEPENDENCIES {
            self.sort();
            if self.entries.binary_search(&coordinate).is_ok() {
                return Ok(());
            }
            if self.entries.len() == MAX_DEPENDENCIES {
                return Err(crate::Error::Config(
                    "artifact publication exceeds its 65536-part dependency budget".into(),
                ));
            }
        }
        self.entries.try_reserve(1).map_err(|_| super::resource())?;
        self.entries.push(coordinate);
        Ok(())
    }

    fn sort(&mut self) {
        self.entries.sort_unstable();
        self.entries.dedup();
    }

    pub(super) fn finish(mut self) -> Vec<Coordinate> {
        self.sort();
        self.entries
    }
}
