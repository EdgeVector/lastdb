//! Caches that live for one whole pack.
//!
//! The pack reads each live record through a short read call, so that no call
//! passes its finite budget. A new call knows nothing. Before this module, every
//! record paid again for facts that never change during the pack: the block-map
//! page of its batch (a read and a SHA-256 over 16 KiB), the header of its part
//! (a read, a SHA-256 and a stat), and the open of its part file. Records come
//! in key order, so each record meets another batch. A cache that lives for a
//! window of 1,024 records meets only a third of the batches again.
//!
//! `PackCaches` hands those results from one call to the next. It adds no check
//! and removes none. A map page, a part header, and a decoded block come from
//! immutable files that the pack already proved, and each record is still
//! checked against them: its extent, its object bounds, its identity, its
//! length, and its SHA-256.
//!
//! Memory is bounded by the constants below, not by the home:
//! `MAP_PAGES` pages of 16 KiB (64 MiB), `BLOCKS` blocks of 4 KiB (1 MiB),
//! `PARTS` part records of about 200 bytes (6 MiB), and `FILES` descriptors.

use crate::disk_pack::block_plan::BlockId;
use crate::disk_pack::call::Buffer;
use crate::disk_pack::call_io::CallFiles;
use crate::disk_pack::page::{Page, PageAddress};
use crate::disk_pack::{artifact, Current, ReadCall};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Block-map pages kept between calls. One page maps 677 blocks, so this serves
/// more than four thousand batches at a full hit rate.
const MAP_PAGES: usize = 4096;
/// Decoded blocks kept between calls. Records of a packed source share blocks.
const BLOCKS: usize = 256;
/// Verified part headers kept between calls.
const PARTS: usize = 32_768;
/// Open part files kept between calls. The operating system limit on open files
/// can be as low as 256 for a service, so the pool stays small.
const FILES: usize = 64;

pub(super) struct PackCaches {
    pages: HashMap<PageAddress, Arc<Page>>,
    blocks: HashMap<BlockId, Arc<Buffer>>,
    parts: HashMap<PathBuf, (u64, u64)>,
    files: CallFiles,
}

impl PackCaches {
    pub(super) fn new() -> Self {
        Self {
            pages: HashMap::new(),
            blocks: HashMap::new(),
            parts: HashMap::new(),
            files: CallFiles::with_capacity(FILES),
        }
    }

    /// A short read call that starts with everything the pack has already proved.
    pub(super) fn call(&mut self, root: &Path, current: &Current) -> ReadCall {
        let mut call = artifact::metadata_call(root, current.clone());
        // The kept pages count against the page limit of one call, so the limit
        // grows by the same bound and the cursor keeps its own room.
        call.budget.pages += MAP_PAGES as u64;
        call.pages = std::mem::take(&mut self.pages);
        call.blocks = std::mem::take(&mut self.blocks);
        call.parts = std::mem::take(&mut self.parts);
        call.files = std::mem::replace(&mut self.files, CallFiles::with_capacity(FILES));
        call
    }

    /// Take back what a finished call learned, within the fixed bounds.
    pub(super) fn keep(&mut self, call: ReadCall) {
        let ReadCall {
            mut pages,
            mut blocks,
            mut parts,
            files,
            ..
        } = call;
        if pages.len() > MAP_PAGES {
            // Tree pages are read once in a key-order walk. Block maps are the hot set.
            pages.retain(|_, page| page.kind() == 5);
            if pages.len() > MAP_PAGES {
                let mut keep = MAP_PAGES / 2;
                pages.retain(|_, _| {
                    keep = keep.saturating_sub(1);
                    keep > 0
                });
            }
        }
        if blocks.len() > BLOCKS {
            blocks.clear();
        }
        if parts.len() > PARTS {
            parts.clear();
        }
        self.pages = pages;
        self.blocks = blocks;
        self.parts = parts;
        self.files = files;
    }
}
