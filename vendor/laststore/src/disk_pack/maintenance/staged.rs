//! Fully prepared receipt and pointer; only conditional installation spans publication.
use super::PreparedCompaction;
use crate::disk_pack::write_lock::WriteLock;
use crate::disk_pack::{artifact, corrupt, generation, publish, Current};
use crate::{durability, Result};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub const PENDING_GENERATION: &str = "GENERATION.pending-";

/// Bytes and generation identity are immutable after preparation.
pub struct CompactionPublication {
    identity: Uuid,
    owner: std::sync::Arc<WriteLock>,
    _protection: crate::disk_pack::private_files::OutputFiles,
    pub(in crate::disk_pack) expected: Current,
    pub(in crate::disk_pack) current: Current,
    pub(in crate::disk_pack) parts: u64,
    root: PathBuf,
    receipt: PathBuf,
    pointer: PathBuf,
    pointer_file: File,
}

impl PreparedCompaction {
    /// No writer or publication lock is required for this complete preparation.
    pub fn prepare_publication(&mut self) -> Result<CompactionPublication> {
        self.ensure_usable()?;
        let root = self.lock.root().to_path_buf();
        self.failed = true;
        let staged = durability::prepare_native_files(&root, || self.prepare_publication_inner())?;
        self.failed = false;
        self.prepared();
        Ok(staged)
    }

    /// One off-gate durability scope covers catch-up and its prepared receipt.
    /// Files reach fsync here; the final publication still orders and drains the device.
    pub fn catch_up_and_prepare(
        &mut self,
        cut: crate::disk_pack::lifetime::GenerationHold,
    ) -> Result<CompactionPublication> {
        self.ensure_usable()?;
        let root = self.lock.root().to_path_buf();
        self.failed = true;
        let staged = durability::prepare_native_files(&root, || {
            self.catch_up_inner(cut)?;
            self.prepare_publication_inner()
        })?;
        self.failed = false;
        self.prepared();
        Ok(staged)
    }

    fn prepare_publication_inner(&mut self) -> Result<CompactionPublication> {
        let root = self.lock.root();
        for coordinate in &self.output.dependencies {
            self.protection.protect_path(&coordinate.path(root)?)?;
        }
        let expected = self.caught.current().clone();
        let mut current = self.output.current.clone();
        current.generation =
            publish::fresh_generation(root, expected.generation.max(current.generation - 1))?;
        current.previous_generation = expected.generation;
        current.frontier = expected.frontier;
        current.published_ms = artifact::unix_ms();
        current.required_features |= crate::disk_pack::FEATURE_LIVE_PART_COUNTS;
        let receipt = root.join(format!("{PENDING_GENERATION}{}", Uuid::new_v4()));
        let pointer = root.join(format!("CURRENT.pending-{}", Uuid::new_v4()));
        let pointer_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pointer)?;
        let mut staged = CompactionPublication {
            identity: self.identity,
            owner: std::sync::Arc::clone(&self.lock),
            _protection: self.protection.clone(),
            expected,
            current,
            parts: 0,
            root: root.to_path_buf(),
            receipt,
            pointer,
            pointer_file,
        };
        // Existing groups carry exact edit deltas between private rounds. A
        // newly grafted group requests one complete off-gate recount instead.
        if self.live.is_none() {
            self.live = Some(crate::disk_pack::live_parts::LiveParts::bootstrap(
                root,
                &staged.current,
                &self.output.dependencies,
            )?);
        }
        let live = self
            .live
            .as_ref()
            .expect("complete private live references");
        let cut = generation::read_receipt(root, self.caught.current())?;
        let written = generation::write_live_at_with_cache(
            root,
            &staged.current,
            live,
            Some(&cut),
            self.cached_receipt.as_ref(),
            staged.receipt.clone(),
        )?;
        staged.parts = written.dependencies;
        self.cached_receipt = Some(written);
        staged.pointer_file.write_all(&staged.current.encode()?)?;
        durability::sync_dirty_file(&staged.pointer_file)?;
        self.prepared();
        Ok(staged)
    }
}

impl CompactionPublication {
    /// A receipt-format transition must follow already admitted mutations.
    pub fn changes_required_features(&self) -> bool {
        self.current.required_features != self.expected.required_features
    }

    /// Exact replacement dependency count for pending mutation headroom.
    pub fn dependency_parts(&self) -> u64 {
        self.parts
    }

    pub(in crate::disk_pack) fn belongs_to(&self, output: &PreparedCompaction) -> bool {
        self.identity == output.identity && std::sync::Arc::ptr_eq(&self.owner, &output.lock)
    }

    pub(in crate::disk_pack) fn install(&mut self, lock: &WriteLock) -> Result<bool> {
        if !std::ptr::eq(lock, self.owner.as_ref())
            || artifact::current(&self.root)? != self.expected
        {
            return Err(corrupt(
                "prepared compaction disk root differs from its writer",
            ));
        }
        let generations = self.root.join("generations");
        let canonical = generations.join(self.current.generation.to_string());
        // Atomic mkdir is a portable no-replace claim. Existing receipts are
        // never overwritten or verified under the writer boundary.
        match fs::create_dir(&canonical) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        lock.files.invalidate()?;
        // The format's 65,536 dependency limit fits its receipt in one part.
        fs::rename(
            self.receipt.join("part-00000000"),
            canonical.join("part-00000000"),
        )?;
        durability::sync_directory(&File::open(&canonical)?)?;
        durability::sync_directory(&File::open(&generations)?)?;
        // This barrier also orders the newly installed receipt on macOS before
        // CURRENT's rename, even though its bytes were flushed off-gate.
        durability::sync_publication_file(&self.pointer_file)?;
        fs::rename(&self.pointer, self.root.join("CURRENT"))?;
        durability::sync_directory(&File::open(&self.root)?)?;
        Ok(true)
    }
}

impl Drop for CompactionPublication {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.pointer);
        cleanup_abandoned(&self.receipt);
    }
}

/// Exact temporary namespace only; unknown files remain for an operator.
pub(in crate::disk_pack) fn cleanup_abandoned(path: &Path) {
    let _ = fs::remove_file(path.join("part-00000000"));
    let _ = fs::remove_dir(path);
}
