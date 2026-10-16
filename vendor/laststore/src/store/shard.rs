use super::*;

/// One group's address: collection, shard, and hash-group index.
///
/// `group` is `None` on a segment-log home, which has no hash groups.
/// Embedders name this type because a durable batch flushes the keys it
/// wrote. The alias stays a tuple so existing match sites keep their shape.
pub type ShardKey = (String, u16, Option<u32>);
pub(super) type ShardHandle = Arc<Mutex<Shard>>;
pub(super) type FrameKey = (Uuid, u64);

#[derive(Clone)]
pub(super) struct FrameDiskLoc {
    pub(super) path: PathBuf,
    pub(super) disk_offset: u64,
    pub(super) disk_len: u64,
}

#[derive(Default)]
pub(super) struct Shard {
    pub(super) dir: PathBuf,
    pub(super) collection: String,
    pub(super) shard: u16,
    pub(super) data_key: Option<[u8; 32]>,
    pub(super) policy: CollectionPolicy,
    /// Legacy homes keep their compatibility index here. Sorted homes keep
    /// only open-tail entries; None hides a key in every older sealed segment.
    pub(super) index: BTreeMap<String, Option<Loc>>,
    pub(super) sorted_segments: Vec<sorted::Segment>,
    pub(super) sorted_mode: bool,
    /// Legacy encrypted source files still require conversion before a new
    /// sorted tail can use its generation-scoped directory.
    pub(super) sorted_legacy_encrypted: bool,
    pub(super) sorted_rollback_buffer: bool,
    pub(super) sorted_tail_directory_dirty: bool,
    /// An immutable append is visible; its staging link and directory barrier
    /// retire only after the transaction records the inverse operation.
    pub(super) pending_sorted_publish: Option<PathBuf>,
    pub(super) max_sorted_tail_bytes: u64,
    pub(super) plain_high_seq: u64,
    /// Sum of the heap capacities owned by `index` keys.
    ///
    /// Residency refresh runs on point operations. Keeping this total beside
    /// the map makes full key accounting O(1) instead of walking the complete
    /// group index after each read or write.
    pub(super) index_key_bytes: u64,
    pub(super) values: HashMap<String, Vec<u8>>,
    pub(super) seg_bytes: HashMap<u64, Vec<u8>>,
    pub(super) frame_cache: HashMap<FrameKey, Vec<u8>>,
    pub(super) frame_cache_order: VecDeque<FrameKey>,
    pub(super) frame_locs: HashMap<FrameKey, FrameDiskLoc>,
    /// Sum of the heap capacities owned by `frame_locs` paths.
    ///
    /// Residency refresh runs on point operations, so walking every frame
    /// location here would make a point read scale with the complete group.
    pub(super) frame_loc_path_bytes: u64,
    pub(super) open_buf: Vec<u8>,
    /// File offset of `open_buf[0]` within the open segment.
    ///
    /// The open tail used to be held whole, from offset 0, because `read_at`
    /// served the last segment straight out of it. That charged every group
    /// that had ever taken a write for up to `max_segment_bytes` of bytes the
    /// file already had — the single largest thing left in a resident group's
    /// warm-set charge once #942 gave up `seg_bytes` and `values`.
    ///
    /// With a base, `open_buf` may hold only the suffix past `base`, and the
    /// prefix is read back off disk exactly like a sealed segment. The
    /// invariant that makes that safe is `open_buf_base <= file_len`: the base
    /// only ever advances to a point already written, so anything below it is
    /// on disk. Sorted frame AEAD uses logical plaintext offsets here and
    /// frame locations to read flushed records; legacy frame AEAD keeps base 0.
    pub(super) open_buf_base: u64,
    pub(super) segments: Vec<u64>,
    pub(super) open_len: u64,
    /// Plaintext bytes of the open segment already written to `open_file`.
    pub(super) file_len: u64,
    pub(super) open_file: Option<CountedFile>,
    /// The store-wide open-descriptor gauge this group's append handle spends.
    ///
    /// Wired in [`LastStore::shard_handle_at`] as the group is admitted. A
    /// `Default` shard gets its own unshared counter, which is what throwaway
    /// verify shards want: they open files no warm set is accounting for.
    pub(super) fd_gauge: Arc<AtomicUsize>,
    pub(super) open_chunk_uuid: Option<Uuid>,
    pub(super) next_frame_counter: u64,
    pub(super) open_frame_start_csn: Option<u64>,
    pub(super) max_csn: u64,
    pub(super) dirty_ops: u32,
    pub(super) dirty_bytes: u64,
    /// Record-byte residue of this group. Maintained by every index mutation
    /// (`insert_index` / `remove_index` / `replace_index` / `clear_index`)
    /// and by every delete-marker append, so it is exact for a resident group
    /// without walking the index. Rebuilt from the segment scan on load, so a
    /// cold group is exact the moment it is resident; persisted into the id
    /// sidecar so a cold group can answer without a load.
    pub(super) residue: GroupResidue,
    /// Segment stamps this group's on-disk id sidecar is known to describe, or
    /// `None` if this run has no such proof.
    ///
    /// Seeded at load from the sidecar's own header and updated on every write,
    /// so it tracks the file rather than just this handle's writes. Both
    /// sidecar writers compare against it and skip a group whose sidecar is
    /// already valid: [`LastStore::persist_key_sidecars`] so repeat calls cost
    /// a readdir per group instead of rewriting every id list, and eviction so
    /// a group cycling through the warm set does not rewrite identical bytes
    /// on what is otherwise a pure read path.
    pub(super) sidecar_stamps: Option<Vec<keysidecar::SegmentStamp>>,
    pub(super) frame_compression_stats: Arc<Mutex<BTreeMap<String, FrameCompressionStats>>>,
}

impl Shard {
    pub(super) fn uses_sorted_index(&self) -> bool {
        self.sorted_mode || !self.sorted_segments.is_empty()
    }

    pub(super) fn cache_legacy_bodies(&self) -> bool {
        self.data_key.is_none() && !self.uses_sorted_index()
    }

    /// True when `stamps` (from disk) is this handle's view of the files.
    ///
    /// A scan lease uses this to detect a later writer that flushed a
    /// different Arc. Sealed seqs are immutable, so a new seq or a longer
    /// final segment means this image is stale.
    ///
    /// Sidecar validity is a different question (`sidecar_stamps`). A corrupt
    /// sidecar is `None` and must still be repaired when the handle matches
    /// disk (`eviction_repairs_a_corrupt_sidecar`).
    pub(super) fn matches_segment_stamps(&self, stamps: &[keysidecar::SegmentStamp]) -> bool {
        if stamps.len() != self.segments.len() {
            return false;
        }
        if stamps
            .iter()
            .zip(self.segments.iter())
            .any(|(stamp, seq)| stamp.seq != *seq)
        {
            return false;
        }
        match stamps.last() {
            None => self.file_len == 0,
            Some(last) => last.len == self.file_len,
        }
    }

    pub(super) fn lookup(&self, id: &str) -> Result<Option<Loc>> {
        if let Some(location) = self.index.get(id) {
            return Ok(*location);
        }
        for (segment, sealed) in self.sorted_segments.iter().enumerate().rev() {
            if let Some(record) = sealed.find(self.data_key.as_ref(), id)? {
                return Ok(record.body.map(|(offset, body_len)| Loc::Sorted {
                    segment,
                    offset,
                    body_len,
                    len: 7 + record.key.len() as u64 + body_len as u64,
                }));
            }
        }
        Ok(None)
    }

    /// Visit current live keys in order without building a complete key map.
    pub(super) fn visit_keys(
        &self,
        start: &str,
        end: Option<&str>,
        mut visit: impl FnMut(&str, Loc) -> bool,
    ) -> Result<u64> {
        let mut visited = 0u64;
        if self.sorted_segments.is_empty() {
            for (id, location) in self.index.range(start.to_string()..) {
                if end.is_some_and(|end| id.as_str() >= end) {
                    break;
                }
                if let Some(location) = location {
                    visited += 1;
                    if !visit(id, *location) {
                        break;
                    }
                }
            }
            return Ok(visited);
        }
        let mut merged = Merge::new(
            &self.sorted_segments,
            self.data_key.as_ref(),
            &self.index,
            start,
        )?;
        while let Some(record) = merged.next_key()? {
            if end.is_some_and(|end| record.key.as_str() >= end) {
                break;
            }
            let Some(location) = record.body else {
                continue;
            };
            let location = match location {
                BodyLocation::Tail(location) => location,
                BodyLocation::Sealed {
                    segment,
                    offset,
                    length,
                } => Loc::Sorted {
                    segment,
                    offset,
                    body_len: length,
                    len: 7 + record.key.len() as u64 + length as u64,
                },
            };
            visited += 1;
            if !visit(&record.key, location) {
                break;
            }
        }
        Ok(visited)
    }

    pub(super) fn live_locations(&self) -> Result<Vec<(String, Loc)>> {
        let mut entries = Vec::new();
        self.visit_keys("", None, |key, location| {
            entries.push((key.to_string(), location));
            true
        })?;
        Ok(entries)
    }

    pub(super) fn live_keys(&self) -> Result<BTreeSet<String>> {
        let mut keys = BTreeSet::new();
        self.visit_keys("", None, |key, _| {
            keys.insert(key.to_string());
            true
        })?;
        Ok(keys)
    }

    pub(super) fn insert_index(&mut self, id: String, loc: Loc) -> Result<()> {
        let previous_len = self.lookup(&id)?.map(|location| location.record_len());
        self.insert_index_known(id, loc, previous_len);
        Ok(())
    }

    // Resolve the previous record before append. A seal can replace physical
    // locations, but it preserves the old live record's encoded length.
    // Publication after append must not perform another fallible disk read.
    pub(super) fn insert_index_known(&mut self, id: String, loc: Loc, previous_len: Option<u64>) {
        let new_len = loc.record_len();
        if let Some(old_len) = previous_len {
            self.residue.live_bytes = self.residue.live_bytes.saturating_sub(old_len);
            self.residue.dead_bytes = self.residue.dead_bytes.saturating_add(old_len);
        }
        if matches!(loc, Loc::Sorted { .. }) {
            if let Some((key, _)) = self.index.remove_entry(&id) {
                self.index_key_bytes = self.index_key_bytes.saturating_sub(key.capacity() as u64);
            }
            self.residue.live_bytes = self.residue.live_bytes.saturating_add(new_len);
            return;
        }
        match self.index.entry(id) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.insert(Some(loc));
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.index_key_bytes = self
                    .index_key_bytes
                    .saturating_add(entry.key().capacity() as u64);
                entry.insert(Some(loc));
            }
        }
        self.residue.live_bytes = self.residue.live_bytes.saturating_add(new_len);
    }

    pub(super) fn remove_index(&mut self, id: &str) -> Result<()> {
        let previous_len = self.lookup(id)?.map(|location| location.record_len());
        self.remove_index_known(id, previous_len);
        Ok(())
    }

    pub(super) fn remove_index_known(&mut self, id: &str, previous_len: Option<u64>) {
        self.remove_index_known_at(id, previous_len, None);
    }

    pub(super) fn remove_index_known_at(
        &mut self,
        id: &str,
        previous_len: Option<u64>,
        location: Option<Loc>,
    ) {
        if let Some(old_len) = previous_len {
            self.residue.live_bytes = self.residue.live_bytes.saturating_sub(old_len);
            self.residue.dead_bytes = self.residue.dead_bytes.saturating_add(old_len);
        }
        if !self.uses_sorted_index() || matches!(location, Some(Loc::Sorted { .. })) {
            if let Some((id, _)) = self.index.remove_entry(id) {
                self.index_key_bytes = self.index_key_bytes.saturating_sub(id.capacity() as u64);
            }
        } else {
            match self.index.entry(id.to_string()) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.insert(None);
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    self.index_key_bytes = self
                        .index_key_bytes
                        .saturating_add(entry.key().capacity() as u64);
                    entry.insert(None);
                }
            }
        }
    }

    /// Account one delete marker appended to this group. The marker makes an
    /// id unreachable and is itself dead the moment it lands: only a rewrite
    /// removes it.
    pub(super) fn note_delete_marker(&mut self, marker_len: u64) {
        self.residue.dead_bytes = self.residue.dead_bytes.saturating_add(marker_len);
    }

    /// Adopt a freshly rewritten index. Every record it addresses was just
    /// written by the rewrite and nothing else is in the new segment, so the
    /// group has no dead bytes.
    pub(super) fn replace_index(&mut self, index: BTreeMap<String, Loc>) {
        self.index_key_bytes = index.keys().map(|id| id.capacity() as u64).sum::<u64>();
        self.residue = GroupResidue {
            live_bytes: index.values().map(Loc::record_len).sum(),
            dead_bytes: 0,
        };
        self.index = index
            .into_iter()
            .map(|(key, location)| (key, Some(location)))
            .collect();
    }

    pub(super) fn clear_index(&mut self) {
        self.index.clear();
        self.index_key_bytes = 0;
        self.residue = GroupResidue::default();
    }

    pub(super) fn insert_frame_loc(&mut self, key: FrameKey, loc: FrameDiskLoc) {
        let path_bytes = loc.path.capacity() as u64;
        if let Some(previous) = self.frame_locs.insert(key, loc) {
            self.frame_loc_path_bytes = self
                .frame_loc_path_bytes
                .saturating_sub(previous.path.capacity() as u64);
        }
        self.frame_loc_path_bytes = self.frame_loc_path_bytes.saturating_add(path_bytes);
    }

    pub(super) fn replace_frame_locs(&mut self, frame_locs: HashMap<FrameKey, FrameDiskLoc>) {
        self.frame_loc_path_bytes = frame_locs
            .values()
            .map(|loc| loc.path.capacity() as u64)
            .sum::<u64>();
        self.frame_locs = frame_locs;
    }

    pub(super) fn clear_frame_locs(&mut self) {
        self.frame_locs.clear();
        self.frame_loc_path_bytes = 0;
    }

    pub(super) fn rename_frame_loc_paths(&mut self, src: &Path, dst: &Path) {
        let mut removed = 0u64;
        let mut added = 0u64;
        for loc in self.frame_locs.values_mut() {
            if loc.path == src {
                removed = removed.saturating_add(loc.path.capacity() as u64);
                loc.path = dst.to_path_buf();
                added = added.saturating_add(loc.path.capacity() as u64);
            }
        }
        self.frame_loc_path_bytes = self
            .frame_loc_path_bytes
            .saturating_sub(removed)
            .saturating_add(added);
    }
}
