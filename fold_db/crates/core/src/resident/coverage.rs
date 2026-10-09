//! Certificates for complete, query-requested hash partitions.
//! Certificates contain no record bodies and no global write counter.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub(crate) struct PartitionRead {
    molecule: String,
    hash: String,
    start: String,
    end: Option<String>,
    valid: AtomicBool,
    complete: AtomicBool,
}

pub(super) type CoverageRegistry =
    BTreeMap<(String, String), HashMap<(String, Option<String>), Arc<PartitionRead>>>;

impl ResidentGraph {
    /// Read only the requested page from a certified partition. Release the
    /// index lock before tip resolution; mutations invalidate the certificate.
    pub(crate) fn resolve_partition_window(
        &self,
        molecule: &str,
        hash: &str,
        window: &crate::schema::types::field::KeyWindow,
        include_tombstones: bool,
    ) -> Option<Vec<ResidentTip>> {
        use crate::schema::types::field::KeyWindow;
        use std::ops::Bound;
        let proof = self
            .partition_coverage
            .lock()
            .expect("partition coverage lock")
            .get(&(molecule.to_owned(), hash.to_owned()))?
            .get(&(String::new(), None))?
            .clone();
        if !proof.valid.load(Ordering::Acquire) || !proof.complete.load(Ordering::Acquire) {
            return None;
        }
        let (mut skip, limit, mut cursor) = match window {
            KeyWindow::Offset { offset, limit } => (*offset, *limit, None),
            KeyWindow::After { after, limit } if after.hash.as_deref() == Some(hash) => (
                0,
                *limit,
                Some(ResidentMoleculeKey::new(
                    hash,
                    after.range.as_deref().unwrap_or(""),
                )),
            ),
            KeyWindow::After { .. } => return None,
        };
        let start = ResidentMoleculeKey::new(hash, "");
        let end = ResidentMoleculeKey::new(format!("{hash}\0"), "");
        let mut output = Vec::new();
        while output.len() < limit {
            let ask = skip.saturating_add(limit - output.len()).clamp(1, 128);
            let keys = self
                .key_index
                .read()
                .expect("key index lock")
                .get(molecule)
                .map(|keys| {
                    keys.range((
                        cursor
                            .as_ref()
                            .map_or(Bound::Included(&start), Bound::Excluded),
                        Bound::Excluded(&end),
                    ))
                    .take(ask)
                    .cloned()
                    .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if keys.is_empty() {
                break;
            }
            cursor = keys.last().cloned();
            for key in keys {
                let tip = self.resolve_tip(molecule, &key.hash, &key.range)?.value;
                if !include_tombstones
                    && tip
                        .key_metadata
                        .as_ref()
                        .is_some_and(|meta| meta.tombstoned)
                {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                } else {
                    output.push(tip);
                }
                if output.len() == limit {
                    break;
                }
            }
        }
        if !proof.valid.load(Ordering::Acquire) {
            return None;
        }
        self.metrics.record_key_set_hit();
        Some(output)
    }

    /// Begin an uncapped partition interval read. Pages cannot certify coverage.
    pub(crate) fn begin_partition_interval_read(
        &self,
        molecule: &str,
        hash: &str,
        start: &str,
        end: Option<&str>,
    ) -> Arc<PartitionRead> {
        let read = Arc::new(PartitionRead {
            molecule: molecule.to_string(),
            hash: hash.to_string(),
            start: start.to_string(),
            end: end.map(str::to_string),
            valid: AtomicBool::new(true),
            complete: AtomicBool::new(false),
        });
        if molecule.len() + hash.len() + start.len() + end.map_or(0, str::len) > 1024 {
            read.valid.store(false, Ordering::Release);
            return read;
        }
        let mut coverage = self
            .partition_coverage
            .lock()
            .expect("partition coverage lock");
        if let Some(existing) = coverage
            .get(&(molecule.to_string(), hash.to_string()))
            .and_then(|intervals| intervals.get(&(start.to_string(), end.map(str::to_string))))
            .filter(|read| read.valid.load(Ordering::Acquire))
        {
            return Arc::clone(existing);
        }
        // Concurrent cold readers share the same validity ticket. Replacing
        // pending tickets would prevent certification under continuous load.
        // Bound only certificates: no tips or atoms are copied into this registry.
        if coverage.values().map(HashMap::len).sum::<usize>() >= 1024 {
            for old in coverage.values().flat_map(HashMap::values) {
                old.valid.store(false, Ordering::Release);
            }
            coverage.clear();
        }
        let intervals = coverage
            .entry((molecule.to_string(), hash.to_string()))
            .or_default();
        if intervals.len() >= 32 {
            for old in intervals.values() {
                old.valid.store(false, Ordering::Release);
            }
            intervals.clear();
        }
        if let Some(old) =
            intervals.insert((read.start.clone(), read.end.clone()), Arc::clone(&read))
        {
            old.valid.store(false, Ordering::Release);
        }
        read
    }

    /// Durable writers can bypass resident apply (generation swaps and repair).
    /// Invalidate only the certificates for the molecule that they change.
    pub(crate) fn invalidate_molecule_coverage(&self, molecule: &str) {
        self.invalidate_page_molecule(molecule);
        let mut coverage = self
            .partition_coverage
            .lock()
            .expect("partition coverage lock");
        let start = (molecule.to_string(), String::new());
        let end = (format!("{molecule}\0"), String::new());
        let keys: Vec<_> = coverage
            .range(start..end)
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            for old in coverage
                .remove(&key)
                .into_iter()
                .flat_map(HashMap::into_values)
            {
                old.valid.store(false, Ordering::Release);
                if old.complete.load(Ordering::Acquire) {
                    self.metrics.record_key_set_demote();
                }
            }
        }
    }

    pub(super) fn invalidate_partition(&self, molecule: &str, hash: &str) {
        self.invalidate_page_partition(molecule, hash);
        if let Some(intervals) = self
            .partition_coverage
            .lock()
            .expect("partition coverage lock")
            .remove(&(molecule.to_string(), hash.to_string()))
        {
            for old in intervals.values() {
                old.valid.store(false, Ordering::Release);
                if old.complete.load(Ordering::Acquire) {
                    self.metrics.record_key_set_demote();
                }
            }
        }
    }

    fn partition_interval_has_dirty(&self, read: &PartitionRead) -> bool {
        let start = ResidentMoleculeKey::new(&read.hash, &read.start);
        let end = read.end.as_ref().map_or_else(
            || ResidentMoleculeKey::new(format!("{}\0", read.hash), ""),
            |end| ResidentMoleculeKey::new(&read.hash, end),
        );
        {
            let index = self.key_index.read().expect("key index lock");
            let dirty = self.dirty.read().expect("resident dirty lock");
            if index.get(&read.molecule).is_some_and(|keys| {
                keys.range(start.clone()..end.clone()).any(|key| {
                    dirty.contains(&key.dirty_key(&read.molecule))
                        || dirty.contains(&DirtyKey::MoleculeTip {
                            molecule_uuid: read.molecule.clone(),
                            hash: key.hash.clone(),
                            range: key.range.clone(),
                        })
                })
            }) {
                return true;
            }
        }
        self.key_tombstones
            .read()
            .expect("key tombstone lock")
            .get(&read.molecule)
            .is_some_and(|keys| keys.range(start..end).next().is_some())
    }

    /// Install exactly the tips from an uncapped read, then certify coverage.
    /// Slot revisions guard every install. A mutation, durable completion, or
    /// eviction invalidates the read ticket and prevents certification.
    pub(crate) fn finish_partition_read(
        &self,
        read: &PartitionRead,
        tips: Vec<ResidentTip>,
    ) -> bool {
        if tips.iter().any(|tip| {
            tip.molecule_uuid != read.molecule
                || tip.hash != read.hash
                || tip.range < read.start
                || read.end.as_ref().is_some_and(|end| &tip.range >= end)
        }) {
            return false;
        }
        if !read.valid.load(Ordering::Acquire) || self.partition_interval_has_dirty(read) {
            return false;
        }
        let mut expected = Vec::with_capacity(tips.len());
        for tip in tips {
            if tip.molecule_uuid != read.molecule || tip.hash != read.hash {
                return false;
            }
            let observed = self.observe_slot(&tip.molecule_uuid, &tip.hash, &tip.range);
            // A write after the durable read invalidates the partition ticket.
            // The scoped slot lease prevents eviction from recycling identity
            // between this check and the conditional install.
            if !read.valid.load(Ordering::Acquire) {
                return false;
            }
            expected.push(ResidentMoleculeKey::new(&tip.hash, &tip.range));
            if self
                .rehydrate_tip_at(tip.clone(), Some(observed.revisions()))
                .is_none_or(|hit| hit.value != tip)
            {
                return false;
            }
        }
        let start = ResidentMoleculeKey::new(&read.hash, &read.start);
        let end = read.end.as_ref().map_or_else(
            || ResidentMoleculeKey::new(format!("{}\0", read.hash), ""),
            |end| ResidentMoleculeKey::new(&read.hash, end),
        );
        let snapshot = self.resident_key_set_range(&read.molecule, Some(&start), Some(&end));
        expected.sort_unstable();
        expected.dedup();
        if !snapshot.tombstones.is_empty() || snapshot.keys != expected {
            return false;
        }
        let _slots = self.slots.read().expect("resident slot lock");
        if !read.valid.load(Ordering::Acquire) || self.partition_interval_has_dirty(read) {
            return false;
        }
        read.complete.store(true, Ordering::Release);
        let complete = read.valid.load(Ordering::Acquire);
        if complete {
            self.metrics.record_key_set_complete();
        }
        complete
    }

    /// Return a consistent cached partition. Atom bodies are resolved later.
    pub(crate) fn resolve_partition_interval(
        &self,
        molecule: &str,
        hash: &str,
        start: &str,
        end: Option<&str>,
    ) -> Option<Vec<ResidentTip>> {
        let read = self
            .partition_coverage
            .lock()
            .expect("partition coverage lock")
            .get(&(molecule.to_string(), hash.to_string()))
            .and_then(|intervals| {
                intervals.values().find(|read| {
                    read.valid.load(Ordering::Acquire)
                        && read.complete.load(Ordering::Acquire)
                        && read.start.as_str() <= start
                        && match (read.end.as_deref(), end) {
                            (None, _) => true,
                            (Some(_), None) => false,
                            (Some(cover_end), Some(end)) => cover_end >= end,
                        }
                })
            })
            .cloned()?;
        if !read.valid.load(Ordering::Acquire) || !read.complete.load(Ordering::Acquire) {
            return None;
        }
        let start = ResidentMoleculeKey::new(hash, start);
        let end = end.map_or_else(
            || ResidentMoleculeKey::new(format!("{hash}\0"), ""),
            |end| ResidentMoleculeKey::new(hash, end),
        );
        let snapshot = self.resident_key_set_range(molecule, Some(&start), Some(&end));
        if !snapshot.tombstones.is_empty() {
            return None;
        }
        let mut tips = Vec::with_capacity(snapshot.keys.len());
        for key in snapshot.keys {
            tips.push(self.resolve_tip(molecule, &key.hash, &key.range)?.value);
        }
        if !read.valid.load(Ordering::Acquire) {
            return None;
        }
        self.metrics.record_key_set_hit();
        Some(tips)
    }
}
