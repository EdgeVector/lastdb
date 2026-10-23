//! Carry a bounded recent locator delta into the normal writer's next commit.
use super::PreparedCompaction;
use crate::disk_pack::lifetime::GenerationHold;
use crate::disk_pack::snapshot::PreparedMutation;
use crate::disk_pack::tree_cursor::TreeCursor;
use crate::disk_pack::{artifact, corrupt, Current, DirectoryEntry, Location};
use crate::Result;

pub(in crate::disk_pack) struct MergeDelta {
    pub(in crate::disk_pack) identity: uuid::Uuid,
    pub(in crate::disk_pack) expected: Current,
    pub(in crate::disk_pack) mutations: Vec<PreparedMutation>,
    _cut: GenerationHold,
}

struct Budget {
    limit: usize,
    examined: usize,
    bytes: usize,
}

impl PreparedCompaction {
    pub(in crate::disk_pack) fn has_identity(&self, identity: uuid::Uuid) -> bool {
        self.identity == identity
    }

    pub(in crate::disk_pack) fn publication_base(
        &mut self,
    ) -> crate::disk_pack::publish::PublicationBase {
        crate::disk_pack::publish::PublicationBase {
            current: self.output.current.clone(),
            live: std::mem::take(&mut self.live),
            cached: self.cached_receipt.take(),
        }
    }

    pub(in crate::disk_pack) fn dependency_parts(&self) -> u64 {
        self.live.coordinates().len() as u64
    }

    pub(in crate::disk_pack) fn reserve_merge(&mut self, bytes: u64) -> Result<()> {
        let total = self
            ._space
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| corrupt("compaction merge reservation overflow"))?;
        let extension = self._space.reserve_extension(&self.policy, total)?;
        self._space.absorb(extension);
        Ok(())
    }

    /// No output is written here. The next normal publication builds one set
    /// of metadata for both these existing locations and the incoming writes.
    pub(in crate::disk_pack) fn merge_delta(
        &self,
        cut: GenerationHold,
        limit: usize,
    ) -> Result<MergeDelta> {
        self.ensure_usable()?;
        super::concurrent::ensure_newer_compatible(self.caught.current(), cut.current())?;
        if cut.root() != self.lock.root() {
            return Err(corrupt("compaction merge cut belongs to another home"));
        }
        let mut delta = MergeDelta {
            identity: self.identity,
            expected: cut.current().clone(),
            mutations: Vec::new(),
            _cut: cut,
        };
        if delta.expected.frontier == self.caught.current().frontier {
            return Ok(delta);
        }
        let root = self.lock.root();
        let current = &delta.expected;
        let mut call = artifact::metadata_call(root, current.clone());
        let mut groups = TreeCursor::recent(
            &mut call,
            current.directory.clone(),
            self.caught.current().generation,
        )?;
        let mut examined = 0usize;
        let mut budget = Budget {
            limit: limit.min(4096),
            examined: 0,
            bytes: 0,
        };
        while let Some((key, value)) = groups.next(&mut call)? {
            examined += 1;
            if examined > 4096 {
                return Err(behind());
            }
            let entry = DirectoryEntry::decode(&value)?;
            let collection = super::compact::collection(&key, entry.group)?;
            self.collect_merge_group(
                current,
                &collection,
                entry,
                &mut budget,
                &mut delta.mutations,
            )?;
            call = artifact::metadata_call(root, current.clone());
        }
        Ok(delta)
    }

    fn collect_merge_group(
        &self,
        current: &Current,
        collection: &str,
        entry: DirectoryEntry,
        budget: &mut Budget,
        mutations: &mut Vec<PreparedMutation>,
    ) -> Result<()> {
        let mut call = artifact::metadata_call(self.lock.root(), current.clone());
        let mut records =
            TreeCursor::recent(&mut call, entry.locator, self.caught.current().generation)?;
        while let Some((key, value)) = records.next(&mut call)? {
            budget.examined += 1;
            if budget.examined > 4096 {
                return Err(behind());
            }
            let location = Location::decode_for_current(&value, current)?;
            if location.version.batch > current.frontier {
                return Err(corrupt(
                    "compaction merge location exceeds its current frontier",
                ));
            }
            if location.version.batch > self.caught.current().frontier {
                budget.bytes = budget
                    .bytes
                    .saturating_add(key.len())
                    .saturating_add(collection.len())
                    .saturating_add(256);
                if mutations.len() >= budget.limit || budget.bytes > 16 * 1024 * 1024 {
                    return Err(behind());
                }
                mutations
                    .try_reserve(1)
                    .map_err(|_| crate::disk_pack::resource())?;
                mutations.push(PreparedMutation {
                    collection: collection.to_owned(),
                    group: entry.group,
                    key: String::from_utf8(key)
                        .map_err(|_| corrupt("compaction merge key is not UTF-8"))?,
                    location,
                });
            }
            if budget.examined % 128 == 0 {
                call = artifact::metadata_call(self.lock.root(), current.clone());
            }
        }
        Ok(())
    }
}

fn behind() -> crate::Error {
    crate::Error::WriteCapacity(
        "compaction_behind: merge delta exceeds its bounded metadata budget".into(),
    )
}
