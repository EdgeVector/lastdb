use super::tree_cursor::TreeCursor;
use super::{corrupt, DirectoryEntry, Location, MutationVersion, ReadCall, Source};
use crate::Result;

/// One encoded molecule/hash scope from the caller's held key and placement codecs.
/// Explicit artifact groups are not a production placement-codec allocation.
pub struct HashScope {
    collection: String,
    prefix: String,
    groups: Vec<u16>,
}

impl HashScope {
    /// `prefix` ends at the first NUL separator, after the complete encoded hash.
    /// The caller supplies every configured group for that hash, at most sixteen.
    pub fn new(collection: String, prefix: String, mut groups: Vec<u16>) -> Result<Self> {
        if collection.len() > super::KEY_BYTES - 4
            || prefix.len() > super::KEY_BYTES
            || prefix.find('\0') != prefix.len().checked_sub(1)
            || prefix.len() < 2
            || groups.is_empty()
            || groups.len() > 16
            || groups.iter().any(|group| *group >= 1024)
        {
            return Err(crate::Error::Config(
                "disk-pack range requires one complete encoded hash and 1..16 exact groups".into(),
            ));
        }
        groups.sort_unstable();
        if groups.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(crate::Error::Config(
                "disk-pack range groups must be distinct".into(),
            ));
        }
        Ok(Self {
            collection,
            prefix,
            groups,
        })
    }
}

/// Half-open encoded range suffix bounds within the held hash.
pub struct RangeBounds {
    /// Inclusive encoded range suffix; empty means the first range under this hash.
    pub start: String,
    /// Exclusive encoded range suffix; None means the exact end of this hash.
    pub end: Option<String>,
}

/// One exact record selected by an ordered hash range.
pub struct RangeRecord {
    /// Complete storage key, in the held codec's order.
    pub key: String,
    /// Unchanged stored body bytes.
    pub body: Vec<u8>,
    /// Physical complete-batch mutation order.
    pub version: MutationVersion,
}

/// A bounded page. A short successful page always exhausts the requested range,
/// unless `resume` is set.
pub struct RangePage {
    /// Ordered live records; tombstones count as work but do not appear here.
    pub records: Vec<RangeRecord>,
    /// True only after all configured group cursors reach the exact upper bound.
    pub exhausted: bool,
    /// Set only by [`HashRangeCursor::next_page_yielding`]. The call ran out of
    /// key budget in a run of tombstones. Every key of the range up to and
    /// including this key was examined and none of them is live, so the next
    /// call continues at the least key above it. `records` is empty then.
    pub resume: Option<Vec<u8>>,
}

struct GroupCursor {
    cursor: TreeCursor,
    pending: Option<(Vec<u8>, Location)>,
    finished: bool,
}

/// In-process continuation tied to one captured CURRENT and one shared ReadCall.
/// It is not an unauthenticated external continuation token.
pub struct HashRangeCursor<'a> {
    call: &'a mut ReadCall,
    collection: String,
    end: Vec<u8>,
    groups: Vec<GroupCursor>,
    failed: bool,
}

impl ReadCall {
    /// Seek only the configured groups of one named hash at the captured generation.
    pub fn hash_range(
        &mut self,
        scope: HashScope,
        bounds: RangeBounds,
    ) -> Result<HashRangeCursor<'_>> {
        let start_bytes = scope
            .prefix
            .len()
            .checked_add(bounds.start.len())
            .filter(|length| *length <= super::KEY_BYTES)
            .ok_or_else(|| corrupt("range start exceeds key limit"))?;
        let end_bytes = scope
            .prefix
            .len()
            .checked_add(bounds.end.as_ref().map_or(0, String::len))
            .filter(|length| *length <= super::KEY_BYTES)
            .ok_or_else(|| corrupt("range end exceeds key limit"))?;
        self.charge((start_bytes + end_bytes + scope.collection.len()) as u64)?;
        let start = format!("{}{}", scope.prefix, bounds.start).into_bytes();
        let end = if let Some(end) = bounds.end {
            format!("{}{}", scope.prefix, end).into_bytes()
        } else {
            let mut end = scope.prefix.into_bytes();
            *end.last_mut().expect("nonempty scope") = 1;
            end
        };
        if start.len() > super::KEY_BYTES || end.len() > super::KEY_BYTES || start > end {
            return Err(crate::Error::Config(
                "disk-pack encoded range bounds are invalid".into(),
            ));
        }
        self.range_in_groups(scope.collection, start, end, &scope.groups)
    }

    pub(super) fn range_in_groups(
        &mut self,
        collection: String,
        start: Vec<u8>,
        end: Vec<u8>,
        source_groups: &[u16],
    ) -> Result<HashRangeCursor<'_>> {
        if start.len() > super::KEY_BYTES
            || end.len() > super::KEY_BYTES
            || source_groups.is_empty()
            || source_groups.len() > 1024
            || source_groups.iter().any(|group| *group >= 1024)
        {
            return Err(corrupt("administrative range exceeds format bounds"));
        }
        self.charge((source_groups.len() * std::mem::size_of::<GroupCursor>()) as u64)?;
        let mut groups = Vec::with_capacity(source_groups.len());
        let directory = self.current.directory.clone();
        for &group in source_groups {
            if start >= end {
                break;
            }
            let key = Self::directory_key(&collection, group)?;
            let Some(value) = self.find(&directory, &key)? else {
                continue;
            };
            let entry = DirectoryEntry::decode(&value)?;
            if entry.group != group || entry.converted_frontier > self.current.frontier {
                return Err(corrupt("range directory coordinate or frontier"));
            }
            groups.push(GroupCursor {
                cursor: TreeCursor::seek(self, entry.locator, &start)?,
                pending: None,
                finished: start == end,
            });
        }
        Ok(HashRangeCursor {
            call: self,
            collection,
            end,
            groups,
            failed: false,
        })
    }
}

impl HashRangeCursor<'_> {
    /// Charge a selected pending-write value against this cursor's original budget.
    /// The caller copies its body only after this succeeds.
    pub fn account_memory_record(&mut self, key_bytes: usize, body_bytes: usize) -> Result<()> {
        self.call.examine()?;
        let bytes = key_bytes
            .checked_add(body_bytes)
            .ok_or_else(super::resource)?;
        self.call.charge(bytes as u64)
    }

    /// Continue only with the original held call. Budget failure poisons this cursor.
    /// An error never returns a short successful page or an exhausted result.
    pub fn next_page(&mut self, limit: usize) -> Result<RangePage> {
        self.next(limit, false)
    }

    /// Like [`Self::next_page`], but a key budget that runs out inside a run of
    /// tombstones is not an error. The page is then empty and `resume` names the
    /// last tombstone examined, so a caller walks a range of dead keys one call
    /// at a time with no failed read. Any other budget failure, and a failure
    /// after a live record was selected, still returns the error.
    pub fn next_page_yielding(&mut self, limit: usize) -> Result<RangePage> {
        self.next(limit, true)
    }

    fn next(&mut self, limit: usize, yielding: bool) -> Result<RangePage> {
        if self.failed {
            return Err(corrupt("range cursor requires a fresh call after an error"));
        }
        if limit == 0 || limit as u64 > self.call.budget.keys {
            return Err(crate::Error::Config(
                "range page limit must fit the positive call key budget".into(),
            ));
        }
        self.failed = true;
        let result = self.page(limit, yielding)?;
        self.failed = false;
        Ok(result)
    }

    fn page(&mut self, limit: usize, yielding: bool) -> Result<RangePage> {
        self.call.charge(
            (limit
                * (std::mem::size_of::<RangeRecord>() + std::mem::size_of::<(String, Location)>()))
                as u64,
        )?;
        let mut selected_records = Vec::new();
        selected_records
            .try_reserve_exact(limit)
            .map_err(|_| super::resource())?;
        let mut exhausted = false;
        let mut last_dead: Option<Vec<u8>> = None;
        while selected_records.len() < limit {
            match self.fill_pending() {
                Err(crate::Error::ReadBudgetExceeded)
                    if yielding && selected_records.is_empty() && last_dead.is_some() =>
                {
                    self.failed = false;
                    return Ok(RangePage {
                        records: Vec::new(),
                        exhausted: false,
                        resume: last_dead,
                    });
                }
                other => other?,
            }
            let selected = self
                .groups
                .iter()
                .enumerate()
                .filter_map(|(number, group)| group.pending.as_ref().map(|(key, _)| (number, key)))
                .min_by(|(_, a), (_, b)| a.cmp(b))
                .map(|(number, _)| number);
            let Some(selected) = selected else {
                exhausted = true;
                break;
            };
            let (key, location) = self.groups[selected].pending.take().expect("selected key");
            if self.groups.iter().any(|group| {
                group
                    .pending
                    .as_ref()
                    .is_some_and(|(other, _)| other == &key)
            }) {
                return Err(corrupt(
                    "range key occurs in more than one configured group",
                ));
            }
            if location.source == Source::Tombstone {
                if yielding && selected_records.is_empty() {
                    last_dead = Some(key);
                }
                continue;
            }
            let key =
                String::from_utf8(key).map_err(|_| corrupt("range storage key is not UTF-8"))?;
            selected_records.push((key, location));
        }
        let bodies = self
            .call
            .read_selected_records(&self.collection, &selected_records)?;
        let records = selected_records
            .into_iter()
            .zip(bodies)
            .map(|((key, location), body)| RangeRecord {
                key,
                body,
                version: location.version,
            })
            .collect();
        Ok(RangePage {
            records,
            exhausted,
            resume: None,
        })
    }

    fn fill_pending(&mut self) -> Result<()> {
        for group in &mut self.groups {
            if group.finished || group.pending.is_some() {
                continue;
            }
            match group.cursor.next(self.call)? {
                Some((key, value)) if key < self.end => {
                    let location = Location::decode_for_current(&value, &self.call.current)?;
                    if location.version.batch > self.call.current.frontier {
                        return Err(corrupt("range location exceeds committed frontier"));
                    }
                    group.pending = Some((key, location));
                }
                _ => group.finished = true,
            }
        }
        Ok(())
    }
}
