use super::*;

/// Record-byte residue of one hash group: how many record bytes the live
/// index still addresses versus how many are dead — superseded put records,
/// the put records of deleted ids, and the delete markers themselves.
///
/// Both totals are **plaintext record lengths**, the unit every put and delete
/// appends in, not on-disk block allocation. A store whose groups are sealed
/// or compressed at rest still reports the same ratio, because both sides are
/// measured in the same unit; the ratio is what a compaction trigger needs.
///
/// A LastStore delete is an append: it makes the id unreachable and adds a
/// marker, and no byte leaves the segment until the group is rewritten. Without
/// this counter the only observable on a deleted plane is filesystem block
/// slack (`st_blocks` past `st_size`), which a delete does not change — so a
/// plane could hold gigabytes of deleted rows and read as 0% reclaimable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupResidue {
    /// Record bytes the live index addresses.
    pub live_bytes: u64,
    /// Record bytes no live id addresses (superseded puts, deleted puts, and
    /// delete markers).
    pub dead_bytes: u64,
}

/// Residue of one collection, summed over every hash group on disk.
///
/// Produced by [`LastStore::collection_residue`] without loading a single cold
/// group: a resident group answers from its in-memory counters, a cold group
/// answers from its newest sorted seal or the residue its id sidecar recorded,
/// and a group with neither is counted under `unknown_bytes` (its on-disk
/// size) so a trigger can see how much of the plane it could not measure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CollectionResidue {
    /// Groups enumerated on disk (plus resident groups the directory listing
    /// did not show yet).
    pub groups: u64,
    /// Groups answered from a resident handle (exact, current).
    pub groups_resident: u64,
    /// Groups answered from an id sidecar (exact as of the sidecar's stamps;
    /// any records appended since count as live under `unknown_bytes`).
    pub groups_sidecar: u64,
    /// Groups answered from sorted seal metadata. A newer append tail counts
    /// as unknown bytes; the sealed counters describe the last seal.
    pub groups_sealed: u64,
    /// Groups with no resident handle and no usable persisted residue.
    pub groups_unknown: u64,
    /// Record bytes live ids address, over the groups that could answer.
    pub live_bytes: u64,
    /// Dead record bytes, over the groups that could answer.
    pub dead_bytes: u64,
    /// On-disk bytes of groups (or appended suffixes) whose residue is not
    /// known. Never counted as dead.
    pub unknown_bytes: u64,
}

impl CollectionResidue {
    /// Dead record bytes as a fraction of measured record bytes, in basis
    /// points. Zero when nothing was measured.
    #[must_use]
    pub fn dead_bps(&self) -> u64 {
        let measured = self.live_bytes.saturating_add(self.dead_bytes);
        if measured == 0 {
            return 0;
        }
        ((u128::from(self.dead_bytes) * 10_000) / u128::from(measured)) as u64
    }
}

/// File name of a retire receipt, directly under the store root.
///
/// A missing file is a no-op. The file is not a collection and is not a scan
/// of `tips`.
pub const RETIRED_GROUPS_RECEIPT_FILE: &str = "retired-groups.receipt";

/// One hash group named by a retire receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredGroupId {
    /// Collection directory under `data/`.
    pub collection: String,
    /// Shard number. Not the on-disk hex name.
    pub shard: u16,
    /// Hash-group number. Not the on-disk hex name.
    pub group: u32,
}

/// Whether a retired-group compact may start.
///
/// `footprint_stop_bytes` is the caller's pressure stop. Fold passes
/// `pressure_footprint_target_bytes` (4 GiB, then the RAM minimum), the same
/// stop `footprint_evict_should_stop` uses while host pressure is high.
/// This store does not read `phys_footprint` itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredCompactGate {
    /// Host pressure is high. Either the store flag or a fresh sample.
    pub pressure_high: bool,
    /// Current `phys_footprint`. `None` is not a reading, not zero.
    pub phys_footprint_bytes: Option<u64>,
    /// Skip when footprint is strictly over this stop.
    pub footprint_stop_bytes: u64,
}

impl RetiredCompactGate {
    /// `true` when the whole pass must not open a group.
    #[must_use]
    pub fn blocks(self) -> bool {
        self.pressure_high
            || self
                .phys_footprint_bytes
                .is_some_and(|bytes| bytes > self.footprint_stop_bytes)
    }
}

/// Groups named in `path`.
///
/// `Ok(None)` when the file is absent. That is a no-op, not an error.
/// Each line is `collection shard group` in decimal. `#` comments and blank
/// lines are ignored.
pub fn read_retire_receipt(path: &Path) -> Result<Option<Vec<RetiredGroupId>>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut groups = Vec::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let collection = fields.next().unwrap_or("");
        let shard_text = fields.next().unwrap_or("");
        let group_text = fields.next().unwrap_or("");
        if collection.is_empty()
            || shard_text.is_empty()
            || group_text.is_empty()
            || fields.next().is_some()
        {
            return Err(Error::Config(format!(
                "retire receipt line {} must be collection shard group",
                index + 1
            )));
        }
        let shard = shard_text.parse::<u16>().map_err(|_| {
            Error::Config(format!("retire receipt line {} has a bad shard", index + 1))
        })?;
        let group = group_text.parse::<u32>().map_err(|_| {
            Error::Config(format!("retire receipt line {} has a bad group", index + 1))
        })?;
        groups.push(RetiredGroupId {
            collection: collection.to_string(),
            shard,
            group,
        });
    }
    Ok(Some(groups))
}
