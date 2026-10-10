//! The apply pass. It rewrites the groups that hold a match.
//!
//! The group rewrite uses the PR 1480 primitives in the same order as
//! `maintenance_shrink`:
//!
//! 1. `remove_index` drops each matched key from the group in memory.
//! 2. `compact_shard` writes the new segment, syncs the directory, and
//!    deletes the older segments lowest first.
//! 3. `finish_legacy_rewrite` deletes any older segment that remains.
//!
//! Before each load of a group, `tail` checks that the load cuts nothing. A
//! group that changed since the count pass, or a new segment that is torn,
//! ends the run with a gate mismatch and leaves the bytes as they are.
//!
//! A kill after step 2 writes the new segment and before it deletes the last
//! old segment leaves the new segment and a suffix of the old segments. The
//! next load replays the old segments first. A dropped key that an old
//! segment still holds is live again. A second run of the reap finds that
//! key and drops it again. That second run needs `--already-applied-ok`,
//! because it matches fewer keys than the plan expects.

use super::count::{CollectionCount, GroupCount};
use super::plan::CollectionRules;
use super::run::ReapEvent;
use super::scan::{refuse_non_plain_group, scan_group, GroupScan, ScanDepth};
use super::{group_label, ReapError};
use crate::store::maintenance::{finish_legacy_rewrite, segment_bytes};
use crate::store::{LastStore, Shard};

/// The result of the apply pass for one collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionApplied {
    /// Collection name.
    pub collection: String,
    /// Groups that were rewritten.
    pub groups_rewritten: u64,
    /// Keys that were dropped.
    pub dropped_keys: u64,
    /// Sum of key length and value length of the dropped keys.
    pub dropped_bytes: u64,
    /// Segment bytes of the rewritten groups before the rewrite.
    pub bytes_before: u64,
    /// Segment bytes of the rewritten groups after the rewrite.
    pub bytes_after: u64,
}

/// Check that a group still holds what the count pass saw.
fn check_against_count(
    scan: &GroupScan,
    planned: &GroupCount,
    label: &str,
) -> Result<(), ReapError> {
    if scan.keys != planned.keys
        || scan.matched_keys != planned.matched_keys
        || scan.matched_bytes != planned.matched_bytes
    {
        return Err(ReapError::GateMismatch(format!(
            "{label} changed after the count pass: now {} keys and {} matched, counted {} keys and {} matched",
            scan.keys, scan.matched_keys, planned.keys, planned.matched_keys
        )));
    }
    Ok(())
}

/// Check the group that was read again from disk after the rewrite.
///
/// No key may match. The kept keys must be the keys before the rewrite
/// minus the matched keys, with the same bytes.
pub(super) fn check_after_rewrite(
    before: &GroupScan,
    after: &GroupScan,
    label: &str,
) -> Result<(), ReapError> {
    let want_kept = before.keys - before.matched_keys;
    if after.matched_keys != 0 {
        return Err(ReapError::GateMismatch(format!(
            "{label}: {} matched keys remain after the rewrite",
            after.matched_keys
        )));
    }
    if after.keys != want_kept {
        return Err(ReapError::GateMismatch(format!(
            "{label}: {} keys remain after the rewrite, expected {want_kept}",
            after.keys
        )));
    }
    if after.kept_digest != before.kept_digest {
        return Err(ReapError::GateMismatch(format!(
            "{label}: the kept keys or values changed in the rewrite"
        )));
    }
    Ok(())
}

/// Drop the matched keys from the loaded group. The new segment leaves them
/// out.
fn drop_matched(sh: &mut Shard, matched: &[String]) -> Result<(), ReapError> {
    for id in matched {
        sh.remove_index(id)?;
    }
    Ok(())
}

impl LastStore {
    /// Rewrite the groups of one collection that the count pass marked.
    ///
    /// One group is loaded at a time and dropped before the next. After each
    /// rewrite the group is loaded again from disk and checked.
    pub(super) fn reap_apply_collection(
        &self,
        rules: &CollectionRules,
        count: &CollectionCount,
        on_event: &mut dyn FnMut(ReapEvent<'_>),
    ) -> Result<CollectionApplied, ReapError> {
        let mut applied = CollectionApplied {
            collection: rules.collection.clone(),
            groups_rewritten: 0,
            dropped_keys: 0,
            dropped_bytes: 0,
            bytes_before: 0,
            bytes_after: 0,
        };
        for planned in &count.matched_groups {
            let (before, after) = self.reap_rewrite_group(rules, planned)?;
            applied.groups_rewritten += 1;
            applied.dropped_keys += planned.matched_keys;
            applied.dropped_bytes += planned.matched_bytes;
            applied.bytes_before += before;
            applied.bytes_after += after;
            on_event(ReapEvent::GroupRewritten {
                collection: &rules.collection,
                shard: planned.shard,
                group: planned.group,
                dropped_keys: planned.matched_keys,
                bytes_before: before,
                bytes_after: after,
            });
        }
        if applied.dropped_keys != count.matched_keys
            || applied.dropped_bytes != count.matched_bytes
        {
            return Err(ReapError::GateMismatch(format!(
                "{}: dropped {} keys, counted {}",
                rules.collection, applied.dropped_keys, count.matched_keys
            )));
        }
        Ok(applied)
    }

    /// Rewrite one group. Returns the segment bytes before and after.
    fn reap_rewrite_group(
        &self,
        rules: &CollectionRules,
        planned: &GroupCount,
    ) -> Result<(u64, u64), ReapError> {
        let collection = rules.collection.as_str();
        let label = group_label(collection, planned.shard, planned.group);
        let key = (collection.to_string(), planned.shard, planned.group);
        let handle = self.reap_open_for_apply(key.clone())?;
        let mut sh = handle.lock().expect("poison");
        refuse_non_plain_group(&sh, collection, planned.shard, planned.group)?;
        let before = scan_group(&sh, rules.matcher(), ScanDepth::Digest)?;
        check_against_count(&before, planned, &label)?;
        let bytes_before = segment_bytes(&sh.dir);
        drop_matched(&mut sh, &before.matched)?;
        LastStore::compact_shard(&mut sh)?;
        finish_legacy_rewrite(&sh)?;
        let bytes_after = segment_bytes(&sh.dir);
        drop(sh);
        drop(handle);
        // Read the group again from disk. The first handle is gone, so this
        // load replays the segment files and sees what a restart sees. The
        // tail check runs first: a torn new segment is reported and not cut.
        let handle = self.reap_open_for_apply(key)?;
        let sh = handle.lock().expect("poison");
        refuse_non_plain_group(&sh, collection, planned.shard, planned.group)?;
        let after = scan_group(&sh, rules.matcher(), ScanDepth::Digest)?;
        check_after_rewrite(&before, &after, &label)?;
        Ok((bytes_before, bytes_after))
    }
}
