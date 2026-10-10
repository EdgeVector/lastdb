//! The count pass. It runs the matcher on every live key and writes nothing.

use super::plan::CollectionRules;
use super::scan::{refuse_non_plain_group, scan_group, ScanDepth};
use super::ReapError;
use crate::store::LastStore;

/// The matches in one group that holds at least one match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCount {
    /// Shard number.
    pub shard: u16,
    /// Hash group, or `None` on a segment-log store.
    pub group: Option<u32>,
    /// Live keys in the group.
    pub keys: u64,
    /// Keys that a rule matches.
    pub matched_keys: u64,
    /// Sum of key length and value length of the matched keys.
    pub matched_bytes: u64,
}

/// The result of the count pass for one collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionCount {
    /// Collection name.
    pub collection: String,
    /// Groups on disk that the pass loaded.
    pub groups: u64,
    /// Live keys the pass visited.
    pub keys_scanned: u64,
    /// Live keys that a rule matches.
    pub matched_keys: u64,
    /// Sum of key length and value length of the matched keys.
    pub matched_bytes: u64,
    /// `expect_keys` of the rules, if given.
    pub expect_keys: Option<u64>,
    /// `expect_bytes` of the rules, if given.
    pub expect_bytes: Option<u64>,
    /// True when the counts pass the gate.
    pub ok: bool,
    /// The groups that hold a match.
    pub matched_groups: Vec<GroupCount>,
}

impl CollectionCount {
    /// Why the counts fail the gate, or `None` when they pass.
    ///
    /// The gate needs `expect_keys`. The matched keys must equal it. With
    /// `already_applied_ok`, fewer matched keys also pass: an earlier run
    /// already dropped the rest. More matched keys never pass.
    pub fn gate_problem(&self, already_applied_ok: bool) -> Option<String> {
        let Some(expect_keys) = self.expect_keys else {
            return Some(format!("{}: expect_keys is missing", self.collection));
        };
        let matched = self.matched_keys;
        let partial = already_applied_ok && matched < expect_keys;
        if matched != expect_keys && !partial {
            let hint = if matched < expect_keys {
                " (pass --already-applied-ok if an earlier run dropped the rest)"
            } else {
                ""
            };
            return Some(format!(
                "{}: matched {matched} keys, expected {expect_keys}{hint}",
                self.collection
            ));
        }
        let expect_bytes = self.expect_bytes?;
        let bytes = self.matched_bytes;
        if bytes == expect_bytes || (partial && bytes < expect_bytes) {
            return None;
        }
        Some(format!(
            "{}: matched {bytes} bytes, expected {expect_bytes}",
            self.collection
        ))
    }
}

impl LastStore {
    /// Run the matcher over every live key of every group of the collection.
    ///
    /// One group is loaded at a time and dropped before the next. A group
    /// with a sorted index or a data key is refused. A group whose newest
    /// segment ends in a torn record is refused before the load, because the
    /// load would cut the file. Nothing is written.
    pub(super) fn reap_count_collection(
        &self,
        rules: &CollectionRules,
    ) -> Result<CollectionCount, ReapError> {
        let collection = rules.collection.as_str();
        let mut count = CollectionCount {
            collection: rules.collection.clone(),
            groups: 0,
            keys_scanned: 0,
            matched_keys: 0,
            matched_bytes: 0,
            expect_keys: rules.expect_keys,
            expect_bytes: rules.expect_bytes,
            ok: false,
            matched_groups: Vec::new(),
        };
        for (shard, group) in self.handles_on_disk(collection)? {
            let handle = self.reap_open_for_count((collection.to_string(), shard, group))?;
            let sh = handle.lock().expect("poison");
            refuse_non_plain_group(&sh, collection, shard, group)?;
            let scan = scan_group(&sh, rules.matcher(), ScanDepth::Count)?;
            drop(sh);
            drop(handle);
            count.groups += 1;
            count.keys_scanned += scan.keys;
            count.matched_keys += scan.matched_keys;
            count.matched_bytes += scan.matched_bytes;
            if scan.matched_keys > 0 {
                count.matched_groups.push(GroupCount {
                    shard,
                    group,
                    keys: scan.keys,
                    matched_keys: scan.matched_keys,
                    matched_bytes: scan.matched_bytes,
                });
            }
        }
        Ok(count)
    }
}
