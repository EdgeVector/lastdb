//! Destination routing and entry partition.

use super::super::*;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::LogEntry;
use crate::sync::org_sync::{SyncPartitioner, SyncTarget};

impl SyncEngine {
    /// Resolve a `SyncDestination` to a target index in `targets`.
    ///
    /// Returns 0 (personal) only for `Personal`. A scoped destination must have
    /// a matching configured target; falling back to personal would seal share
    /// data under the wrong key and path.
    pub(crate) fn destination_to_target_idx(
        dest: &SyncDestination,
        targets: &[SyncTarget],
    ) -> SyncResult<usize> {
        let target_prefix = match dest {
            SyncDestination::Personal => return Ok(0),
            SyncDestination::Share { share_prefix, .. } => share_prefix,
        };
        for (i, t) in targets.iter().enumerate() {
            if !t.prefix.is_empty() && t.prefix == *target_prefix {
                return Ok(i);
            }
        }
        Err(SyncError::MissingSyncTarget {
            share_prefix: target_prefix.clone(),
        })
    }

    /// Partition a single pending entry into one or more (target_idx, sub_entry)
    /// pairs.
    ///
    /// For `Put` / `Delete`, this returns exactly one pair, with the original
    /// entry unchanged. For `BatchPut` / `BatchDelete`, items are grouped by
    /// target index — homogeneous batches still produce a single pair with the
    /// original batch intact (no allocation of new items). Mixed-prefix batches
    /// are split into one sub-batch per target, each sealed under the correct
    /// crypto provider. All sub-entries share the original `seq`, `timestamp_ms`,
    /// and `device_id`; seq ordering / dedupe tracking is preserved because the
    /// original log position is unchanged.
    pub(crate) fn partition_entry(
        partitioner: &Option<SyncPartitioner>,
        entry: &LogEntry,
        targets: &[SyncTarget],
    ) -> SyncResult<Vec<(usize, LogEntry)>> {
        let Some(p) = partitioner else {
            return Ok(vec![(0, entry.clone())]);
        };

        match &entry.op {
            LogOp::Put {
                namespace,
                key,
                value,
            } => {
                let dest = p.partition_catalog_or_key(namespace, key, Some(value));
                let idx = Self::destination_to_target_idx(&dest, targets)?;
                Ok(vec![(idx, entry.clone())])
            }
            LogOp::Delete { namespace, key } => {
                let dest = p.partition_catalog_or_key(namespace, key, None);
                let idx = Self::destination_to_target_idx(&dest, targets)?;
                Ok(vec![(idx, entry.clone())])
            }
            LogOp::BatchPut { namespace, items } => {
                let mut by_target: std::collections::BTreeMap<usize, Vec<(String, String)>> =
                    std::collections::BTreeMap::new();
                for (k, v) in items {
                    let dest = p.partition_catalog_or_key(namespace, k, Some(v));
                    let idx = Self::destination_to_target_idx(&dest, targets)?;
                    by_target
                        .entry(idx)
                        .or_default()
                        .push((k.clone(), v.clone()));
                }
                if by_target.len() == 1 {
                    // Homogeneous: return the original batch intact, no clone of items.
                    let idx = *by_target.keys().next().expect("len == 1");
                    return Ok(vec![(idx, entry.clone())]);
                }
                Ok(by_target
                    .into_iter()
                    .map(|(idx, sub_items)| {
                        let sub_entry = LogEntry {
                            seq: entry.seq,
                            timestamp_ms: entry.timestamp_ms,
                            device_id: entry.device_id.clone(),
                            op: LogOp::BatchPut {
                                namespace: namespace.clone(),
                                items: sub_items,
                            },
                        };
                        (idx, sub_entry)
                    })
                    .collect())
            }
            LogOp::BatchDelete { namespace, keys } => {
                let mut by_target: std::collections::BTreeMap<usize, Vec<String>> =
                    std::collections::BTreeMap::new();
                for k in keys {
                    let dest = p.partition_catalog_or_key(namespace, k, None);
                    let idx = Self::destination_to_target_idx(&dest, targets)?;
                    by_target.entry(idx).or_default().push(k.clone());
                }
                if by_target.len() == 1 {
                    let idx = *by_target.keys().next().expect("len == 1");
                    return Ok(vec![(idx, entry.clone())]);
                }
                Ok(by_target
                    .into_iter()
                    .map(|(idx, sub_keys)| {
                        let sub_entry = LogEntry {
                            seq: entry.seq,
                            timestamp_ms: entry.timestamp_ms,
                            device_id: entry.device_id.clone(),
                            op: LogOp::BatchDelete {
                                namespace: namespace.clone(),
                                keys: sub_keys,
                            },
                        };
                        (idx, sub_entry)
                    })
                    .collect())
            }
            LogOp::PhysicalDigest { namespace, items } => {
                let mut by_target: std::collections::BTreeMap<usize, Vec<(String, String)>> =
                    std::collections::BTreeMap::new();
                for (k, digest) in items {
                    let dest = p.partition_log_key(k);
                    let idx = Self::destination_to_target_idx(&dest, targets)?;
                    by_target
                        .entry(idx)
                        .or_default()
                        .push((k.clone(), digest.clone()));
                }
                if by_target.len() == 1 {
                    let idx = *by_target.keys().next().expect("len == 1");
                    return Ok(vec![(idx, entry.clone())]);
                }
                Ok(by_target
                    .into_iter()
                    .map(|(idx, sub_items)| {
                        let sub_entry = LogEntry {
                            seq: entry.seq,
                            timestamp_ms: entry.timestamp_ms,
                            device_id: entry.device_id.clone(),
                            op: LogOp::PhysicalDigest {
                                namespace: namespace.clone(),
                                items: sub_items,
                            },
                        };
                        (idx, sub_entry)
                    })
                    .collect())
            }
            LogOp::Unknown { .. } => {
                // No keys to split. Keep the original record on personal.
                Ok(vec![(0, entry.clone())])
            }
            LogOp::MutationIntent { mutations } => {
                // Intent is one unsplittable commit. Named storage_prefix
                // selects that instance's cloud head. Empty prefix is the
                // unprefixed personal instance: if share attached that
                // instance to an org head, the commit must follow, or a
                // second node restores catalog membership and never the row.
                let dest = if let Some(prefix) = mutations
                    .iter()
                    .find_map(|mutation| mutation.storage_prefix.clone())
                    .filter(|prefix| !prefix.is_empty())
                {
                    p.partition_storage_prefix(&prefix).ok_or_else(|| {
                        SyncError::MissingSyncTarget {
                            share_prefix: prefix.clone(),
                        }
                    })?
                } else {
                    p.partition_mutation_intent(mutations)
                };
                let idx = Self::destination_to_target_idx(&dest, targets)?;
                Ok(vec![(idx, entry.clone())])
            }
            LogOp::LogicalCommit { changes } => {
                let mut by_target: std::collections::BTreeMap<
                    usize,
                    Vec<crate::sync::log::LogicalChange>,
                > = std::collections::BTreeMap::new();
                for change in changes {
                    let dest = p.partition_log_key(&change.key);
                    let idx = Self::destination_to_target_idx(&dest, targets)?;
                    by_target.entry(idx).or_default().push(change.clone());
                }
                if by_target.len() == 1 {
                    let idx = *by_target.keys().next().expect("len == 1");
                    return Ok(vec![(idx, entry.clone())]);
                }
                Ok(by_target
                    .into_iter()
                    .map(|(idx, changes)| {
                        (
                            idx,
                            LogEntry {
                                seq: entry.seq,
                                timestamp_ms: entry.timestamp_ms,
                                device_id: entry.device_id.clone(),
                                op: LogOp::LogicalCommit { changes },
                            },
                        )
                    })
                    .collect())
            }
        }
    }
}
