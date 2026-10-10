//! Live DB admin: inventory (byte breakdown) + mutation-history clear.
//!
//! These run against the **running** store (no exclusive offline open). They are
//! the product surface for "what's inside my DB?" and "trim long history".

use super::delete_ledger::{AtomDeleteLedgerEntry, DeleteLedgerHandle};
use super::storage_breakdown::{SchemaStorage, StorageBreakdown};
use super::AtomStore;
use crate::atom::{molecule_key_codec, MutationEvent};
use crate::clock::unix_nanos;
use crate::kind_partition;
use crate::protein::{Protein, MEMBER_BACKREF_PREFIX, PROTEIN_RECORD_PREFIX};
use crate::schema::types::field::{build_storage_key, FilterUtils};
use crate::schema::SchemaError;
use crate::storage::traits::PhysicalScanCursor;
use crate::storage::KvMutation;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Colon form, NUL-plane start, exclusive end covering `moc\0` writes and leftover `moc:`.
fn moc_plane(storage_prefix: Option<&str>) -> (String, String, String) {
    let colon = build_storage_key(storage_prefix, molecule_key_codec::MOC_PREFIX);
    let (start, end) = kind_partition::colon_plane_bounds(&colon);
    (colon, start, end)
}

fn moc_molecule_from_key(key: &str) -> Option<String> {
    kind_partition::rest_of(key, "moc")
        .filter(|rest| !rest.is_empty())
        .map(str::to_owned)
}

fn cursor_is_moc_plane(key: &str) -> bool {
    kind_partition::rest_of(key, "moc").is_some()
}

/// Rate-limited operator progress for the long-running `gc-atoms` phases.
///
/// The admin request is a single UDS response, so stdout cannot move until the
/// request returns. Structured daemon events are the live surface operators can
/// observe while the request is still running. Keep these fields deliberately
/// small and non-sensitive: phase, state, and a monotonic row count only.
struct GcAtomsProgress<'a> {
    phase: &'a str,
    dry_run: bool,
    rows_walked: u64,
    last_emitted_at: std::time::Instant,
    interval: std::time::Duration,
}

impl<'a> GcAtomsProgress<'a> {
    const INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

    fn start(phase: &'a str, dry_run: bool) -> Self {
        Self::with_interval(phase, dry_run, Self::INTERVAL)
    }

    fn with_interval(phase: &'a str, dry_run: bool, interval: std::time::Duration) -> Self {
        tracing::info!(
            target: "fold_db::gc_atoms",
            phase,
            state = "started",
            rows_walked = 0,
            dry_run,
            "gc-atoms progress"
        );
        Self {
            phase,
            dry_run,
            rows_walked: 0,
            last_emitted_at: std::time::Instant::now(),
            interval,
        }
    }

    fn walked(&mut self, rows: u64) {
        self.rows_walked = self.rows_walked.saturating_add(rows);
        if self.last_emitted_at.elapsed() < self.interval {
            return;
        }
        tracing::info!(
            target: "fold_db::gc_atoms",
            phase = self.phase,
            state = "running",
            rows_walked = self.rows_walked,
            dry_run = self.dry_run,
            "gc-atoms progress"
        );
        self.last_emitted_at = std::time::Instant::now();
    }

    fn finish(self) {
        tracing::info!(
            target: "fold_db::gc_atoms",
            phase = self.phase,
            state = "completed",
            rows_walked = self.rows_walked,
            dry_run = self.dry_run,
            "gc-atoms progress"
        );
    }
}

mod report_dangling_tips;
pub use report_dangling_tips::*;
mod report_gc;
pub use report_gc::*;
mod report_inventory;
pub use report_inventory::*;
mod report_locator_probe;
pub use report_locator_probe::*;
mod report_molecule_keys;
pub use report_molecule_keys::*;
mod report_order_log;
pub use report_order_log::*;
mod report_rekey;
pub use report_rekey::*;
mod dangling_tips;
mod gc_atoms_delete;
mod gc_atoms_probe;
mod gc_atoms_probe_support;
mod gc_orphans;
mod history_prune;
mod inventory;
mod order_log;
mod rekey;
mod superseded_retention;
mod tip_chain_prune;
mod tip_history_drain;
mod tombstones;

/// Strip an optional storage prefix from a full storage key, returning the base
/// key (`mk:…`, `atom:…`, …). `None` when the key is outside that scope.
fn strip_storage_prefix<'a>(storage_prefix: Option<&str>, full_key: &'a str) -> Option<&'a str> {
    match storage_prefix {
        None => Some(full_key),
        Some(p) => {
            let with_sep = format!("{p}:");
            full_key.strip_prefix(&with_sep)
        }
    }
}

/// Has the `mk:` head moved since the planning scan observed it?
///
/// Compares the three fields a tip append necessarily changes: the body it
/// points at, the chain link, and the write clock. `written_at` alone is not
/// enough — it is a coarse nanosecond stamp taken by the writer, and a
/// same-value rewrite can reuse an atom uuid — so all three are checked.
///
/// A row that vanished (`None`) counts as changed: something deleted or purged
/// the key underneath the run, and re-creating it from a stale snapshot would
/// resurrect a record another verb deliberately removed.
/// Index of the first dated `tv:` node older than `cutoff_ns`. Nodes with
/// `written_at == 0` are undatable and stay in the kept prefix.
fn first_expired_tv_index(
    nodes: &[(String, crate::atom::AtomEntry, u64)],
    cutoff_ns: u64,
) -> Option<usize> {
    nodes
        .iter()
        .position(|(_, entry, _)| entry.written_at > 0 && entry.written_at < cutoff_ns)
}

fn head_is_unchanged(
    current: Option<&super::atom_store::PerKeyRecord>,
    observed: &super::atom_store::PerKeyRecord,
) -> bool {
    let Some(cur) = current else {
        return false;
    };
    cur.entry.atom_uuid == observed.entry.atom_uuid
        && cur.entry.prev_tip_id == observed.entry.prev_tip_id
        && cur.entry.written_at == observed.entry.written_at
}

/// Read an `atom:` row's `created_at` without opening its content seal.
///
/// Only `content` is sealed, so the timestamp is readable as plain JSON on both
/// sealed and unsealed rows. `None` means the row did not parse or carried no
/// usable timestamp — callers treat that as "cannot be dated", never as "old".
fn atom_row_created_at(value_bytes: &[u8]) -> Option<DateTime<Utc>> {
    let val = crate::atom::atom_row_header(value_bytes).ok()?;
    let raw = val.get("created_at")?.as_str()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

fn atom_row_source_schema_name(value_bytes: &[u8]) -> Option<String> {
    crate::atom::atom_row_header(value_bytes)
        .ok()?
        .get("source_schema_name")?
        .as_str()
        .map(str::to_string)
}

/// Walk JSON for `"atom_uuid": "..."` string values (nested molecule / conflict blobs).
fn collect_atom_uuid_strings(bytes: &[u8], sink: &mut HashSet<String>) {
    let Ok(val) = serde_json::from_slice::<Value>(bytes) else {
        return;
    };
    fn walk(v: &Value, sink: &mut HashSet<String>) {
        match v {
            Value::Object(map) => {
                if let Some(Value::String(u)) = map.get("atom_uuid") {
                    if !u.is_empty() {
                        sink.insert(u.clone());
                    }
                }
                // also new_atom_uuid / old_atom_uuid shapes
                for key in ["new_atom_uuid", "old_atom_uuid", "conflict_loser_atom"] {
                    if let Some(Value::String(u)) = map.get(key) {
                        if !u.is_empty() {
                            sink.insert(u.clone());
                        }
                    }
                }
                for child in map.values() {
                    walk(child, sink);
                }
            }
            Value::Array(arr) => {
                for child in arr {
                    walk(child, sink);
                }
            }
            _ => {}
        }
    }
    walk(&val, sink);
}
