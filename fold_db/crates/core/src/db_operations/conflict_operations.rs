use crate::clock::unix_nanos_wide;
use std::collections::{BTreeSet, HashSet};

use super::core::DbOperations;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::StorageError;
use crate::SyncConflict;

/// `mcc:{M}` — cached list of unresolved conflict ids for molecule `M`.
///
/// The hot query path annotates every result field by asking "does this
/// molecule have unresolved conflicts?". Answering used to prefix-scan the
/// colon form `conflict:{M}:`, which has no partition separator and so swept
/// every hash group. The live per-molecule prefix is `conflict\0{mol}:`. That
/// key contains NUL, so the walk stays inside one partition's fanout groups.
/// Measured 2026-07-29: 52 of 406 full-collection sweeps on a single-card read
/// were those conflict scans.
///
/// The cache is a JSON array of conflict ids (`"{mol}:{ts}"`). Empty array ⇒
/// skip entirely (O(1)). Non-empty ⇒ **point-get** each `conflict:{id}` instead
/// of scanning. Absent (pre-cache homes) ⇒ scan once, then best-effort stamp
/// the id list so later reads never sweep. A legacy bare-number stamp is
/// treated as "unknown" and re-derived.
fn molecule_conflict_index_key(molecule_uuid: &str) -> String {
    format!("mcc:{molecule_uuid}")
}

/// `hcu:mols` — home-level set of molecule uuids that hold unresolved conflicts.
///
/// The `mcc:{M}` cache above made each per-molecule answer a single `get_item`,
/// but the annotate path still paid one round trip *per field molecule*, and a
/// board read enumerates the whole schema. Measured 2026-07-30 on the primary:
/// one `kanban list --column todo` cost `gets +383` against a `+0` idle control,
/// with the ten slowest requests on the node all this schema at 1.30-1.36 s and
/// `loads=0` — resident data, no admission wait, all of it inside query
/// execution. Worse, `conflict:` rows are plane residue that still resolve out
/// of the legacy `sync_conflicts` collection, so each of those gets is a
/// dual-read: on the primary at 2026-07-31, 16.2M of 21.3M dual-read
/// fallthroughs — 76% of all of them — were served by that one plane.
///
/// A home holds very few unresolved conflicts (usually none) but a query
/// enumerates hundreds of molecules, so the question "does molecule M have a
/// conflict?" is far better answered from the small side. This record names the
/// conflicted molecules once, so annotation costs **one** get per query instead
/// of one per field molecule.
///
/// Writers do not delete this key and do not read-modify-write the molecule
/// list. A concurrent append would lose the other update, and a lost molecule
/// makes annotate report a real conflict as clean. Each writer appends one
/// `hcu:evt\0{seq}` instead. `seq` is `{unix_nanos:020}:{writer_id}`. The NUL
/// keeps the range inside one partition: at most `hash_group_partition_fanout`
/// groups, not every group. A key without NUL is a failed review.
///
/// `folded_seq` is the watermark the fold has already applied. Annotate reads
/// events with `seq` greater than that watermark. An absent stamp is unknown.
/// It is not re-derived from `conflict\0`, and it is not clean.
pub(crate) const HOME_CONFLICT_INDEX_KEY: &str = "hcu:mols";

/// Partition prefix for one conflict-event append. The NUL is the partition
/// boundary. Do not build an event key that omits it.
pub(crate) const HOME_CONFLICT_EVENT_PREFIX: &str = "hcu:evt\0";

/// Above this many distinct conflicted molecules the stamp stays a lower bound
/// (`complete: false`). Annotate sets `has_conflicts` true for a named molecule
/// and the string `"unknown"` for every other field. An unnamed molecule is
/// not clean. The cap bounds the record. It does not start a conflict walk.
const HOME_CONFLICT_INDEX_MAX_MOLECULES: usize = 10_000;

/// The stored form of [`HOME_CONFLICT_INDEX_KEY`].
///
/// `complete: false` means the list is a lower bound, not a proof the home is
/// clean. `folded_seq` is required. An old stamp that omits it fails to
/// deserialize and stays unknown. It is not re-derived.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct HomeConflictIndex {
    complete: bool,
    molecules: Vec<String>,
    folded_seq: String,
}

/// One append. The value is the molecule uuid, not the conflict payload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct HomeConflictEvent {
    molecule: String,
}

/// Exclusive end of the event partition. `hcu:evt\0` becomes `hcu:evt\u{1}`,
/// which is `partition_end`. A range that stops here stays inside one
/// partition's fanout. The longest common prefix of the bounds is `hcu:evt`
/// and has no NUL, so a backend that scans that prefix is not this bound.
fn partition_exclusive_end(partition: &str) -> String {
    let mut end = partition.to_string();
    if end.ends_with('\0') {
        end.pop();
        end.push('\u{1}');
    }
    end
}

fn event_scan_bounds(storage_prefix: Option<&str>, folded_seq: &str) -> (String, String) {
    let partition = build_storage_key(storage_prefix, HOME_CONFLICT_EVENT_PREFIX);
    let start = if folded_seq.is_empty() {
        partition.clone()
    } else {
        // `\0` sorts before `:`, so `{folded_seq}:{writer}` stays in range.
        format!("{partition}{folded_seq}\0")
    };
    (start, partition_exclusive_end(&partition))
}

fn seq_of_event_key(key: &str) -> Option<&str> {
    key.rsplit_once('\0')
        .map(|(_, seq)| seq)
        .filter(|seq| !seq.is_empty())
}

fn seq_in_fold_window(seq: &str, folded_seq: &str, cutoff: &str) -> bool {
    folded_seq < seq && seq <= cutoff
}

fn union_molecules(stamp: &[String], events: &[HomeConflictEvent]) -> HashSet<String> {
    let mut molecules = HashSet::with_capacity(stamp.len() + events.len());
    molecules.extend(stamp.iter().cloned());
    molecules.extend(events.iter().map(|event| event.molecule.clone()));
    molecules
}

fn capped_molecules(molecules: &HashSet<String>) -> Vec<String> {
    molecules
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(HOME_CONFLICT_INDEX_MAX_MOLECULES)
        .collect()
}

/// A previously incomplete stamp stays incomplete. A complete stamp that grows
/// past the cap becomes a lower bound. Do not promote `complete` here: only
/// the copy builder has scanned `conflict\0`.
fn merged_stamp(index: &HomeConflictIndex, events: &[HomeConflictEvent]) -> (bool, Vec<String>) {
    let molecules = union_molecules(&index.molecules, events);
    let complete = index.complete && molecules.len() <= HOME_CONFLICT_INDEX_MAX_MOLECULES;
    (complete, capped_molecules(&molecules))
}

mod home_index;
