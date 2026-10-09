use crate::clock::unix_nanos_wide;
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Escape hatch: force every reader back onto the per-molecule path.
///
/// This changes the behaviour of the hottest read in the node, so it ships with
/// a way to turn it off that does not need a rebuild — and it is what makes the
/// A/B measurable on one binary. The disabled path is exactly the pre-change
/// path, because `None` is what the index already returns when it cannot answer.
///
/// Read once: the query path consults this per request, and an env read per
/// annotate call would be its own (smaller) version of the problem being fixed.
fn home_conflict_index_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| env_flag::var_truthy("LASTDB_HOME_CONFLICT_INDEX_DISABLE"))
}

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

/// What annotate may say about one home after one stamp get and, when the
/// stamp exists, one event range. `Disabled` is the env escape hatch back to
/// the per-molecule path. `Unknown` is not clean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeConflictAnnotation {
    /// `LASTDB_HOME_CONFLICT_INDEX_DISABLE` is set. The caller walks
    /// `conflict\0{mol}:` once per field molecule.
    Disabled,
    /// No usable stamp. Do not treat a missing `has_conflicts` as clean.
    Unknown,
    /// Stamp `complete: true`. A molecule absent from `molecules` is clean.
    Known { molecules: HashSet<String> },
    /// Stamp `complete: false`. `molecules` is a lower bound. Every other
    /// field is unknown, not clean.
    Incomplete { molecules: HashSet<String> },
}

/// One append. The value is the molecule uuid, not the conflict payload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct HomeConflictEvent {
    molecule: String,
}

impl HomeConflictEvent {
    pub(crate) fn named(molecule: &str) -> Self {
        Self {
            molecule: molecule.to_string(),
        }
    }
}

impl DbOperations {
    /// Point-get `hcu:mols`. Absent, unreadable, or an old stamp that fails to
    /// deserialize is `None`. This does not derive a stamp and does not walk
    /// `conflict\0`.
    async fn load_home_conflict_index(
        &self,
        storage_prefix: Option<&str>,
    ) -> Option<HomeConflictIndex> {
        let key = build_storage_key(storage_prefix, HOME_CONFLICT_INDEX_KEY);
        self.atoms()
            .raw()
            .get_item::<HomeConflictIndex>(&key)
            .await
            .unwrap_or_default()
    }

    /// Stamp plus events newer than `folded_seq`.
    ///
    /// The only reads are the point get and, when the stamp exists, the event
    /// range. A miss returns [`HomeConflictAnnotation::Unknown`]. It does not
    /// call [`Self::get_unresolved_conflicts`] or
    /// [`Self::home_has_unresolved_conflicts`].
    pub async fn read_home_conflict_annotation(
        &self,
        storage_prefix: Option<&str>,
    ) -> HomeConflictAnnotation {
        if home_conflict_index_disabled() {
            return HomeConflictAnnotation::Disabled;
        }
        let Some(index) = self.load_home_conflict_index(storage_prefix).await else {
            return HomeConflictAnnotation::Unknown;
        };
        let events = match self
            .home_conflict_events_after(storage_prefix, &index.folded_seq)
            .await
        {
            Ok(rows) => rows.into_iter().map(|(_, event)| event).collect::<Vec<_>>(),
            // The range failed, so the stamp may be behind an append. Do not
            // call the page clean.
            Err(_) => {
                return HomeConflictAnnotation::Incomplete {
                    molecules: index.molecules.into_iter().collect(),
                };
            }
        };
        let molecules = union_molecules(&index.molecules, &events);
        if index.complete && molecules.len() <= HOME_CONFLICT_INDEX_MAX_MOLECULES {
            HomeConflictAnnotation::Known { molecules }
        } else {
            HomeConflictAnnotation::Incomplete { molecules }
        }
    }

    /// Molecules a complete stamp names, including newer events.
    ///
    /// `Some` is a complete set: a molecule absent from it has no conflict.
    /// `None` is unknown (no stamp, a disabled index, or `complete: false`).
    /// Callers must not treat `None` as clean.
    pub async fn home_conflicted_molecules(
        &self,
        storage_prefix: Option<&str>,
    ) -> Option<HashSet<String>> {
        match self.read_home_conflict_annotation(storage_prefix).await {
            HomeConflictAnnotation::Known { molecules } => Some(molecules),
            _ => None,
        }
    }

    /// Whether this home holds any unresolved conflict.
    ///
    /// `Some(false)` is the only clean answer, and only a complete stamp with
    /// no named molecule produces it. `None` is unknown, not clean.
    pub async fn home_has_unresolved_conflicts(
        &self,
        storage_prefix: Option<&str>,
    ) -> Option<bool> {
        match self.read_home_conflict_annotation(storage_prefix).await {
            HomeConflictAnnotation::Known { molecules } => Some(!molecules.is_empty()),
            HomeConflictAnnotation::Incomplete { .. } => Some(true),
            HomeConflictAnnotation::Unknown | HomeConflictAnnotation::Disabled => None,
        }
    }

    /// Append one `hcu:evt\0{seq}` for `molecule_uuid` and leave `hcu:mols`.
    ///
    /// A delete of the stamp used to force the next reader to walk
    /// `conflict\0`. That walk is no longer the annotate path. The event names
    /// the molecule so a complete stamp cannot hide it. The event does not
    /// clear a molecule: a later clear would hide a concurrent append.
    pub async fn invalidate_home_conflict_index(
        &self,
        storage_prefix: Option<&str>,
        molecule_uuid: &str,
    ) -> Result<(), SchemaError> {
        self.append_home_conflict_event(storage_prefix, molecule_uuid)
            .await
    }

    /// List all unresolved sync conflicts, optionally filtered by molecule UUID.
    /// When `storage_prefix` is `Some`, scans org-prefixed keys.
    pub async fn get_unresolved_conflicts(
        &self,
        molecule_uuid: Option<&str>,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<SyncConflict>, SchemaError> {
        // Per-molecule path: honour the id-list cache. Empty → O(1). Non-empty
        // → point-get each id (no prefix sweep). Missing/legacy → fall through
        // to one scan and self-heal.
        if let Some(mol) = molecule_uuid {
            let index_key = build_storage_key(storage_prefix, &molecule_conflict_index_key(mol));
            if let Some(ids) = self
                .atoms()
                .raw()
                .get_item::<Vec<String>>(&index_key)
                .await
                .map_err(|e| SchemaError::InvalidData(format!("read conflict index {mol}: {e}")))?
            {
                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let original_len = ids.len();
                let mut out = Vec::with_capacity(original_len);
                let mut live_ids = Vec::with_capacity(original_len);
                for id in ids {
                    let key = build_storage_key(
                        storage_prefix,
                        &crate::kind_partition::anchored("conflict", &id),
                    );
                    match self
                        .atoms()
                        .raw()
                        .get_item::<SyncConflict>(&key)
                        .await
                        .map_err(|e| SchemaError::InvalidData(format!("read conflict {id}: {e}")))?
                    {
                        Some(c) if !c.resolved => {
                            live_ids.push(id);
                            out.push(c);
                        }
                        // Resolved or missing: drop from the cache so the next
                        // call stays honest without a scan.
                        _ => {}
                    }
                }
                if live_ids.len() != original_len {
                    let _ = self.atoms().raw().put_item(&index_key, &live_ids).await;
                }
                return Ok(out);
            }
        }

        let base_prefix = match molecule_uuid {
            Some(mol) => crate::kind_partition::anchored("conflict", &format!("{mol}:")),
            None => crate::kind_partition::anchored("conflict", ""),
        };
        let prefix = build_storage_key(storage_prefix, &base_prefix);

        let items: Vec<(String, SyncConflict)> = self
            .atoms()
            .raw()
            .scan_items_with_prefix(&prefix)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to scan conflicts: {e}")))?;

        let conflicts: Vec<SyncConflict> = items
            .into_iter()
            .map(|(_, c)| c)
            .filter(|c| !c.resolved)
            .collect();

        // Self-heal: stamp the id list so the next annotate is point-gets only.
        if let Some(mol) = molecule_uuid {
            let index_key = build_storage_key(storage_prefix, &molecule_conflict_index_key(mol));
            let ids: Vec<String> = conflicts.iter().map(|c| c.id.clone()).collect();
            let _ = self.atoms().raw().put_item(&index_key, &ids).await;
        }

        Ok(conflicts)
    }

    /// Mark a conflict as resolved by its ID.
    /// When `storage_prefix` is `Some`, keys are org-prefixed.
    pub async fn resolve_conflict(
        &self,
        conflict_id: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        // The conflict_id is "{mol_uuid}:{ts}", stored at key "conflict:{id}"
        let base_key = crate::kind_partition::anchored("conflict", conflict_id);
        let key = build_storage_key(storage_prefix, &base_key);

        let mut conflict: SyncConflict = self
            .atoms()
            .raw()
            .get_item(&key)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to read conflict: {e}")))?
            .ok_or_else(|| SchemaError::NotFound(format!("Conflict not found: {conflict_id}")))?;

        conflict.resolved = true;
        self.atoms()
            .raw()
            .put_item(&key, &conflict)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("Failed to update conflict: {e}")))?;

        // Drop the cache so the next read re-derives the id list. conflict_id
        // is "{mol_uuid}:{ts_nanos}" with a single trailing timestamp segment.
        if let Some((mol, _)) = conflict_id.rsplit_once(':') {
            let index_key = build_storage_key(storage_prefix, &molecule_conflict_index_key(mol));
            let _ = self.atoms().raw().delete_item(&index_key).await;
            // Name the molecule and leave the stamp. A clear that sorts after
            // a concurrent append would hide that conflict. A failed append
            // over-reports when the stamp already names the molecule.
            let _ = self
                .invalidate_home_conflict_index(storage_prefix, mol)
                .await;
        }

        Ok(())
    }

    /// Record a newly created unresolved conflict id so the annotate path
    /// point-gets it instead of scanning.
    pub async fn note_conflict_created(
        &self,
        molecule_uuid: &str,
        conflict_id: &str,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let index_key =
            build_storage_key(storage_prefix, &molecule_conflict_index_key(molecule_uuid));
        let mut ids: Vec<String> = self
            .atoms()
            .raw()
            .get_item(&index_key)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("read conflict index {molecule_uuid}: {e}"))
            })?
            .unwrap_or_default();
        if !ids.iter().any(|id| id == conflict_id) {
            ids.push(conflict_id.to_string());
        }
        self.atoms()
            .raw()
            .put_item(&index_key, &ids)
            .await
            .map_err(|e| {
                SchemaError::InvalidData(format!("write conflict index {molecule_uuid}: {e}"))
            })?;
        // Append in the same call that records the conflict. Not best-effort:
        // a missing event lets a complete stamp hide this molecule.
        self.invalidate_home_conflict_index(storage_prefix, molecule_uuid)
            .await?;
        Ok(())
    }

    /// Fold events at or below the cutoff into `hcu:mols`, then delete only
    /// the keys that were folded.
    ///
    /// Does not call [`Self::home_has_unresolved_conflicts`] or
    /// [`Self::get_unresolved_conflicts`]. Does not list `mcc:` keys. An empty
    /// event range does not write a complete empty stamp.
    pub async fn fold_home_conflict_events(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let range_start = unix_nanos_wide();
        let cutoff_nanos = range_start.saturating_sub(1_000_000_000);
        let cutoff = format!("{cutoff_nanos:020}");
        let loaded = self.load_stamp_for_fold(storage_prefix).await?;
        let Some(existing) = loaded else {
            // Old stamp bytes that do not deserialize. Leave them. Do not
            // re-derive.
            return Ok(());
        };
        let folded_seq = existing
            .as_ref()
            .map(|index| index.folded_seq.clone())
            .unwrap_or_default();
        let rows = self
            .home_conflict_events_after(storage_prefix, &folded_seq)
            .await?;
        let mut fold_events = Vec::new();
        let mut fold_keys = Vec::new();
        for (key, event) in rows {
            let Some(seq) = seq_of_event_key(&key) else {
                continue;
            };
            if seq_in_fold_window(seq, &folded_seq, &cutoff) {
                fold_keys.push(key);
                fold_events.push(event);
            }
        }
        let Some(index) = existing else {
            if fold_events.is_empty() {
                return Ok(());
            }
            let molecules = capped_molecules(&union_molecules(&[], &fold_events));
            self.write_home_conflict_stamp(
                storage_prefix,
                HomeConflictIndex {
                    complete: false,
                    molecules,
                    folded_seq: cutoff,
                },
            )
            .await?;
            return self.delete_event_keys(&fold_keys).await;
        };
        let (complete, molecules) = merged_stamp(&index, &fold_events);
        self.write_home_conflict_stamp(
            storage_prefix,
            HomeConflictIndex {
                complete,
                molecules,
                folded_seq: cutoff,
            },
        )
        .await?;
        self.delete_event_keys(&fold_keys).await
    }

    /// The only caller of [`Self::get_unresolved_conflicts`] with `None`.
    ///
    /// Runs only when `LASTDB_BUILD_CONFLICT_STAMP_ON_COPY` is set, on the
    /// ephemeral copy. Annotate and [`Self::fold_home_conflict_events`] do not
    /// call this. The stamp stays on this home. Do not copy it onto the primary.
    pub async fn build_home_conflict_stamp_on_copy(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        if !env_flag::var_truthy("LASTDB_BUILD_CONFLICT_STAMP_ON_COPY") {
            return Ok(());
        }
        let started = unix_nanos_wide();
        let conflicts = self.get_unresolved_conflicts(None, storage_prefix).await?;
        let names: HashSet<String> = conflicts
            .into_iter()
            .map(|conflict| conflict.molecule_uuid)
            .collect();
        let complete = names.len() <= HOME_CONFLICT_INDEX_MAX_MOLECULES;
        let molecules = capped_molecules(&names);
        self.write_home_conflict_stamp(
            storage_prefix,
            HomeConflictIndex {
                complete,
                molecules,
                folded_seq: format!("{started:020}"),
            },
        )
        .await
    }

    async fn append_home_conflict_event(
        &self,
        storage_prefix: Option<&str>,
        molecule_uuid: &str,
    ) -> Result<(), SchemaError> {
        let seq = next_home_conflict_event_seq();
        let key = build_storage_key(storage_prefix, &home_conflict_event_key(&seq));
        debug_assert!(
            key.contains('\0'),
            "a conflict event key without NUL walks every hash group"
        );
        self.atoms()
            .raw()
            .put_item(&key, &HomeConflictEvent::named(molecule_uuid))
            .await
            .map_err(|e| SchemaError::InvalidData(format!("append home conflict event: {e}")))
    }

    /// `Ok(None)` means the stamp bytes exist and do not deserialize. `Ok(Some(None))`
    /// means the key is absent.
    async fn load_stamp_for_fold(
        &self,
        storage_prefix: Option<&str>,
    ) -> Result<Option<Option<HomeConflictIndex>>, SchemaError> {
        let key = build_storage_key(storage_prefix, HOME_CONFLICT_INDEX_KEY);
        match self.atoms().raw().get_item::<HomeConflictIndex>(&key).await {
            Ok(index) => Ok(Some(index)),
            Err(StorageError::SerializationError(_)) => Ok(None),
            Err(error) => Err(SchemaError::InvalidData(format!(
                "read home conflict stamp: {error}"
            ))),
        }
    }

    async fn home_conflict_events_after(
        &self,
        storage_prefix: Option<&str>,
        folded_seq: &str,
    ) -> Result<Vec<(String, HomeConflictEvent)>, SchemaError> {
        let (start, end) = event_scan_bounds(storage_prefix, folded_seq);
        debug_assert!(start.contains('\0') && !end.contains('\0'));
        let rows = self
            .atoms()
            .raw()
            .scan_items_in_range::<HomeConflictEvent>(&start, &end)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("range home conflict events: {e}")))?;
        Ok(rows
            .into_iter()
            .filter(|(key, _)| seq_of_event_key(key).is_some_and(|seq| folded_seq < seq))
            .collect())
    }

    async fn write_home_conflict_stamp(
        &self,
        storage_prefix: Option<&str>,
        index: HomeConflictIndex,
    ) -> Result<(), SchemaError> {
        let key = build_storage_key(storage_prefix, HOME_CONFLICT_INDEX_KEY);
        self.atoms()
            .raw()
            .put_item(&key, &index)
            .await
            .map_err(|e| SchemaError::InvalidData(format!("write home conflict stamp: {e}")))
    }

    async fn delete_event_keys(&self, keys: &[String]) -> Result<(), SchemaError> {
        for key in keys {
            self.atoms().raw().delete_item(key).await.map_err(|e| {
                SchemaError::InvalidData(format!("delete home conflict event: {e}"))
            })?;
        }
        Ok(())
    }
}

/// `{unix_nanos:020}:{pid}-{counter}`. The writer suffix keeps two appends in
/// the same nanosecond from sharing a key. Compare the whole string. Do not
/// compare it to a unix-seconds watermark: a seconds string sorts above every
/// zero-padded nanos seq.
pub(crate) fn next_home_conflict_event_seq() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:020}:{}-{}", unix_nanos_wide(), std::process::id(), n)
}

pub(crate) fn home_conflict_event_key(seq: &str) -> String {
    format!("{HOME_CONFLICT_EVENT_PREFIX}{seq}")
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
