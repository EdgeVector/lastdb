use super::row_drops::note_row_drop;
use super::DbOperations;
use crate::schema::types::key_value::KeyValue;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

/// Ceiling on distinct dangling `(atom_uuid, key)` identities retained for the
/// health gauge.
///
/// The distinct set exists so a damaged node can report how *much* is broken,
/// not just how often it was read. That is only useful while the damage is
/// small enough to repair row by row; past this many distinct broken edges the
/// answer is "run the repair verb", not "here is the list". Capping keeps a
/// genuinely corrupt store from turning a status gauge into unbounded memory.
pub(super) const UNRESOLVED_IDENTITY_CAP: usize = 1024;

/// How much is actually broken behind the unresolved-atom skip events.
///
/// Two different numbers, because a record has many fields and each field
/// carries its own tip -> atom edge. One unreadable row with five dangling
/// field tips is five `edges` and one `row`. Reporting `edges` as a row count
/// overstates the damage by however many fields happen to be broken — on the
/// primary that was 7 vs 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnresolvedDamage {
    /// Distinct `(atom_uuid, key)` edges — one per broken *field* tip.
    pub edges: u64,
    /// Distinct keys among those edges: how many row reads come back short.
    ///
    /// This is an *index* key, so a record reachable under two index keys
    /// counts twice. It is a ceiling on unreadable records, and much closer to
    /// one than `edges` is.
    pub rows: u64,
    /// True once the identity set stopped growing at
    /// [`UNRESOLVED_IDENTITY_CAP`], making both figures floors, not totals.
    pub capped: bool,
}

/// Exact key identities retained beside the existing read integrity gauge.
/// The gauge keeps its old lossy-string dedup contract; this owner-only report
/// uses structured keys so `hash:range` cannot collide with a hash value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnresolvedAtomIdentityReport {
    pub identities: Vec<UnresolvedAtomIdentity>,
    pub edges: u64,
    pub rows: u64,
    pub capped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnresolvedAtomIdentity {
    pub atom_uuid: String,
    pub key: KeyValue,
    pub molecule_uuid: Option<String>,
    pub schema: Option<String>,
    pub field: Option<String>,
    /// A personal read resolves the body from the `main` namespace.
    pub storage_namespace: Option<String>,
    /// Exact storage-form tip key when the caller retained it.
    pub tip_storage_key: Option<String>,
    /// First body key attempted under the current atom key encoding.
    /// Null when a partition-prefixed body has no proven partition.
    pub atom_storage_key: Option<String>,
}

type UnresolvedAtomIdentityKey = (String, KeyValue, Option<String>);
pub(super) type UnresolvedAtomIdentityMap =
    HashMap<UnresolvedAtomIdentityKey, UnresolvedAtomIdentity>;

#[derive(Clone, Copy, Default)]
pub(crate) struct UnresolvedAtomContext<'a> {
    pub molecule_uuid: Option<&'a str>,
    pub schema: Option<&'a str>,
    pub field: Option<&'a str>,
    pub atom_partition: Option<&'a crate::atom::AtomPartition>,
    pub tip_storage_key: Option<&'a str>,
}

fn unresolved_identity_report(
    set: &UnresolvedAtomIdentityMap,
    capped: bool,
) -> UnresolvedAtomIdentityReport {
    let mut identities: Vec<_> = set.values().cloned().collect();
    identities.sort_by(|a, b| {
        a.key
            .hash
            .cmp(&b.key.hash)
            .then(a.key.range.cmp(&b.key.range))
            .then(a.atom_uuid.cmp(&b.atom_uuid))
            .then(a.molecule_uuid.cmp(&b.molecule_uuid))
    });
    let mut keys = HashSet::new();
    let mut unkeyed = 0u64;
    for identity in &identities {
        if identity.key.hash.is_none() && identity.key.range.is_none() {
            unkeyed += 1;
        } else {
            keys.insert(&identity.key);
        }
    }
    UnresolvedAtomIdentityReport {
        edges: identities.len() as u64,
        rows: keys.len() as u64 + unkeyed,
        identities,
        capped,
    }
}

/// Collapse dangling `(atom_uuid, key)` edges onto the keys they fall on.
///
/// Derived from the identity set rather than tracked in a second set: the set
/// is capped at [`UNRESOLVED_IDENTITY_CAP`], so this is a bounded walk, it runs
/// only on the `/api/status` path, and it cannot drift out of step with the
/// edge count the way a separately maintained set could.
///
/// The key can be empty — `filter_utils::fetch` records the skip before it has
/// a key to name, and logs without one. An unkeyed edge is *unattributable*,
/// not *shared*, so each counts as its own row. Collapsing them onto the empty
/// string would report five broken rows as one, which is the under-count
/// direction and worse than the over-count this function exists to remove.
fn distinct_rows(identities: &HashSet<(String, String)>) -> u64 {
    let mut keys = HashSet::new();
    let mut unattributable = 0u64;
    for (_atom_uuid, key) in identities {
        if key.is_empty() {
            unattributable += 1;
        } else {
            keys.insert(key.as_str());
        }
    }
    keys.len() as u64 + unattributable
}

impl DbOperations {
    /// Cumulative unresolved atom-row skips observed by query hydration.
    #[must_use]
    pub fn unresolved_atom_skip_count(&self) -> u64 {
        self.unresolved_atom_skips.load(Ordering::Relaxed)
    }

    /// Distinct damage behind the skip events: broken edges, and the rows those
    /// edges fall on.
    ///
    /// This is the *damage size*; [`Self::unresolved_atom_skip_count`] is the
    /// *exposure*. Operators need both: the first says how much is broken, the
    /// second says how often callers were served short reads.
    ///
    /// Both figures come off one lock acquisition, so they always describe the
    /// same snapshot — reading them separately could report more rows than
    /// edges, which is impossible and would look like a bug in the gauge.
    #[must_use]
    pub fn unresolved_atom_distinct(&self) -> UnresolvedDamage {
        let (edges, rows) = self
            .unresolved_atom_identities
            .lock()
            .map_or((0, 0), |set| (set.len() as u64, distinct_rows(&set)));
        UnresolvedDamage {
            edges,
            rows,
            capped: self.unresolved_identities_capped.load(Ordering::Relaxed),
        }
    }

    /// Return exact structured identities for the owner socket.
    /// A poisoned lock is an error; it must never look like an empty set.
    pub fn unresolved_atom_identity_report(&self) -> Result<UnresolvedAtomIdentityReport, String> {
        let set = self
            .unresolved_atom_details
            .lock()
            .map_err(|_| "unresolved atom identity lock is poisoned".to_string())?;
        Ok(unresolved_identity_report(
            &set,
            self.unresolved_details_capped.load(Ordering::Relaxed),
        ))
    }

    /// Record one query row skipped because its tip points at a missing atom.
    ///
    /// Bumps the node-lifetime event counter (a health gauge), remembers the
    /// distinct broken edge, and bumps the caller's per-query tally if one is
    /// installed by [`with_query_row_drop_tally`].
    ///
    /// This runs only when a row is already broken, so neither identity lock
    /// is on the healthy read path. Both identity sets stay bounded by the cap.
    pub(crate) fn record_unresolved_atom_skip(
        &self,
        atom_uuid: &str,
        key: &KeyValue,
        context: UnresolvedAtomContext<'_>,
    ) {
        let UnresolvedAtomContext {
            molecule_uuid,
            schema,
            field,
            atom_partition,
            tip_storage_key,
        } = context;
        self.unresolved_atom_skips.fetch_add(1, Ordering::Relaxed);
        if !self.unresolved_identities_capped.load(Ordering::Relaxed) {
            if let Ok(mut set) = self.unresolved_atom_identities.lock() {
                if set.len() < UNRESOLVED_IDENTITY_CAP {
                    set.insert((atom_uuid.to_string(), key.to_string()));
                } else {
                    self.unresolved_identities_capped
                        .store(true, Ordering::Relaxed);
                }
            }
        }
        let atom_storage_key = match (self.atoms().atom_key_encoding(), atom_partition) {
            (crate::atom::AtomKeyEncoding::PartitionPrefix, None) => None,
            (encoding, partition) => Some(crate::atom::atom_key_codec::storage_key(
                encoding, partition, atom_uuid,
            )),
        };
        if let Ok(mut details) = self.unresolved_atom_details.lock() {
            let identity_key = (
                atom_uuid.to_string(),
                key.clone(),
                molecule_uuid.map(ToString::to_string),
            );
            if let Some(known) = details.get_mut(&identity_key) {
                if known.molecule_uuid.is_none() {
                    known.molecule_uuid = molecule_uuid.map(ToString::to_string);
                }
                if known.schema.is_none() {
                    known.schema = schema.map(ToString::to_string);
                }
                if known.field.is_none() {
                    known.field = field.map(ToString::to_string);
                }
                if known.tip_storage_key.is_none() {
                    known.tip_storage_key = tip_storage_key.map(ToString::to_string);
                }
                if known.atom_storage_key.is_none() {
                    known.atom_storage_key = atom_storage_key;
                }
            } else if details.len() < UNRESOLVED_IDENTITY_CAP {
                details.insert(
                    identity_key,
                    UnresolvedAtomIdentity {
                        atom_uuid: atom_uuid.to_string(),
                        key: key.clone(),
                        molecule_uuid: molecule_uuid.map(ToString::to_string),
                        schema: schema.map(ToString::to_string),
                        field: field.map(ToString::to_string),
                        storage_namespace: Some("main".to_string()),
                        tip_storage_key: tip_storage_key.map(ToString::to_string),
                        atom_storage_key,
                    },
                );
            } else {
                self.unresolved_details_capped
                    .store(true, Ordering::Relaxed);
            }
        }
        note_row_drop(|drops| drops.unresolved = drops.unresolved.saturating_add(1));
    }
}
