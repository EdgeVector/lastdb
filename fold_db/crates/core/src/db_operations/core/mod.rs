use super::atom_store::AtomStore;
use super::attribution_ledger::AttributionLedger;
use super::change_feed::ChangeFeedStore;
use super::db_catalog_store::DbCatalogStore;
use super::lineage_index::LineageIndex;
use super::metadata_store::MetadataStore;
use super::molecule_key_store::MoleculeKeyStore;
use super::public_key_store::PublicKeyStore;
use super::schema_store::SchemaStore;
use crate::schema::types::key_value::KeyValue;
use crate::schema::SchemaError;
use crate::storage::traits::*;
use crate::storage::TypedKvStore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Ceiling on distinct dangling `(atom_uuid, key)` identities retained for the
/// health gauge.
///
/// The distinct set exists so a damaged node can report how *much* is broken,
/// not just how often it was read. That is only useful while the damage is
/// small enough to repair row by row; past this many distinct broken edges the
/// answer is "run the repair verb", not "here is the list". Capping keeps a
/// genuinely corrupt store from turning a status gauge into unbounded memory.
const UNRESOLVED_IDENTITY_CAP: usize = 1024;

/// Node-lifetime accounting for how long molecule write gates are **held**.
///
/// The request-phase `molecule_gate` measures the opposite end of the same
/// lock: how long a writer *waited* to acquire it. Wait time alone cannot
/// distinguish the two situations that produce it, and they have opposite
/// fixes:
///
/// - **many writers, one hot key** — each holder is fast, the queue is deep.
///   The fix belongs to the caller's key layout (spread the key).
/// - **one slow holder** — the queue is shallow, but whoever holds the gate is
///   stalling inside it. PR 3 moved `restore_missing_molecules` ahead of the
///   apply gate so a cold disk read cannot extend the hold. A remaining stall
///   inside the gate is a write-path bug.
///
/// Measured on the live primary 2026-08-06: `molecule_gate` was 95% of
/// `kanban-probe` mutation wall time (729 s over 541 mutations) while every
/// sample in the recent ring showed single-digit *microseconds* of wait — a
/// bursty stall that the wait gauge alone could not attribute to either cause.
///
/// Counted for every acquisition in both resident modes: the guard is moved
/// into the deferred persist task under `LASTDB_RESIDENT_MODE=write`, so the
/// hold outlives the request that opened it and a per-request phase would
/// silently record zero there. A node-lifetime counter is honest in both.
#[derive(Debug, Default)]
pub struct MoleculeGateHoldStats {
    total_us: AtomicU64,
    count: AtomicU64,
    max_us: AtomicU64,
}

impl MoleculeGateHoldStats {
    /// Record one completed hold.
    pub(crate) fn record(&self, held: std::time::Duration) {
        let us = u64::try_from(held.as_micros()).unwrap_or(u64::MAX);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Snapshot the three counters.
    ///
    /// Read with three separate atomic loads, so a hold completing mid-read can
    /// land in `count` but not `total_us`. That skews an average by one sample
    /// and cannot produce an impossible reading, which is the right trade for
    /// keeping the recording side lock-free on the write path.
    #[must_use]
    pub fn snapshot(&self) -> MoleculeGateHold {
        MoleculeGateHold {
            total_us: self.total_us.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

/// A read of [`MoleculeGateHoldStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MoleculeGateHold {
    /// Summed hold time across every completed acquisition, microseconds.
    pub total_us: u64,
    /// How many gate acquisitions have been released.
    ///
    /// The denominator for a mean hold. Gates still held right now are absent
    /// from both this and `total_us` — an in-flight stall shows up when it
    /// ends, so a node wedged under a gate reports the stall late rather than
    /// never. `max_us` is what surfaces a single pathological hold.
    pub count: u64,
    /// Longest single hold observed, microseconds.
    pub max_us: u64,
}

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
type UnresolvedAtomIdentityMap = HashMap<UnresolvedAtomIdentityKey, UnresolvedAtomIdentity>;

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

/// What one query dropped between "the index says this row exists" and "the
/// caller was handed this row".
///
/// Both members are page slots the caller paid for and did not receive, so both
/// have to be added back before `has_more` is decided — see `page_payload` /
/// `cursor_payload` in `lastdb_host`.
///
/// `tombstoned` exists because the key index and the row materializer disagree
/// about what a deleted row is. `count_rows` gates on `KeyMetadata.tombstoned`;
/// the read additionally drops a row whose *body* is a tombstone value, and that
/// second gate runs after the window is taken. Such a row is counted by
/// `total_count`, consumes a page slot, and is not `unresolved` (its atom
/// resolved fine) — so before this it was invisible to both sides. Measured on
/// the primary's `Papercut` partition 2026-08-09: `total_count` 1037, distinct
/// rows reachable 606, `unresolved_rows` 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryRowDrops {
    /// Rows whose tip pointed at an atom body that could not be resolved.
    pub unresolved: u64,
    /// Rows whose atom body is a content tombstone.
    pub tombstoned: u64,
    /// Page keys this query emitted in **verified API form** — a caller can
    /// point-read them back. NOT a drop: see the key-form note below.
    pub keys_api_form: u64,
    /// Page keys this query emitted in **storage form** — under a blinding
    /// codec that is a one-way partition token, so a keyed read at it resolves
    /// nothing. NOT a drop: see the key-form note below.
    pub keys_opaque_form: u64,
}

impl QueryRowDrops {
    /// Page slots consumed but not delivered.
    ///
    /// Deliberately only the two DROP members. The key-form counters ride the
    /// same tally (one task-local, see below) but they count rows the caller
    /// *did* receive, so adding them here would inflate `has_more` and re-serve
    /// every page — the exact non-terminating drain `cursor_payload` documents.
    #[must_use]
    pub fn total(self) -> u64 {
        self.unresolved.saturating_add(self.tombstoned)
    }

    /// How to read the `key.hash` values this query emitted.
    ///
    /// `None` when the query emitted no page keys at all (a keyed fast-path
    /// read, or an empty result) — the caller has nothing to classify.
    #[must_use]
    pub fn key_form(self) -> Option<KeyForm> {
        match (self.keys_api_form, self.keys_opaque_form) {
            (0, 0) => None,
            (_, 0) => Some(KeyForm::Api),
            (0, _) => Some(KeyForm::Opaque),
            _ => Some(KeyForm::Mixed),
        }
    }
}

/// Whether the `key.hash` values in a page can be fed back as a key.
///
/// The defect this exists to end: under
/// [`HashKeyEncoding::BlindV1`](crate::atom::HashKeyEncoding::BlindV1) a page
/// key that could not be recovered to API form is an HMAC token that is
/// *shape-indistinguishable* from a real plaintext key. A caller listing rows
/// and then point-reading each one gets zero rows back and reads that as
/// "the data is missing" rather than "you asked with the wrong key".
///
/// Measured 2026-08-06 on the live primary: `Page{0,2} fields=["oid"]` returned
/// `key.hash = "aymNiZLjUChyji2SSJd8Wg"` where `fields=["repo"]` (the hash
/// field) returned `"schema-infra"`, with an identical `key.range`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyForm {
    /// Every emitted key is addressable — point-read it directly.
    Api,
    /// Every emitted key is a storage-form token. Do NOT use it as a key;
    /// re-read the partition projecting the schema's hash field to get
    /// addressable keys.
    Opaque,
    /// Some rows recovered and some did not. Treat any key from this page as
    /// unverified until a keyed read confirms it.
    Mixed,
}

impl KeyForm {
    /// The wire token for the `/api/query` `key_form` field.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Opaque => "opaque",
            Self::Mixed => "mixed",
        }
    }
}

tokio::task_local! {
    /// Per-query tally of rows the index counted and the caller never received.
    ///
    /// The node-lifetime counter on `DbOperations` is shared, so a
    /// before/after delta around one query would pick up skips from every
    /// other query running concurrently — on a node with 28 UDS workers that
    /// reports dangling edges against healthy requests. This is task-scoped,
    /// and the read path is a plain `.await` chain with no `tokio::spawn`
    /// between the handler and atom hydration, so the tally follows the one
    /// request that installed it.
    ///
    /// **One task-local carrying both counters, deliberately.** Two nested
    /// `scope()` calls wrap the read future in one more layer of generic future
    /// type, and that layer propagates through the whole `.await` chain: it blew
    /// rustc's type-layout recursion limit in `lastdb_node`'s integration tests
    /// ("queries overflow the depth limit… query depth increased by 130 when
    /// computing layout of {async fn body of watch_target()}") while every
    /// `-p` build of the touched crates stayed green. A second counter is not
    /// worth a second scope.
    static QUERY_ROW_DROPS: std::cell::Cell<QueryRowDrops>;
}

/// Bump one field of the per-query tally, if one is installed.
fn note_row_drop(bump: impl Fn(&mut QueryRowDrops)) {
    let _ = QUERY_ROW_DROPS.try_with(|tally| {
        let mut drops = tally.get();
        bump(&mut drops);
        tally.set(drops);
    });
}

/// Note one row dropped for a content tombstone, if a tally is installed.
///
/// Free function rather than a `DbOperations` method: the drop happens in the
/// field read path, which holds the resolved values but not always the handle.
pub fn note_tombstoned_row() {
    note_row_drop(|drops| drops.tombstoned = drops.tombstoned.saturating_add(1));
}

/// Note how many page keys a query emitted in each key form, if a tally is
/// installed.
///
/// Called once per resolved page at the single boundary that holds the
/// storage → API key mapping
/// (`HashRangeQueryProcessor::rename_page_to_api_keys`), because that is the
/// only place that knows which form escaped to the caller.
pub fn note_page_key_forms(api_form: u64, opaque_form: u64) {
    note_row_drop(|drops| {
        drops.keys_api_form = drops.keys_api_form.saturating_add(api_form);
        drops.keys_opaque_form = drops.keys_opaque_form.saturating_add(opaque_form);
    });
}

/// Run `fut` with a fresh per-query row-drop tally.
///
/// Returns the future's output plus the rows *this* query dropped after the
/// index had counted them: unresolvable tip → atom edges, and content
/// tombstones the window had already spent a slot on.
pub async fn with_query_row_drop_tally<F>(fut: F) -> (F::Output, QueryRowDrops)
where
    F: std::future::Future,
{
    QUERY_ROW_DROPS
        .scope(std::cell::Cell::new(QueryRowDrops::default()), async move {
            let out = fut.await;
            (out, QUERY_ROW_DROPS.with(std::cell::Cell::get))
        })
        .await
}

/// Database operations with pluggable storage backend
///
/// Uses the storage abstraction layer (Last Store locally, with optional cloud sync).
///
/// All persistence is encapsulated in domain store structs whose
/// namespace fields are private. External callers reach them through
/// `schemas()`, `atoms()`, `public_keys()`, and `metadata()`.
#[derive(Clone)]
pub struct DbOperations {
    /// Primary namespace backend used to open side stores that are not one of
    /// the pre-wired domain stores below.
    store: Arc<dyn NamespacedStore>,
    /// Schema / schema-state / superseded-by namespaces
    schema_store: SchemaStore,
    /// Database/schema references into the node-universal molecule store.
    db_catalog: DbCatalogStore,
    /// Per-molecule content/index/OPE keys and access-domain wraps.
    molecule_keys: MoleculeKeyStore,
    /// Main namespace — atoms, molecules, mutation events, sync conflicts
    atom_store: AtomStore,
    /// System-wide public key
    public_key_store: PublicKeyStore,
    /// Metadata + idempotency + process results
    metadata_store: MetadataStore,
    /// Node-local, rebuildable proof rows for an isolated attribution migration.
    attribution_ledger: AttributionLedger,
    /// Durable app change feed. Product rows remain authoritative. Skipped by
    /// store-level capture (`crate::sync::policy::CAPTURE_SKIP_NAMESPACES`),
    /// but the chunk-backup plane does not consult that list, so this still
    /// rides cloud backup sealed (`ENC:`).
    change_feed: ChangeFeedStore,
    /// Forward + reverse lineage indexes for derived molecules. Both
    /// backing namespaces (`lineage_forward`, `lineage_reverse`) are skipped
    /// by store-level capture via
    /// `crate::sync::policy::CAPTURE_SKIP_NAMESPACES`, but still ride cloud
    /// backup (encrypted, not at-rest-exempt).
    /// Scaffolding only; no production call sites yet (PR 6 of
    /// `projects/molecule-provenance-dag`).
    lineage_index: LineageIndex,

    /// T0 resident graph — rehydrate miss / hit for schema·tip·atom·blob.
    /// Product law: `concepts-lastdb-rehydrate` (memory → disk → cloud).
    resident: Arc<crate::resident::ResidentGraph>,
    /// Cumulative count of query rows skipped because a tip referenced an atom
    /// body that could not be resolved. `/api/query` reads a before/after delta
    /// around one query and reports it as `unresolved_rows`.
    ///
    /// This counts *events*, not damaged rows: one broken edge re-read a
    /// thousand times bumps this a thousand times. Pair it with
    /// [`Self::unresolved_atom_distinct`] before drawing a conclusion about
    /// how much is actually broken.
    unresolved_atom_skips: Arc<AtomicU64>,
    /// Distinct `(atom_uuid, key)` identities behind `unresolved_atom_skips`,
    /// bounded by [`UNRESOLVED_IDENTITY_CAP`].
    ///
    /// The event counter alone cannot distinguish "four rows are broken and a
    /// hot client re-reads them" from "we are losing rows and it is
    /// accelerating" — the first is a trivial repair, the second is an
    /// incident. On the primary those looked identical until the daemon log
    /// was grouped by hand.
    ///
    /// An identity is one broken *field* tip, not one row: group by the `key`
    /// half for a row count. [`UnresolvedDamage`] reports both.
    unresolved_atom_identities: Arc<Mutex<HashSet<(String, String)>>>,
    /// Set once the identity set hits the cap, so the reported distinct count
    /// can be labelled a floor rather than a total.
    unresolved_identities_capped: Arc<AtomicBool>,
    /// Exact structured keys for owner diagnosis. Kept separate so the
    /// existing status gauge retains its historical dedup contract.
    unresolved_atom_details: Arc<Mutex<UnresolvedAtomIdentityMap>>,
    unresolved_details_capped: Arc<AtomicBool>,
    /// How long molecule write gates are held — the other end of the
    /// `molecule_gate` wait phase. See [`MoleculeGateHoldStats`].
    molecule_gate_hold: Arc<MoleculeGateHoldStats>,
}

impl DbOperations {
    /// Create from a NamespacedStore (works with any backend).
    ///
    /// Mini has no in-process native embedding index. Mutation writes still
    /// deliver Search app outbox batches (`db_operations::search_index`).
    pub async fn from_namespaced_store(
        store: Arc<dyn NamespacedStore>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_content_and_hash_key(
            store,
            None,
            crate::atom::MoleculeKeyCodec::plain(),
        )
        .await
    }

    /// Like [`Self::from_namespaced_store`], additionally sealing atom
    /// `content` when `atom_content_key` is set (plain hash-group packaging).
    /// HashKey encoding defaults to plain.
    pub async fn from_namespaced_store_with_atom_content_key(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_content_and_hash_key(
            store,
            atom_content_key,
            crate::atom::MoleculeKeyCodec::plain(),
        )
        .await
    }

    /// Like [`Self::from_namespaced_store_with_atom_content_key`], with an
    /// explicit HashKey storage encoding (design-lastdb-hashkey-blind-v1).
    pub async fn from_namespaced_store_with_atom_content_and_hash_key(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_and_molecule_keys_mode(
            store,
            atom_content_key,
            hash_key_codec,
            None,
            true,
        )
        .await
    }

    /// Open the domain stores for an isolated meter repair.
    ///
    /// This path skips normal meter hydrate. It does not start normal writers;
    /// the caller must run an explicit bounded repair before exposure.
    pub async fn from_namespaced_store_for_meter_repair(
        store: Arc<dyn NamespacedStore>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_and_molecule_keys_mode(
            store,
            None,
            crate::atom::MoleculeKeyCodec::plain(),
            None,
            false,
        )
        .await
    }

    /// Production constructor with a node key for durable molecule-key wraps.
    pub async fn from_namespaced_store_with_atom_and_molecule_keys(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
        molecule_wrap_key: Option<[u8; 32]>,
    ) -> Result<Self, crate::storage::StorageError> {
        Self::from_namespaced_store_with_atom_and_molecule_keys_mode(
            store,
            atom_content_key,
            hash_key_codec,
            molecule_wrap_key,
            true,
        )
        .await
    }

    async fn from_namespaced_store_with_atom_and_molecule_keys_mode(
        store: Arc<dyn NamespacedStore>,
        atom_content_key: Option<[u8; 32]>,
        hash_key_codec: crate::atom::MoleculeKeyCodec,
        molecule_wrap_key: Option<[u8; 32]>,
        admit_meter_trust: bool,
    ) -> Result<Self, crate::storage::StorageError> {
        // Open all required namespaces
        let main_kv = store.open_namespace("main").await?;
        let metadata_kv = store.open_namespace("metadata").await?;
        let schema_states_kv = store.open_namespace("schema_states").await?;
        let schemas_kv = store.open_namespace("schemas").await?;
        let db_catalog_kv = store.open_namespace("db_catalog").await?;
        let molecule_keys_kv = store.open_namespace("molecule_keys").await?;
        let public_keys_kv = store.open_namespace("public_keys").await?;
        let idempotency_kv = store.open_namespace("idempotency").await?;
        let superseded_by_kv = store.open_namespace("schema_superseded_by").await?;
        let schema_index_kv = store.open_namespace("schema_index").await?;
        // `lineage_forward` / `lineage_reverse` back the derived-molecule
        // lineage index. Skipped by store-level capture per
        // `crate::sync::policy::CAPTURE_SKIP_NAMESPACES`; rebuildable from
        // replay (project 2). Both namespaces are still cloud-backed-up
        // (encrypted since #1153, not at-rest-exempt) — capture-skip does not
        // mean cloud-excluded. No production writers exist until
        // `projects/view-compute-as-mutations` wires them in.
        let lineage_forward_kv = store.open_namespace("lineage_forward").await?;
        let lineage_reverse_kv = store.open_namespace("lineage_reverse").await?;
        let change_feed_kv = store.open_namespace("change_feed").await?;
        let attribution_ledger_kv = store.open_namespace("attribution_ledger").await?;
        // The keep-small snapshot gets its own plane (2026-09-21). It is one
        // whole-map value rewritten every debounce tick, so every put is dead
        // bytes the moment the next one lands. In `metadata` (no automatic
        // compaction; shares hash groups with correctness records) that grew
        // one group to 39 GB and looped the primary. `keep_small` is
        // capture-skipped, backup-excluded and residual-self-compacted, so the
        // plane stays one live value plus whatever the last compact left.
        let keep_small_kv = store
            .open_namespace(super::KEEP_SMALL_SNAPSHOT_COLLECTION)
            .await?;

        // Wrap KvStores in TypedKvStore adapters
        let main_store = Arc::new(TypedKvStore::new(main_kv));
        let metadata_typed = Arc::new(TypedKvStore::new(metadata_kv));
        let schema_states_store = Arc::new(TypedKvStore::new(schema_states_kv));
        let schemas_store = Arc::new(TypedKvStore::new(schemas_kv));
        let db_catalog_store = Arc::new(TypedKvStore::new(db_catalog_kv));
        let molecule_keys_store = Arc::new(TypedKvStore::new(molecule_keys_kv));
        let public_keys_typed = Arc::new(TypedKvStore::new(public_keys_kv));
        let idempotency_typed = Arc::new(TypedKvStore::new(idempotency_kv));
        let superseded_by_store = Arc::new(TypedKvStore::new(superseded_by_kv));
        let schema_index_store = Arc::new(TypedKvStore::new(schema_index_kv));

        // Domain stores
        let schema_store = {
            let store = SchemaStore::new(schemas_store, schema_states_store, superseded_by_store);
            match atom_content_key {
                Some(key) => store.with_catalog_unwrap_key(key),
                None => store,
            }
        };
        let db_catalog = DbCatalogStore::new(db_catalog_store);
        let molecule_keys = MoleculeKeyStore::new(molecule_keys_store, molecule_wrap_key);
        // Resolve the atom body key encoding against the home, not just the
        // environment, and refuse to serve a flat view of a migrated home.
        // Every construction path funnels through here, so this is the one
        // place a boot can pick the wrong addressing.
        let resident_metrics = store
            .logical_resident_set()
            .and_then(|set| set.lock().expect("poison").metrics().cloned());
        let resident = Arc::new(match resident_metrics {
            Some(metrics) => crate::resident::ResidentGraph::new().with_metrics(metrics),
            None => crate::resident::ResidentGraph::new(),
        });
        let atom_store = AtomStore::new_with_content_and_hash_key_codec(
            main_store,
            schema_index_store,
            atom_content_key,
            hash_key_codec,
        )
        .with_resident_graph(Arc::clone(&resident))
        .with_namespaced_store(Arc::clone(&store))
        .with_molecule_keys(molecule_keys.clone())
        .with_keep_small_persist(Arc::new(TypedKvStore::new(keep_small_kv)))
        .resolve_boot_encoding()
        .await?;
        atom_store
            .recover_catalog_atom_ref_transitions(&db_catalog)
            .await
            .map_err(|error| {
                crate::storage::StorageError::BackendError(format!(
                    "recover database-catalog atom references: {error}"
                ))
            })?;
        // Fail-closed meter TRUST, not fail-closed availability. #2186 refused
        // boot on any hydrate error; on 2026-09-25 that made every new build
        // refuse the primary home because its keep_small group (1.08 GB of
        // superseded snapshots) was over the cold-load cap. The meters are
        // gauge state, rebuildable from authoritative records: a failed
        // hydrate leaves every domain incomplete (reports stay incomplete,
        // meter-dependent deletion stays closed) and the node boots. Tom's
        // decision: decision-2026-09-25-lastdb-node-stays-upgradeable-soft-hard-cap.
        if admit_meter_trust {
            if let Err(e) = atom_store.hydrate_keep_small().await {
                atom_store
                    .note_keep_small_hydrate_failure(&e.to_string())
                    .await;
                tracing::warn!(
                    target: "fold_node::database",
                    error = %e,
                    "LASTDB_KEEP_SMALL_HYDRATE_FAILED meters marked incomplete; boot continues \
                     (reports stay incomplete and meter-dependent deletion stays closed until \
                     the liveness bootstrap re-measures)"
                );
            }
        }
        let public_key_store = PublicKeyStore::new(public_keys_typed);
        let metadata_store = MetadataStore::new(metadata_typed, idempotency_typed);
        let attribution_ledger = AttributionLedger::new(attribution_ledger_kv).await?;
        let change_feed = ChangeFeedStore::new(change_feed_kv).await?;
        let lineage_index = LineageIndex::new(lineage_forward_kv, lineage_reverse_kv);

        Ok(Self {
            store,
            schema_store,
            db_catalog,
            molecule_keys,
            atom_store,
            public_key_store,
            metadata_store,
            attribution_ledger,
            change_feed,
            lineage_index,
            resident,
            unresolved_atom_skips: Arc::new(AtomicU64::new(0)),
            unresolved_atom_identities: Arc::new(Mutex::new(HashSet::new())),
            unresolved_identities_capped: Arc::new(AtomicBool::new(false)),
            unresolved_atom_details: Arc::new(Mutex::new(HashMap::new())),
            unresolved_details_capped: Arc::new(AtomicBool::new(false)),
            molecule_gate_hold: Arc::new(MoleculeGateHoldStats::default()),
        })
    }

    // ===== Domain store accessors (public) =====

    /// Access the schema domain store.
    pub fn schemas(&self) -> &SchemaStore {
        &self.schema_store
    }

    /// Access the exact-key database catalog.
    pub fn db_catalog(&self) -> &DbCatalogStore {
        &self.db_catalog
    }

    /// Access durable per-molecule key bundles and domain wraps.
    pub fn molecule_keys(&self) -> &MoleculeKeyStore {
        &self.molecule_keys
    }

    /// Access the durable node-local app change feed.
    pub fn change_feed(&self) -> &ChangeFeedStore {
        &self.change_feed
    }

    /// Access the atom domain store.
    pub fn atoms(&self) -> &AtomStore {
        &self.atom_store
    }

    /// Access the resident graph (T0: resolve / rehydrate / apply).
    pub fn resident(&self) -> &Arc<crate::resident::ResidentGraph> {
        &self.resident
    }

    /// Override the atom body key encoding for the whole node.
    ///
    /// Production reads the encoding from the environment once, when the atom
    /// store is constructed, so a running node never straddles two encodings.
    /// This seam exists so a caller that owns the whole `DbOperations` — the
    /// migration driver, and the tests that prove the read paths under
    /// [`crate::atom::AtomKeyEncoding::PartitionPrefix`] — can name the
    /// encoding explicitly instead of mutating process-global state.
    #[must_use]
    pub fn with_atom_key_encoding(mut self, encoding: crate::atom::AtomKeyEncoding) -> Self {
        self.atom_store = self.atom_store.with_atom_key_encoding(encoding);
        self
    }

    /// Access the system public-key domain store.
    pub fn public_keys(&self) -> &PublicKeyStore {
        &self.public_key_store
    }

    /// Access the metadata / idempotency / process-results domain store.
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata_store
    }

    /// Access copy-migration attribution proof rows.
    pub fn attribution(&self) -> &AttributionLedger {
        &self.attribution_ledger
    }

    /// Access the derived-molecule lineage index (forward + reverse, local-only).
    pub fn lineage(&self) -> &LineageIndex {
        &self.lineage_index
    }

    /// Borrow the primary namespaced store handle.
    pub fn namespaced_store(&self) -> Arc<dyn NamespacedStore> {
        Arc::clone(&self.store)
    }

    /// Cold shard loads since open — one relaxed atomic load, safe to call on
    /// the per-request telemetry path. `None` on backends without hash groups.
    pub fn cold_shard_loads(&self) -> Option<u64> {
        self.store.cold_shard_loads()
    }

    /// Ids stepped over by keys-only walks since open — the scan sensor.
    ///
    /// One relaxed atomic load. A read path that claims to be scan-free can be
    /// pinned by asserting this counter does not move across the call.
    pub fn walk_ids_visited(&self) -> Option<u64> {
        self.store.walk_ids_visited()
    }

    /// Full read-cost picture (cold loads + warm residency vs budget).
    ///
    /// Takes the warm-set lock — sampler and `/api/status` only. Use
    /// [`Self::cold_shard_loads`] on hot paths.
    pub fn read_cost(&self) -> Option<crate::storage::traits::ReadCostStats> {
        self.store.read_cost()
    }

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

    /// Molecule write-gate hold accounting, for pairing with the
    /// `molecule_gate` wait phase.
    #[must_use]
    pub fn molecule_gate_hold(&self) -> MoleculeGateHold {
        self.molecule_gate_hold.snapshot()
    }

    /// The shared hold recorder, handed to each gate guard so it can book its
    /// own duration when it drops.
    #[must_use]
    pub(crate) fn molecule_gate_hold_stats(&self) -> Arc<MoleculeGateHoldStats> {
        Arc::clone(&self.molecule_gate_hold)
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

    /// Open a side namespace on the primary storage backend.
    pub async fn open_namespace(
        &self,
        name: &str,
    ) -> Result<Arc<dyn KvStore>, crate::storage::StorageError> {
        self.store.open_namespace(name).await
    }

    /// Flush all pending writes to durable storage
    pub async fn flush(&self) -> Result<(), SchemaError> {
        self.schema_store.flush().await?;
        self.db_catalog.flush().await?;
        self.molecule_keys.flush().await?;
        self.atom_store.flush().await?;
        self.public_key_store.flush().await?;
        self.metadata_store.flush().await?;
        self.attribution_ledger.flush().await?;
        self.lineage_index.flush().await?;
        // Apply `durable_through()` per token. An Ok flush does not clear
        // every dirty bit: a token above the seal stays dirty.
        if let (Some(store), Some(set)) = (
            self.store.raw_last_store(),
            self.store.logical_resident_set(),
        ) {
            crate::storage::laststore::logical_path::apply_durable_through(&store, &set);
        }
        Ok(())
    }

    /// One barrier for the groups this batch wrote.
    ///
    /// The namespace stores share one LastStore. Eight scoped flushes would
    /// still be eight barriers. `written` is deduped here; fanout siblings
    /// are not part of it.
    pub async fn flush_dirty_scope(
        &self,
        written: &[laststore::ShardKey],
    ) -> Result<(), SchemaError> {
        let mut scope = written.to_vec();
        scope.sort_unstable();
        scope.dedup();
        let last_store = self.store.raw_last_store();
        // No LastStore means no group barrier. `flush_written_keys` then
        // defaults to a no-op, and a Durable receipt would return while the
        // idempotency row is still pending. The namespace flush is that barrier.
        let Some(store) = last_store else {
            return self.flush().await;
        };
        let foreign = crate::durable_flush::foreign_group_count(&scope, written);
        store.set_flush_foreign_groups(foreign);
        crate::durable_flush::warn_if_flush_foreign(foreign);
        self.store
            .flush_written_keys(&scope)
            .await
            .map_err(|error| SchemaError::InvalidData(format!("scoped flush failed: {error}")))?;
        Ok(())
    }

    /// The LastStore group that holds `key` of namespace `name` (pure; no read or
    /// write). `None` without a LastStore or for a `b64:` key: use [`Self::flush`].
    #[must_use]
    pub fn namespace_row_group(&self, name: &str, key: &str) -> Option<laststore::ShardKey> {
        let store = self.store.raw_last_store()?;
        (!key.starts_with("b64:")).then(|| store.shard_key_of(name, key))
    }
}
