// lint:file-size-ok verbatim move out of reports.rs; one report theme per file, split further when next touched
//! Order-log audit, repair, bloat and compaction report types and helpers.

use super::*;

/// One molecule whose order log is shorter than its live key set.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogShortRow {
    /// Molecule uuid as it appears in the `mk:{M}:…` and `moc:{M}` keys.
    pub molecule: String,
    /// The persisted `moc:{M}` count — how many `mord:` entries the reader
    /// believes are live.
    pub order_count: u64,
    /// Live `mk:{M}:…` **storage rows** walked for this molecule (raw count).
    /// May over-count when two encodings of the same logical (hash, range)
    /// coexist under one molecule (encoding duality residue).
    pub keys: u64,
    /// Distinct decoded `(hash, range)` pairs among those storage rows.
    /// Equals [`Self::keys`] when every row is a unique logical key.
    #[serde(default)]
    pub unique_keys: u64,
    /// `keys - unique_keys`: extra storage rows that decode to a (hash, range)
    /// already seen in this molecule. Non-zero ⇒ H2 (double-count), not a
    /// missing order append.
    #[serde(default)]
    pub duplicate_storage_rows: u64,
    /// `keys - order_count`: raw shortfall (storage rows vs log). Historical
    /// gauge; can be inflated by encoding duality.
    pub shortfall: u64,
    /// `max(0, unique_keys - order_count)`: shortfall against **logical** keys.
    /// Non-zero ⇒ H1 (a real missing order append). Zero with
    /// [`Self::shortfall`] > 0 ⇒ pure H2 (audit over-count only).
    #[serde(default)]
    pub logical_shortfall: u64,
    /// Schema this molecule is a field of, when it can be resolved — so an
    /// operator can tell WHOSE data is affected without hand-joining molecule
    /// uuids against `field_molecule_uuids`. Prefer the schema's descriptive
    /// name when one is registered.
    ///
    /// `None` is a real answer, not a gap to paper over: a molecule whose
    /// schema was dropped, or one written under a schema this node no longer
    /// loads, is exactly the case worth seeing as unattributed. Stamped by the
    /// [`crate::FoldDB`] caller, which owns the schema manager; the store-level
    /// audit itself never resolves schemas (it stays a pure key walk).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

/// In-progress key walk for one molecule inside [`AtomStore::audit_order_log_counts`].
pub(super) struct OpenMoleculeWalk {
    pub(super) molecule: String,
    pub(super) raw_keys: u64,
    pub(super) unique: HashSet<String>,
}

/// Result of auditing every `moc:{M}` order-log count against its molecule's
/// live `mk:` record count.
///
/// **The invariant.** `update_order` is append-only: a write pushes an entry
/// every time a key's `atom_uuid` changes and never removes one, not even when
/// the key is later deleted (see
/// `MoleculeHashRange::set_atom_uuid*` and `sample()`, which dedupes on read
/// precisely because the log over-counts). So every live `mk:` record must have
/// contributed **at least** one entry, and
///
/// ```text
/// moc:{M}  >=  count(mk:{M}:…)
/// ```
///
/// holds for any molecule that has an order log at all. The reverse —
/// `moc:` *below* the key count — cannot be produced by normal operation.
///
/// **What violations mean.** The truncation defect fixed in fold #1084 stamped
/// `moc: = tail.len()` and hard-deleted every `mord:` row above it, leaving
/// `moc:` and the surviving `mord:` rows mutually consistent. Nothing inside
/// the append-log can detect it, and every point read and `HashKey` lookup
/// keeps answering correctly because the `mk:` records are untouched — only
/// `SampleN`, the sole consumer that walks `update_order`, sees the hole. The
/// `mk:` count is the only witness left, which is what this audit compares
/// against.
///
/// [`Self::short_molecules`] being empty is the evidence that no molecule on
/// this store carries a truncated log.
///
/// **Scope.** One `storage_prefix` per call, and the caller that ships today
/// (`lastdb db order-log-audit`) passes `None` — the personal, unprefixed
/// keyspace. Share/org-prefixed molecules live under `{prefix}:mk:…` and are
/// not reached by an unprefixed scan, so a clean report is a statement about
/// the personal keyspace and not about every byte in the home. Same scope as
/// `lastdb db inventory`.
///
/// **Not a violation.** A molecule with `mk:` records and no `moc:` key at all
/// is counted in [`Self::molecules_without_order_count`], not here: Hash/Range
/// molecules (`Card`, etc.) never write an order log, and a pre-split header
/// inlined its order instead. Neither is distinguishable from the other by key
/// shape alone, and neither is damage.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogAudit {
    /// `moc:` count records read.
    pub order_counts_read: u64,
    /// `moc:` rows whose value would not decode as a number.
    ///
    /// **Must be zero for the verdict to mean anything.** A molecule whose
    /// count is unreadable is indistinguishable, further down, from one that
    /// has no order log at all — it lands in
    /// [`Self::molecules_without_order_count`] and is silently cleared. So a
    /// whole-store decode failure would render as a confident all-clear, which
    /// is the worst possible output for a detector. Counted separately so that
    /// failure is loud instead.
    pub order_counts_unreadable: u64,
    /// `mk:` records walked.
    pub keys_scanned: u64,
    /// Molecules fully walked and therefore decided by this call.
    pub molecules_decided: u64,
    /// Decided molecules holding an order log at least as long as their key
    /// count — the healthy population.
    pub molecules_ok: u64,
    /// Decided molecules with `mk:` records but no `moc:` key (Hash/Range, or a
    /// pre-split inline order). Not damage; see the type docs.
    pub molecules_without_order_count: u64,
    /// **The number this audit exists for**: decided molecules whose `moc:` is
    /// below their live **storage-row** key count. Zero means no truncated log
    /// (and no dual-encoding over-count) is on this store. See also
    /// [`Self::logical_entries_missing`] for the H1-only gauge.
    pub molecules_short: u64,
    /// Total raw missing entries (`keys - order_count`) across
    /// [`Self::molecules_short`]. Can be inflated by encoding duality.
    pub entries_missing: u64,
    /// Total **logical** missing entries (`unique_keys - order_count` when
    /// positive) across short molecules. The H1 gauge: real missing appends.
    #[serde(default)]
    pub logical_entries_missing: u64,
    /// Short molecules that also carry duplicate storage encodings of the same
    /// logical key (H2 residue contributing to their raw shortfall).
    #[serde(default)]
    pub molecules_with_duplicate_storage_rows: u64,
    /// The short molecules, worst raw shortfall first.
    pub short_molecules: Vec<OrderLogShortRow>,
    /// `moc:` records whose molecule has **no** live `mk:` record. Ordinary
    /// (every key deleted, log retained by design) — reported so the totals
    /// reconcile rather than appearing to lose rows.
    pub order_counts_without_keys: u64,
    /// Short candidates whose `moc:` was re-read after the key walk.
    ///
    /// Step 1 snapshots every count before step 2 walks a single key, so on a
    /// live node any key written in between is compared against a count taken
    /// earlier and inflates the shortfall. That bias is systematically positive
    /// and scales with write load, which is exactly the condition an operator
    /// runs this under. Only candidates pay the re-read, so the cost is one
    /// point read per finding, not per molecule.
    pub short_candidates_rechecked: u64,
    /// Candidates the re-read cleared: the log had caught up, so the gap was
    /// this audit's own read skew rather than a hole in the store.
    ///
    /// Non-zero is normal on a busy node and is not itself a fault. It is
    /// reported because a large value means the pass raced heavy writes, which
    /// is the context needed to read [`Self::molecules_short`] fairly.
    pub short_candidates_cleared_by_recheck: u64,
    /// True when this call stopped at a molecule boundary before the end of the
    /// `mk:` keyspace. Resume with `after_key = next_after_key`.
    pub more_remaining: bool,
    /// Last key of the last **fully walked** molecule — the resume cursor.
    ///
    /// A partially walked molecule is never decided: its key count would be an
    /// undercount, and an undercount compared against a full `moc:` reads as
    /// healthy. That is a false negative in an audit whose whole purpose is to
    /// find shortfalls, so the cursor is rolled back to the last molecule
    /// boundary and the incomplete molecule is walked again from its start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

/// Result of one bounded order-log shortfall repair page.
///
/// The embedded audit supplies the resume cursor and the before-state. Repair
/// never rewrites existing `mord:` entries: it appends each live logical key
/// absent from the current log and advances `moc:` in one ordered batch.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogRepairReport {
    pub dry_run: bool,
    pub audit: OrderLogAudit,
    /// Short molecules whose live keys were compared with their current log.
    pub molecules_planned: u64,
    /// Missing logical identities the pass would append.
    pub entries_planned: u64,
    /// Molecules changed by this call (zero in dry-run mode).
    pub molecules_repaired: u64,
    /// `mord:` entries appended by this call (zero in dry-run mode).
    pub entries_appended: u64,
    /// Mirrors the audit cursor for operator-friendly resumability.
    pub more_remaining: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

/// One molecule whose order log is longer than its live logical key set
/// (stale append-log residue), or holds an order log with zero live keys.
///
/// Companion to [`OrderLogShortRow`]: that detector finds *missing* log entries;
/// this one finds *excess* entries left by the append-only design when keys are
/// deleted. The reclaimable margin is [`Self::stale_entries`] (and, when the
/// molecule is zero-live, the full [`Self::order_log_bytes`]).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogBloatRow {
    /// Molecule uuid as it appears in `mk:{M}:…` / `moc:{M}` / `mord:{M}:…`.
    pub molecule: String,
    /// Schema this molecule is a field of, when resolved (stamped by FoldDB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Persisted `moc:{M}` count (dense count; sparse entries are included in
    /// the measured entry/byte totals below when present).
    pub order_count: u64,
    /// Live `mk:{M}:…` storage rows walked for this molecule.
    pub live_keys: u64,
    /// Distinct decoded `(hash, range)` pairs among those storage rows.
    pub live_unique_keys: u64,
    /// `max(0, order_log_entries - live_unique_keys)` — exact stale-entry
    /// margin against logical live slots. Sparse append rows deliberately do
    /// not advance `moc:`, so `order_count` alone is not an entry count.
    pub stale_entries: u64,
    /// Exact number of `mord:` rows (dense + sparse) under this molecule.
    pub order_log_entries: u64,
    /// Exact live key+value bytes of those `mord:` rows.
    pub order_log_bytes: u64,
    /// Exact live key+value bytes of the `moc:{M}` count record.
    pub order_count_bytes: u64,
    /// True when the molecule has an order log (or count) and zero live `mk:`.
    pub zero_live: bool,
}

/// Per-schema roll-up of one bloat-audit page (or of the CLI aggregate).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogBloatSchemaStat {
    pub schema_name: String,
    pub molecules: u64,
    pub zero_live_molecules: u64,
    pub order_log_entries: u64,
    pub order_log_bytes: u64,
    pub live_unique_keys: u64,
    pub stale_entries: u64,
    /// Exact order-log + count bytes on zero-live molecules only.
    pub zero_live_bytes: u64,
}

/// Result of one bounded order-log **bloat** audit page.
///
/// The order log is append-only: deletes retire `mk:` tips but leave every prior
/// `mord:` entry (and the `moc:` count) in place. On a long-lived primary that
/// produces a store whose order-log entry count far exceeds live atoms — the
/// 2026-08-17 inventory measured 3.7M order-log entries against 1.5M live
/// atoms, including ~945k entries for schemas with zero live atoms.
///
/// This audit is the **read-only** measurement of that excess. It does not
/// delete or rewrite anything. Bounding and resume match
/// [`AtomStore::audit_order_log_counts`]: soft `max_keys` at molecule
/// boundaries, `next_after_key` cursor, zero-live residual reported only when
/// the call covers the whole `mk:` keyspace (or the CLI aggregates a full run).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogBloatAudit {
    /// `moc:` count records read.
    pub order_counts_read: u64,
    /// `moc:` rows whose value would not decode as a number.
    pub order_counts_unreadable: u64,
    /// `mk:` records walked.
    pub keys_scanned: u64,
    /// Molecules fully walked and therefore decided by this call.
    pub molecules_decided: u64,
    /// Decided molecules with an order log no longer than their live unique keys
    /// (or with no order log at all).
    pub molecules_ok: u64,
    /// Decided molecules with `mk:` records but no `moc:` key.
    pub molecules_without_order_count: u64,
    /// Decided molecules with live keys and `order_count > live_unique_keys`.
    pub molecules_bloated: u64,
    /// Molecules with an order log / count and zero live `mk:` selected by this
    /// page of the zero-live sweep (phase B; 0 on a phase-A `mk:` walk page).
    pub molecules_zero_live: u64,
    /// Sum of exact dense+sparse `stale_entries` across findings this page.
    pub stale_entries: u64,
    /// Sum of exact `order_log_bytes` on findings this page.
    pub order_log_bytes: u64,
    /// Sum of exact `order_count_bytes` on findings this page.
    pub order_count_bytes: u64,
    /// Exact order-log + count bytes on zero-live molecules this page.
    pub zero_live_bytes: u64,
    /// Bloated molecules (live keys > 0, order longer than unique keys), worst
    /// stale-entry margin first.
    pub bloated_molecules: Vec<OrderLogBloatRow>,
    /// Zero-live residue selected by this bounded page of the sweep. Capped,
    /// so it is the size of one commit rather than of the whole store.
    pub zero_live_molecules: Vec<OrderLogBloatRow>,
    /// Per-schema roll-up for findings on this page.
    #[serde(default)]
    pub per_schema: Vec<OrderLogBloatSchemaStat>,
    /// Live molecules on this page. Explicit execute selects clean logs from this list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub live_molecule_ids: Vec<String>,
    /// True when this call stopped before the end of its phase. Resume with
    /// `after_key = next_after_key`; the cursor's namespace (`mk:` or `moc:`)
    /// selects which phase the next call runs.
    pub more_remaining: bool,
    /// The resume cursor: last key of the last fully walked molecule during the
    /// `mk:` walk, or the last `moc:` row consumed during the zero-live sweep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

/// Plan or result of reclaiming order-log residue via `compact-order-log`.
///
/// Explicit execute deletes the log. It does not write a new one.
///
/// * **Zero-live** ([`OrderLogBloatAudit::zero_live_molecules`]): delete every
///   `mord:` row and `moc:` under the molecule commit guard. A writer that
///   lands an `mk:` row before the guarded delete makes the molecule
///   ineligible.
/// * **Bloated and clean** live molecules: delete every `mord:` row and `moc:`.
///   `entries_retained` stays 0. A clean log is residue once SampleN reads
///   `mk:` tips. `retention_seconds` does not keep rows.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderLogZeroLiveCompactionReport {
    /// True unless the caller explicitly requested execution.
    pub dry_run: bool,
    /// Read-only selection evidence for this bounded page.
    pub audit: OrderLogBloatAudit,
    /// Zero-live molecules selected by the audit.
    pub molecules_planned: u64,
    /// Dense + sparse `mord:` rows selected by the audit (zero-live only).
    pub entries_planned: u64,
    /// Exact live key+value bytes of selected zero-live `mord:` + `moc:` rows.
    pub bytes_planned: u64,
    /// Molecules whose order-log rows and count were deleted (zero-live).
    pub molecules_compacted: u64,
    /// Dense + sparse `mord:` rows actually deleted (zero-live).
    pub entries_deleted: u64,
    /// Exact live key+value bytes deleted, measured under the commit guard.
    pub bytes_deleted: u64,
    /// Zero-live audit candidates refused because an `mk:` row existed at recheck.
    pub molecules_skipped_live: u64,
    /// Bloated and clean molecules selected for a full order-log delete.
    #[serde(default)]
    pub molecules_filter_planned: u64,
    /// Always 0. Execute does not keep order-log rows.
    #[serde(default)]
    pub entries_retained_planned: u64,
    /// Every `mord:` row on the bloated and clean molecules in this page.
    #[serde(default)]
    pub entries_stale_planned: u64,
    /// Molecules whose `mord:` rows and `moc:` key were deleted.
    #[serde(default)]
    pub molecules_filtered: u64,
    /// Always 0. Execute does not write a replacement log.
    #[serde(default)]
    pub entries_retained: u64,
    /// `mord:` rows deleted for bloated and clean molecules.
    #[serde(default)]
    pub entries_stale_removed: u64,
    /// Exact bytes of those `mord:` rows plus their `moc:` key.
    #[serde(default)]
    pub bytes_filter_deleted: u64,
    /// Bloated candidates skipped because live keys or the order log changed
    /// under the molecule commit guard (concurrent writer).
    #[serde(default)]
    pub molecules_skipped_concurrent: u64,
    /// Echoed from the caller. It does not keep rows.
    #[serde(default)]
    pub retention_seconds: u64,
    /// Live molecules with at least one sparse row older than the window.
    #[serde(default)]
    pub molecules_expired_planned: u64,
    /// Sparse (and proven-old dense) rows selected by the 30-day window.
    #[serde(default)]
    pub entries_expired_planned: u64,
    /// Exact live key+value bytes of those expired rows.
    #[serde(default)]
    pub bytes_expired_planned: u64,
    /// Live molecules whose expired rows were deleted.
    #[serde(default)]
    pub molecules_expired: u64,
    /// Expired rows actually deleted.
    #[serde(default)]
    pub entries_expired_deleted: u64,
    /// Exact live key+value bytes deleted by the time window.
    #[serde(default)]
    pub bytes_expired_deleted: u64,
    /// Concurrent fan-out used for the execute loops (1 in dry-run).
    #[serde(default)]
    pub fanout: u64,
    /// `mord:`/`moc:` write target is tips (~95% of those bytes).
    #[serde(default)]
    pub tips_bytes_planned: u64,
    /// Tips-plane bytes actually deleted (logical reclaim; compact returns disk).
    #[serde(default)]
    pub tips_bytes_deleted: u64,
    /// True when execute refused to run because a backup cut is held.
    #[serde(default)]
    pub skipped_backup_cut: bool,
    /// Mirrors the audit cursor for operator-friendly resumability.
    pub more_remaining: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

/// Bounded execute fan-out across distinct molecules. Distinct molecules take
/// distinct commit guards; the per-key write gate parallelizes across molecules
/// by design. Override with `LASTDB_ORDER_LOG_COMPACT_FANOUT`.
pub(super) const ORDER_LOG_COMPACT_FANOUT_DEFAULT: usize = 32;

pub(super) fn order_log_compact_fanout() -> usize {
    env_flag::var_parsed("LASTDB_ORDER_LOG_COMPACT_FANOUT")
        .filter(|n: &usize| *n > 0)
        .unwrap_or(ORDER_LOG_COMPACT_FANOUT_DEFAULT)
}

#[derive(Clone, Copy)]
pub(super) enum CompactKind {
    ZeroLive,
    LiveFilter,
}

pub(super) struct OrderLogRowSet {
    pub(super) dense: Vec<(String, u64)>,
    pub(super) sparse: Vec<(String, u64)>,
}

impl OrderLogRowSet {
    pub(super) fn bytes(&self) -> u64 {
        self.dense
            .iter()
            .chain(self.sparse.iter())
            .map(|(_, bytes)| *bytes)
            .sum()
    }

    pub(super) fn keys(&self) -> Vec<String> {
        self.dense
            .iter()
            .chain(self.sparse.iter())
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Stored keys plus the dense twin of each. The scanned bytes stay on the
    /// list, so a legacy `mord:{M}:{seq}` row is deleted as stored.
    pub(super) fn delete_keys(&self) -> Vec<String> {
        let mut keys = self.keys();
        let mut seen: BTreeSet<String> = keys.iter().cloned().collect();
        for key in keys.clone() {
            if let Some(twin) = kind_partition::form_twin(&key) {
                if seen.insert(twin.clone()) {
                    keys.push(twin);
                }
            }
        }
        keys
    }
}

pub(super) fn merge_compact_delta(
    dst: &mut OrderLogZeroLiveCompactionReport,
    src: &OrderLogZeroLiveCompactionReport,
) {
    dst.molecules_planned += src.molecules_planned;
    dst.molecules_compacted += src.molecules_compacted;
    dst.entries_deleted += src.entries_deleted;
    dst.bytes_deleted += src.bytes_deleted;
    dst.molecules_skipped_live += src.molecules_skipped_live;
    dst.molecules_filter_planned += src.molecules_filter_planned;
    dst.molecules_filtered += src.molecules_filtered;
    dst.entries_retained += src.entries_retained;
    dst.entries_stale_removed += src.entries_stale_removed;
    dst.bytes_filter_deleted += src.bytes_filter_deleted;
    dst.molecules_skipped_concurrent += src.molecules_skipped_concurrent;
    dst.molecules_expired += src.molecules_expired;
    dst.entries_expired_deleted += src.entries_expired_deleted;
    dst.bytes_expired_deleted += src.bytes_expired_deleted;
}
