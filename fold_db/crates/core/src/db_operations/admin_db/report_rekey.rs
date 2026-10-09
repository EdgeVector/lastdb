//! Atom partition rekey report, options, checkpoint and flat-reclaim helpers.

use super::*;

/// One tip whose atom body exists **only at the flat key** — dry-run detail for
/// [`AtomPartitionRekeyReport::would_dual_write`].
///
/// After a completed dual-write migration this population must be empty and
/// stay empty; renewed growth means some write path is still placing bodies
/// flat (a `partition: None` caller under partition-prefix encoding), and the
/// tip keys here name the molecules it writes. Same plaintext-catalog rationale
/// as [`UnresolvedTip`]: tip key, uuid, partition prefix — never atom content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WouldDualWriteTip {
    /// The `mk:` tip storage key referencing the flat-only atom.
    pub tip_key: String,
    /// The atom UUID the tip's `entry.atom_uuid` names.
    pub atom_uuid: String,
    /// The partition a prefixed copy would land under.
    pub derived_partition: String,
}

/// Durable checkpoint for the atom body partition-prefix rekey
/// (`amigr:atom_partition_prefix_v1`). Both addressings stay readable for the
/// whole job: dual-write writes the prefixed body + locator first, and flat
/// keys are only removed when [`AtomPartitionRekeyOptions::remove_flat`] is set
/// and the prefixed body has been verified.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AtomPartitionRekeyCheckpoint {
    pub version: u32,
    /// Resume cursor: last fully processed tip storage key (exclusive).
    pub after_tip_key: Option<String>,
    /// Tips *enumerated* on the last pass — **not** a progress count.
    ///
    /// Kept for compatibility with checkpoints written before the walk was
    /// paginated. The old walk re-enumerated the whole `mk:` keyspace on every
    /// pass and skipped past the cursor in memory, so this happened to land near
    /// the cursor's position and got read as "tips migrated". It never was.
    /// [`Self::tips_walked_total`] is the progress numerator.
    pub tips_scanned: u64,
    /// Tips actually walked past the cursor, accumulated across every pass.
    ///
    /// This is the honest progress numerator: it only counts tips this migration
    /// examined and advanced the cursor over.
    #[serde(default)]
    pub tips_walked_total: u64,
    pub dual_written: u64,
    pub already_prefixed: u64,
    pub missing_body: u64,
    pub flat_removed: u64,
    /// True only when an **executing** pass walked the cursor to the end of the
    /// `mk:` keyspace. A dry run can never set this — see
    /// [`AtomPartitionRekeyReport::scan_reached_end`].
    pub completed: bool,
}

/// One call's report from [`AtomStore::rekey_atoms_to_partition_prefix`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomPartitionRekeyReport {
    pub dry_run: bool,
    pub remove_flat: bool,
    /// Tips walked past the cursor by this call. Under the paginated walk this
    /// is exactly the work this call did, not the size of the keyspace.
    pub tips_scanned: u64,
    /// Distinct (uuid, partition) slots considered this call.
    pub slots_considered: u64,
    /// Flat bodies **actually** copied to a prefixed key + locator by this call.
    ///
    /// Always `0` on a dry run — a plan has written nothing. What a dry run found
    /// is reported as [`Self::would_dual_write`], because a field named
    /// `dual_written` carrying "work still to do" is what let a 8.8%-complete
    /// migration be recorded as finished on 2026-07-29.
    pub dual_written: u64,
    /// Dry-run only: flat bodies that still need a prefixed copy — **remaining
    /// work**, not work done. Always `0` on an executing pass.
    #[serde(default)]
    pub would_dual_write: u64,
    /// Prefixed body already present — skipped.
    pub already_prefixed: u64,
    /// Tip named a uuid with no flat and no prefixed body (orphan tip / race).
    pub missing_body: u64,
    /// Flat keys deleted after a verified dual-write (or would be).
    pub flat_removed: u64,
    /// `--remove-flat` slots whose flat key was **kept** because the prefixed
    /// body was gone when the delete phase re-probed it.
    ///
    /// Zero on a healthy store. Non-zero means something reaped a prefixed atom
    /// body underneath a live `mk:` tip while this pass ran — a purge racing the
    /// migration, or a GC walk deleting a body a tip still names. Worth
    /// investigating on its own: the flat copy this counter saved is, at that
    /// point, the only copy left.
    #[serde(default)]
    pub flat_retained_unverified: u64,
    /// The migration is finished: an executing pass walked the cursor to the end
    /// of the `mk:` keyspace.
    ///
    /// Always `false` on a dry run, even when the dry run walked every tip. A
    /// dry run reports [`Self::scan_reached_end`] instead; conflating the two is
    /// the bug this split exists to make unrepresentable.
    pub completed: bool,
    /// This call's walk reached the end of the `mk:` keyspace without hitting
    /// `max_ops`. True for a dry run that enumerated everything — which says
    /// nothing about whether any body was migrated.
    #[serde(default)]
    pub scan_reached_end: bool,
    /// Tips walked past the cursor across every pass of this migration (from the
    /// durable checkpoint). The honest progress numerator.
    #[serde(default)]
    pub tips_walked_total: u64,
    /// Tips per range page this call actually used, after defaulting and the
    /// floor-at-one.
    ///
    /// Reported so a timing taken from this report is self-describing: page size
    /// is the one knob that trades the page's resident bytes against wall clock
    /// on a multi-hour run, so a recorded rate with no page size next to it
    /// cannot be compared to the next one.
    #[serde(default)]
    pub tip_page: u64,
    /// [`UnresolvedTipClass::MisDerivedPartition`] count. Non-zero means
    /// `--remove-flat` is unsafe: live bodies sit at addresses no tip-driven
    /// read derives.
    #[serde(default)]
    pub unresolved_mis_derived: u64,
    /// [`UnresolvedTipClass::OrphanLocator`] count.
    #[serde(default)]
    pub unresolved_orphan_locator: u64,
    /// [`UnresolvedTipClass::UndecodableLocator`] count.
    #[serde(default)]
    pub unresolved_undecodable_locator: u64,
    /// [`UnresolvedTipClass::NoBodyAnywhere`] count.
    #[serde(default)]
    pub unresolved_no_body: u64,
    /// Identifying tuple per unresolved tip, capped by
    /// [`AtomPartitionRekeyOptions::audit_unresolved`]. Empty when auditing is
    /// off. The four counters above classify **every** unresolved tip whether or
    /// not its detail row fit under the cap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<UnresolvedTip>,
    /// True when the cap stopped detail rows from being recorded. The counters
    /// stay complete.
    #[serde(default)]
    pub unresolved_truncated: bool,
    /// Identifying tuple per [`Self::would_dual_write`] tip, recorded on a
    /// dry-run audit and capped by
    /// [`AtomPartitionRekeyOptions::audit_unresolved`]. Empty when auditing is
    /// off or the pass executes. After a completed migration, growth in this
    /// population is an active flat-only writer — these rows name the
    /// molecules it writes, which is what identifies it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub would_dual_write_tips: Vec<WouldDualWriteTip>,
    /// True when the cap stopped would-dual-write detail rows. The counter
    /// stays complete.
    #[serde(default)]
    pub would_dual_write_truncated: bool,
    /// Checkpoint after this call (also persisted unless dry_run).
    pub checkpoint: AtomPartitionRekeyCheckpoint,
}

/// Controls one resumable pass of the atom partition-prefix rekey.
#[derive(Debug, Clone)]
pub struct AtomPartitionRekeyOptions {
    /// Plan only — no writes, no checkpoint update.
    pub dry_run: bool,
    /// Cap how many dual-writes (or already-prefixed skips that still advance
    /// the cursor) this call performs. `None` = walk everything remaining.
    pub max_ops: Option<usize>,
    /// After a verified dual-write, delete the flat `atom:{uuid}` key.
    ///
    /// Safe only when readers can resolve the prefixed body without the flat
    /// key: either the store's encoding is already
    /// [`crate::atom::AtomKeyEncoding::PartitionPrefix`] (hot path dual ladder)
    /// or the caller is measuring and accepts that Flat-encoding hot paths
    /// will miss prefixed-only bodies until the encoding flips. Default false
    /// keeps dual addressability (flat + prefixed) for the whole migration.
    pub remove_flat: bool,
    /// Classify every tip that resolves to no atom body, and record up to this
    /// many identifying detail rows. `None` = no auditing (count only).
    ///
    /// Read-only in itself: classification adds a locator point read, plus one
    /// existence check when the locator disagrees with the tip-derived
    /// partition. It never writes and never repairs — a mis-derived body is
    /// reported, not moved, because deciding where a body belongs is the repair
    /// this audit exists to inform.
    ///
    /// A cap rather than a bool because the detail rows go into the report and
    /// out through the control socket: on a store where the population is
    /// pathological, an uncapped list is what turns a diagnostic into a second
    /// outage. `Some(0)` classifies everything and returns counters only.
    pub audit_unresolved: Option<usize>,
    /// Tips fetched per range page. `None` = [`ATOM_PARTITION_REKEY_TIP_PAGE`].
    ///
    /// Bounds the transient allocation of a pass, and is the seam that lets a
    /// test drive the page boundary — an off-by-one there silently skips or
    /// re-walks tips, which on a body migration is the failure mode worth a test
    /// rather than a comment.
    pub tip_page: Option<usize>,
    /// Report the durable checkpoint and return — no scan, no writes.
    ///
    /// The only safe way to ask a live node "how far along is the migration?".
    /// Every other mode walks tips, and an executing walk rewrites the
    /// checkpoint; an operator inspecting progress must not have to mutate the
    /// thing being measured to read it.
    pub progress_only: bool,
    /// Storage scope (share namespaces). `None` = personal/main.
    pub storage_prefix: Option<String>,
}

impl Default for AtomPartitionRekeyOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            max_ops: None,
            remove_flat: false,
            audit_unresolved: None,
            tip_page: None,
            progress_only: false,
            storage_prefix: None,
        }
    }
}

/// Default tips fetched per `mk:` range page during the rekey walk.
pub const ATOM_PARTITION_REKEY_TIP_PAGE: usize = 1024;

/// Smallest page size the walk can make progress on.
///
/// A resuming page re-reads its inclusive start — the cursor row — and drops it,
/// so a one-row page carries no new work and leaves the cursor where it was. Two
/// is the floor at which a page can hold the cursor row *and* something new.
pub const ATOM_PARTITION_REKEY_MIN_TIP_PAGE: usize = 2;

/// Fold a live re-probe back into a cached existence answer.
///
/// `cached` is the batched (cache-eligible) answer for every key. `live` is the
/// authoritative answer for the subset named by `live_positions` — by
/// construction, exactly the positions `cached` called present.
///
/// Live may only ever turn a `true` into a `false`. That one-directionality is
/// the whole point: it is what makes narrowing safe to apply to a delete gate
/// without re-deriving the gate's logic. A position the cheap tier already
/// called absent is left absent — that direction is conservative for every
/// caller of this pair (it retains a key rather than reclaiming it), and
/// re-probing it live would cost a group load per key of the whole page instead
/// of per key actually about to be destroyed.
pub(super) fn narrow_present_with_live(
    cached: &[bool],
    live_positions: &[usize],
    live: &[bool],
) -> Vec<bool> {
    let mut out = cached.to_vec();
    for (n, &pos) in live_positions.iter().enumerate() {
        let confirmed = live.get(n).copied().unwrap_or(false);
        if let Some(slot) = out.get_mut(pos) {
            *slot = *slot && confirmed;
        }
    }
    out
}

/// Decide which flat atom keys a `--remove-flat` page may reclaim.
///
/// Split out as a pure function because it is the one step of the rekey that
/// destroys data, and the condition it enforces is not obvious from the call
/// site: a flat key may go only when a prefixed copy is confirmed present *at
/// delete time*, not when phase (1) said so. See the caller for why the two
/// differ (the dedup loop marks slots present on a promise that a failed body
/// read can leave unkept).
///
/// `written_flat` are the flat keys whose bodies this page wrote and phase (4)
/// verified — those are unconditionally reclaimable. The three `already_*`
/// slices are positionally aligned, one entry per already-migrated slot.
///
/// Returns `(keys to delete, count retained for want of a verified prefixed
/// body)`. Deduped, because one uuid can be both freshly written under one
/// partition and already migrated under another, and a key counted twice would
/// overstate `flat_removed`.
pub(super) fn flat_keys_to_reclaim(
    written_flat: &[String],
    already_flat: &[String],
    already_flat_present: &[bool],
    already_prefixed_present: &[bool],
) -> (std::collections::BTreeSet<String>, u64) {
    let mut doomed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut retained = 0u64;

    for key in written_flat {
        // The body came *from* this key on this page, so it is there.
        doomed.insert(key.clone());
    }

    for (pos, key) in already_flat.iter().enumerate() {
        if !already_flat_present.get(pos).copied().unwrap_or(false) {
            continue;
        }
        if !already_prefixed_present.get(pos).copied().unwrap_or(false) {
            // No verified destination — keeping the flat key keeps the body
            // reachable. Counted, never silent.
            retained += 1;
            continue;
        }
        doomed.insert(key.clone());
    }

    // A key this page wrote and verified is reclaimable even if it also appears
    // as an unverified `already` slot under a different partition: the verified
    // copy is what makes it safe, and the retained count must not claim
    // otherwise.
    retained -= retained.min(
        already_flat
            .iter()
            .enumerate()
            .filter(|(pos, key)| {
                already_flat_present.get(*pos).copied().unwrap_or(false)
                    && !already_prefixed_present.get(*pos).copied().unwrap_or(false)
                    && written_flat.contains(key)
            })
            .count() as u64,
    );

    (doomed, retained)
}

/// One walked tip of a rekey page, with the three addresses its atom uses.
///
/// Derived once during decode so the batched phases can each project the whole
/// page onto one key list without re-deriving keys per phase.
pub(super) struct RekeySlot {
    pub(super) tip_key: String,
    pub(super) uuid: String,
    /// Partition derived from the *tip key* — the address every tip-driven hot
    /// read builds, and therefore where the body has to end up.
    pub(super) partition: crate::atom::AtomPartition,
    /// `atom:{uuid}` — the unmigrated address.
    pub(super) flat_key: String,
    /// Partition-prefixed address — the migration target.
    pub(super) prefixed_key: String,
    /// The partition hint. Keyed by uuid **alone**, so several slots of one page
    /// can target the same locator with different partitions.
    pub(super) locator_key: String,
}

/// BASE key of the durable rekey checkpoint (storage-prefix applied by caller).
pub const ATOM_PARTITION_REKEY_CHECKPOINT_KEY: &str = "amigr:atom_partition_prefix_v1";
