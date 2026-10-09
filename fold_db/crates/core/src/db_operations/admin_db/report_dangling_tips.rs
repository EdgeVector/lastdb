//! Dangling-tip classification and repair report, option and attempt types.

use super::*;

/// Why one tip in an atom partition-prefix rekey resolved to no atom body.
///
/// The rekey's `missing_body` counter says only "neither the tip-derived
/// prefixed key nor the flat key held a body". That is four different
/// situations with four different severities, and a bare count cannot tell
/// them apart — so it cannot answer the one question `--remove-flat` needs
/// answered: is a body still out there under an address this pass did not try?
///
/// The discriminator is the [`crate::atom::atom_locator_codec`] row. It records
/// where the *writer* actually put the body, independent of what the tip key
/// implies. After a successful write return, every prefixed body and its
/// locator came from one ordered batch. A hard purge deletes the locator with
/// the body. So the locator's presence and its disagreement with the tip-derived
/// partition separate "nothing addresses a body" from "the body is somewhere
/// this pass never looked".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnresolvedTipClass {
    /// The locator names a partition **different** from the tip-derived one, and
    /// the body **is** present there.
    ///
    /// Not benign, and the reason this audit exists: the body is live and
    /// readable by the uuid-only path (which consults the locator), but every
    /// tip-driven hot read derives the partition from the tip key and would miss
    /// it. It also means `--remove-flat` would delete a flat key while judging
    /// this slot unverifiable. Repairable without loss — the body's real address
    /// is known.
    MisDerivedPartition,
    /// A locator row exists and decodes, but no body sits at the partition it
    /// names (nor at the tip-derived one, nor flat).
    ///
    /// The body and its locator are written in one batch, so this should not
    /// occur; when it does, the locator outlived its body. Points at a delete
    /// path that reaped a body without its locator.
    OrphanLocator,
    /// A locator row exists but its value is not a partition prefix.
    ///
    /// Readers degrade an undecodable locator to the flat key, which is also
    /// absent here, so the body is unreachable by every route. Distinguished
    /// from [`Self::NoBodyAnywhere`] because the row itself is evidence a
    /// prefixed body was once written.
    UndecodableLocator,
    /// No locator, no flat body, no prefixed body: nothing in the store
    /// addresses a body for this tip.
    ///
    /// The expected shape of a legitimately hard-purged atom (purge deletes
    /// body, locator, and schema-index row together) — but also the shape of
    /// genuine loss. This bucket is where the audit stops and a purge-ledger or
    /// tombstone cross-check has to take over.
    NoBodyAnywhere,
}

/// One tip that an atom partition-prefix rekey could not resolve to a body,
/// with the addresses that were tried.
///
/// Every field here is already-plaintext catalog data: a `mk:` tip key, an atom
/// UUID, and partition prefixes (molecule UUID + storage-form hash segment).
/// Atom *content* is sealed and never appears — see
/// [`crate::atom::atom_locator_codec`], whose value carries exactly the same
/// material for the same reason.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnresolvedTip {
    /// The `mk:` tip storage key that still references the atom.
    pub tip_key: String,
    /// The atom UUID the tip's `entry.atom_uuid` names.
    pub atom_uuid: String,
    /// Partition taken off the tip's own key — where the rekey looked, and
    /// where every tip-driven hot read looks.
    pub derived_partition: String,
    /// Molecule identity decoded out of `derived_partition`, exposed directly
    /// so a caller can join a batch of these rows against a schema/field
    /// catalog without re-parsing the partition prefix itself. Empty when the
    /// partition does not decode (should not happen for a `mk:`-prefixed
    /// tip, but this is report data, not a panic site).
    #[serde(default)]
    pub molecule_uuid: String,
    /// Schema label owning `molecule_uuid`, resolved from the live schema
    /// catalog by the caller that has one (`FoldDB::repair_dangling_tips`).
    /// `None` when no loaded schema's field molecule matches — orphaned
    /// molecule, schema not yet loaded, or this row came from a caller that
    /// does not have a schema catalog to join against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Partition from the locator row, when one exists and decodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_partition: Option<String>,
    pub class: UnresolvedTipClass,
}

/// A molecule the pre-flight refused to rewrite, and how much residue that
/// left behind.
///
/// `skipped_unrepairable` counts *tips*, which tells an operator the size of the
/// residue but not what it sits on. Until this existed, the only record of which
/// molecules were refused — and why — was a `tracing::warn!` in the daemon log,
/// so a report was readable and the residue in it was still anonymous.
///
/// Counts and lengths only: the offending key embeds a raw `RangeKey`, i.e.
/// record content, so it never appears here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefusedMolecule {
    pub molecule_uuid: String,
    /// Per-key records whose storage key exceeds the limit.
    pub keys_over_limit: usize,
    /// Byte length of the longest such key.
    pub longest_key_bytes: usize,
    /// The limit `longest_key_bytes` exceeds.
    pub key_limit_bytes: usize,
    /// Dangling tips on this molecule the refusal left in place. These are the
    /// `skipped_unrepairable` tips, attributed to the molecule that caused them.
    pub tips_left_in_place: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DanglingTipRepairReport {
    pub dry_run: bool,
    pub scan_started_at: String,
    pub tip_page: u64,
    pub tips_scanned: u64,
    pub repairable_tips: u64,
    pub tips_repaired: u64,
    /// Tips that moved between the scan and the repair — deleted, or repointed
    /// at another atom. Transient: re-running the pass can clear these.
    ///
    /// Read this together with `skipped_molecule_missing` and
    /// `skipped_atom_not_in_molecule`. Those two were counted here until they
    /// were split out, which made a *permanent* residue look like a race and
    /// invited a re-run that could never converge.
    pub skipped_changed: u64,
    /// Tips whose molecule does not exist — a pointer to nothing.
    ///
    /// Permanent: a re-run finds the same absent molecule. A non-zero value is
    /// residue the pass will never clear on its own, not work in progress.
    #[serde(default)]
    pub skipped_molecule_missing: u64,
    /// Tips whose molecule exists but does not hold this atom at the tip's
    /// slot. Permanent, for the same reason as `skipped_molecule_missing`.
    #[serde(default)]
    pub skipped_atom_not_in_molecule: u64,
    pub skipped_body_restored: u64,
    pub skipped_mis_derived: u64,
    pub skipped_unparseable_key: u64,
    /// Retained for wire compatibility. Surgical slot removal does not rewrite
    /// retained sibling keys, so this counter is always zero.
    #[serde(default)]
    pub skipped_unrepairable: u64,
    /// Retained for wire compatibility; always empty for surgical repair.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refused_molecules: Vec<RefusedMolecule>,
    /// Tips the paged scan called repairable that the live-group re-probe then
    /// found a body for, so they were never touched.
    ///
    /// Non-zero means the cheap batched probe and the live index disagreed on
    /// this store — the divergence that makes the re-probe load-bearing rather
    /// than ceremonial. It is also the honest measure of how far a dry-run
    /// number taken from the cheap tier alone would have overstated the damage.
    #[serde(default)]
    pub rescued_by_live_probe: u64,
    /// Tips whose repair returned an error. The walk continues past these; the
    /// tip and its molecule are in whatever state the failing step left them.
    #[serde(default)]
    pub failed_repairs: u64,
    /// The walk stopped early because failures came in an unbroken run, which
    /// reads as a systemic fault rather than a few pathological molecules.
    #[serde(default)]
    pub aborted_on_repeated_failures: bool,
    /// Full molecule materializations attempted by an executing pass.
    ///
    /// Repair candidates are grouped by molecule before execution, so this is
    /// bounded by distinct candidate molecules rather than dangling tips.
    #[serde(default)]
    pub molecule_loads: u64,
    pub storage_keys_deleted: u64,
    pub completed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<UnresolvedTip>,
    #[serde(default)]
    pub unresolved_truncated: bool,
    /// Present only on a key-scoped pass (`--schema` / `--hash-key`). Its
    /// presence is what tells a reader that `completed` means "every tip under
    /// these molecules was walked", not "every tip in the store".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<DanglingTipRepairScopeReport>,
}

/// Restrict `repair_dangling_tips` to the `mk:` tips of named molecules, and
/// optionally to one API HashKey inside them.
///
/// Each molecule (and hash) becomes one `mk:{M}:` (or `mk:{M}:{esc(h)}\0`)
/// prefix range, so the walk is a range read under a known molecule, never a
/// pass over the whole `mk:` plane. Its cost is bounded by the rows of the
/// scoped molecules, not by the store: on the 2026-09-23 primary the
/// whole-store dry run did not return in 3600 s, while every dangling row sat
/// in one schema (BoardCards).
///
/// Unlike [`DanglingTipRepairOptions::key_window`], this IS a repair-scoping
/// knob: the report carries [`DanglingTipRepairReport::scope`], so a scoped
/// `completed: true` cannot be read as a whole-store claim.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DanglingTipRepairScope {
    /// Molecules to walk. Empty walks nothing and completes.
    pub molecule_uuids: Vec<String>,
    /// API-form (plaintext) HashKey. The walk re-derives the storage segment
    /// (blinded on a real home) for every molecule-uuid spelling it reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_key: Option<String>,
}

/// What a key-scoped repair pass actually covered.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DanglingTipRepairScopeReport {
    /// Catalog schema names the scope resolved from. Empty when the caller
    /// scoped by molecule directly.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schemas: Vec<String>,
    pub molecule_uuids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_key: Option<String>,
    /// Distinct `mk:` prefix ranges walked (one per molecule-uuid spelling and
    /// storage hash candidate).
    pub key_ranges: u64,
}

#[derive(Debug, Clone)]
pub struct DanglingTipRepairOptions {
    pub dry_run: bool,
    pub max_ops: Option<usize>,
    pub tip_page: Option<usize>,
    pub audit_unresolved: Option<usize>,
    pub storage_prefix: Option<String>,
    /// Narrow the walk to one sub-range of the `mk:` keyspace, as *bare* (not
    /// storage-scoped) `[start, end)` keys; `end: None` means "to the end of
    /// `mk:`". `None` walks the whole `mk:` range.
    ///
    /// This exists so [`AtomStore::probe_locator_only_population`] can take a
    /// **stratified** sample instead of the first N keys in order. It is not a
    /// repair-scoping knob: an `--execute` run that used it would repair only
    /// part of the store while reporting a `completed` walk of that window.
    pub key_window: Option<(String, Option<String>)>,
    /// Walk only these molecules' tips (see [`DanglingTipRepairScope`]).
    /// `None` walks the whole `mk:` range. Exclusive with `key_window`.
    pub scope: Option<DanglingTipRepairScope>,
}

/// How many repair failures in an unbroken run stop the walk.
///
/// Isolated failures are expected — a store this old holds a few pathological
/// molecules — and skipping them is the whole point of not aborting. An
/// unbroken run of them is a different signal: the store itself is failing, and
/// continuing would just log the same error a million times.
pub(super) const MAX_CONSECUTIVE_REPAIR_FAILURES: u64 = 25;

/// Which existence tier a dangling-tip classification is allowed to trust.
///
/// The two tiers can disagree, and on real data they do: a 2026-08-03 CoW pass
/// over Tom's primary saw ~942 tips move from "body present at the derived key"
/// to "present only via the locator" between a cold scan and every later scan
/// of the same range. Nothing was written in between — only the cache warmed.
///
/// **Which direction of a cheap wrong answer is safe is a property of the
/// CALLER, not of this enum.** For the dangling-tip repair a cheap answer may
/// only ever make it do *less*: a false "present" skips a repairable tip, which
/// costs nothing, while a false "absent" deletes a tip whose body is live.
///
/// The rekey's `--remove-flat` phase runs the other way round — there a false
/// "present" is what moves a flat key into `flat_keys_to_reclaim`'s doomed set,
/// so the cheap tier's *accepted* error mode is the destructive one. Reading
/// this comment's repair-shaped argument as a module-wide rule is how that
/// phase came to gate its delete on three cached probes. Two of them now go
/// through `rekey_exists_confirmed`, which narrows a cheap answer with a live
/// one; the third, phase (4)'s post-write verify, deliberately stays cheap for
/// a reason stated at its call site — read it before "fixing" it.
///
/// So the honest rule is not "no delete may rest on a `Cached` answer"; it is
/// **name the direction a cheap wrong answer moves THIS caller in, and say so
/// where the probe is taken.** Every silent assumption here has been a caller
/// inheriting a neighbour's polarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExistsAuthority {
    /// Batched, may answer a cold group from the key-index cache or sidecar.
    /// For walking millions of tips, where the cost of a live load per key is
    /// prohibitive and a conservative wrong answer is harmless.
    Cached,
    /// Always resolves the live group. Required for any answer a delete depends
    /// on.
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RepairAttempt {
    Repaired {
        deleted_keys: u64,
    },
    /// The tip moved out from under the walk between the scan and the repair —
    /// it was deleted, or it now names a different atom. Transient by
    /// construction: it takes a concurrent writer to produce, and re-running
    /// the pass can resolve it.
    Changed,
    /// The molecule the tip decodes to does not exist. The tip and its locator
    /// are a pointer to nothing.
    ///
    /// Kept distinct from [`RepairAttempt::Changed`] because it is *permanent*.
    /// Re-running the pass finds the same absent molecule and skips the same
    /// tip forever, which is the opposite of what "changed" tells an operator.
    MoleculeMissing,
    /// The molecule exists but does not hold this atom at the tip's
    /// hash/range slot. Permanent for the same reason as
    /// [`RepairAttempt::MoleculeMissing`].
    ///
    /// Worth watching specifically: a molecule is reassembled from its `mk:`
    /// records *plus* its `update_order` append log, and that log is known to
    /// come back short on at least one molecule of the primary — `moc` 74,105
    /// against 9,000 entries loaded, logged as "possible interrupted snapshot
    /// or data loss". A tip for a slot the truncated order dropped would land
    /// here. If this counter tracks that molecule, the residue and the
    /// append-log shortfall are one fault, not two.
    AtomNotInMolecule,
    BodyRestored,
    MisDerived,
}

pub(super) fn record_repair_attempt(report: &mut DanglingTipRepairReport, attempt: RepairAttempt) {
    match attempt {
        RepairAttempt::Repaired { deleted_keys } => {
            report.tips_repaired += 1;
            report.storage_keys_deleted += deleted_keys;
        }
        RepairAttempt::Changed => report.skipped_changed += 1,
        RepairAttempt::MoleculeMissing => report.skipped_molecule_missing += 1,
        RepairAttempt::AtomNotInMolecule => report.skipped_atom_not_in_molecule += 1,
        RepairAttempt::BodyRestored => report.skipped_body_restored += 1,
        RepairAttempt::MisDerived => report.skipped_mis_derived += 1,
    }
}

#[derive(Debug, Clone)]
pub(super) struct RepairKey {
    pub(super) molecule_uuid: String,
    pub(super) hash: String,
    pub(super) range: String,
}

pub(super) struct RepairCandidate {
    pub(super) slot: RekeySlot,
    pub(super) key: RepairKey,
}

pub(super) fn decode_repair_key(storage_prefix: Option<&str>, tip_key: &str) -> Option<RepairKey> {
    let base = strip_storage_prefix(storage_prefix, tip_key)?;
    let rest = base.strip_prefix("mk:")?;
    let (molecule_uuid, suffix) = rest.split_once(':')?;
    let (hash, range) = molecule_key_codec::decode_hash_range_suffix(suffix)?;
    Some(RepairKey {
        molecule_uuid: molecule_uuid.to_string(),
        hash,
        range,
    })
}
