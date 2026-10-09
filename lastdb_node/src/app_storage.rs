//! Storage attribution by `owner_app_id` — the read surface behind
//! `lastdb app storage --json`.
//!
//! Slice 2 of brain `north-star-lastdb-app-storage-attribution`. Slice 1
//! landed the keep-small budget/histogram primitives; this module turns the
//! per-schema meters that write path already maintains into a per-owner
//! ranking.
//!
//! **Scan-free by construction.** The input is one point read of the
//! in-process keep-small projection plus one in-memory schema-metadata
//! lookup per metered schema. There is no collection walk, no prefix scan,
//! and no `db inventory` here.
//!
//! **Nothing is silently dropped.** Every live byte lands in exactly one row:
//! an app row, the reserved `system` row, or the reserved `unattributed` row.
//! `complete` is true only when `unattributed_bytes == 0`, so partial
//! attribution can never read as exact.
//!
//! Slice 4 adds `reconciliation_lag` and a bounded, resumable job that pages
//! declared schema/key layouts (never a collection scan) to recover
//! `unresolved_schema_bytes` into owner buckets. Structural
//! `unattributable_plane_bytes` stay separate.

use chrono::{DateTime, Utc};
use fold_db::db_operations::MeterTrustState;
use fold_db::db_operations::{KeepSmallSnapshot, LIVE_BUDGET_BYTES};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Metadata key for the durable reconciliation checkpoint. Point get/put.
pub const RECONCILE_CHECKPOINT_KEY: &str = "keep_small:reconciliation";

/// Default number of declared layouts one reconcile page visits.
pub const RECONCILE_PAGE_SIZE: usize = 32;

/// Hard cap so a caller cannot turn one page into a catalog walk.
pub const RECONCILE_PAGE_SIZE_MAX: usize = 256;

/// Reserved row for schemas the node knows but no app owns (core planes).
pub const SYSTEM_OWNER: &str = "system";

/// Reserved row for live bytes the projection cannot attribute today.
pub const UNATTRIBUTED_OWNER: &str = "unattributed";

/// Whether the write-path meters cover this home's history, or only the part
/// of it this process happened to write.
///
/// The report is built entirely from the keep-small snapshot. That makes it
/// scan-free, and it also means the report cannot tell a home with no data
/// from a home whose meters were never hydrated: both present as zeros. This
/// enum carries the one bit that separates them, supplied by the caller, which
/// is the only layer that can see both the meters and the schema catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MetersOrigin {
    /// A durable snapshot was hydrated at boot, or the home is genuinely new.
    /// The totals are a measurement.
    #[default]
    Measured,
    /// Boot found no `keep_small:meters` row on a home that already holds
    /// schemas. The totals are a floor covering only this process's own
    /// writes, and organic writes never close the gap — they meter what they
    /// write, never the pre-existing corpus.
    NotHydrated,
    /// Boot hydrated a snapshot the store had already written past: the
    /// process stopped after the last debounce tick without the shutdown
    /// flush. The totals are the last persisted gauge, not the home as it is.
    /// Repaired by the liveness bootstrap, like a miss.
    StaleAfterUncleanStop,
    /// The persisted trust payload says that the counters lack an independent
    /// audit. The byte values remain available, but they are not exact.
    Incomplete,
}

impl MetersOrigin {
    /// Decide from the two facts the caller can see cheaply, both already in
    /// memory: whether boot missed the snapshot, and whether the catalog holds
    /// any schema at all.
    ///
    /// A genuinely fresh home misses the snapshot too, but its catalog is
    /// empty, so it stays [`MetersOrigin::Measured`] and still reports
    /// `complete`.
    #[must_use]
    pub fn from_boot(hydrate_missed: bool, catalog_non_empty: bool) -> Self {
        if hydrate_missed && catalog_non_empty {
            Self::NotHydrated
        } else {
            Self::Measured
        }
    }

    /// [`Self::from_boot`] plus the unclean-stop verdict from the meters. A
    /// stale snapshot only exists on a home that had schemas, so it needs no
    /// catalog check.
    #[must_use]
    pub fn from_meters(
        hydrate_missed: bool,
        stale_after_unclean_stop: bool,
        catalog_non_empty: bool,
    ) -> Self {
        if stale_after_unclean_stop {
            Self::StaleAfterUncleanStop
        } else {
            Self::from_boot(hydrate_missed, catalog_non_empty)
        }
    }

    /// True when the totals are a measurement rather than a floor.
    #[must_use]
    pub fn is_measured(self) -> bool {
        matches!(self, Self::Measured)
    }

    /// Add the persisted provenance state to the legacy boot facts.
    #[must_use]
    pub fn from_trust(
        trust: MeterTrustState,
        hydrate_missed: bool,
        stale_after_unclean_stop: bool,
        catalog_non_empty: bool,
    ) -> Self {
        if trust == MeterTrustState::Absent && !catalog_non_empty {
            Self::from_meters(hydrate_missed, stale_after_unclean_stop, catalog_non_empty)
        } else if !trust.is_complete() {
            Self::Incomplete
        } else {
            Self::from_meters(hydrate_missed, stale_after_unclean_stop, catalog_non_empty)
        }
    }
}

/// What the node could learn about one metered schema's owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerResolution {
    /// Schema carries a non-empty `owner_app_id`.
    App(String),
    /// Schema is registered locally and carries no `owner_app_id`.
    System,
    /// Schema is not resolvable on this node — attribution is unknown, and
    /// its bytes must show up as `unattributed` rather than vanish.
    Unknown,
}

/// Which bucket a row reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerKind {
    App,
    System,
    Unattributed,
}

/// Owner recovered by a reconciliation page for one schema name.
///
/// Only `app` and `system` are stored: an unresolved name is the absence of
/// an overlay entry, not a persisted `unattributed` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredOwner {
    pub kind: OwnerKind,
    pub owner: String,
}

impl StoredOwner {
    #[must_use]
    pub fn from_resolution(resolution: &OwnerResolution) -> Option<Self> {
        match resolution {
            OwnerResolution::App(owner) => Some(Self {
                kind: OwnerKind::App,
                owner: owner.clone(),
            }),
            OwnerResolution::System => Some(Self {
                kind: OwnerKind::System,
                owner: SYSTEM_OWNER.to_string(),
            }),
            OwnerResolution::Unknown => None,
        }
    }

    #[must_use]
    pub fn to_resolution(&self) -> OwnerResolution {
        match self.kind {
            OwnerKind::App => OwnerResolution::App(self.owner.clone()),
            OwnerKind::System => OwnerResolution::System,
            OwnerKind::Unattributed => OwnerResolution::Unknown,
        }
    }
}

/// Durable cursor + recovered owner overlay for the bounded job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReconciliationCheckpoint {
    /// When the last page wrote this checkpoint. `None` = no page has run.
    pub last_pass_at: Option<DateTime<Utc>>,
    /// Exclusive schema-name cursor. The next page starts after this name.
    /// `None` after a pass has visited every declared layout.
    pub resume_after: Option<String>,
    /// Schema names resolved by a durable catalog point-get after the live
    /// cache missed. Live cache still wins when it knows the owner.
    #[serde(default)]
    pub overlay: BTreeMap<String, StoredOwner>,
}

/// How far the job is from an exact owner split.
///
/// Structural plane bytes (`unattributable_plane_bytes`) are **not** in this
/// object: reconciliation only targets unresolved schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationLag {
    pub unresolved_schema_count: u64,
    pub unresolved_schema_bytes: u64,
    /// When the last reconciliation page touched the projection.
    pub last_pass_at: Option<DateTime<Utc>>,
    /// Next schema name the job will visit. `None` when the current pass
    /// finished every declared layout (or no layouts exist).
    pub resume_after: Option<String>,
}

/// One owner's line in the ranking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRow {
    /// App id, or a reserved bucket name ([`SYSTEM_OWNER`] /
    /// [`UNATTRIBUTED_OWNER`]).
    pub owner: String,
    pub kind: OwnerKind,
    pub live_bytes: u64,
    pub atom_count: u64,
    pub schema_count: u64,
    /// The schemas that produced this row, sorted. This is the attribution
    /// evidence: an operator can check the grouping instead of trusting it.
    pub schemas: Vec<String>,
}

/// `lastdb app storage` response body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppStorageReport {
    pub measured_at: DateTime<Utc>,
    /// Always false: point read of write-path totals, never a store walk.
    pub heavy: bool,
    /// True only when every live byte is attributed to an app or to `system`
    /// AND the meters were hydrated (see [`MetersOrigin`]). Un-hydrated meters
    /// report zeros with nothing unattributed, so the byte test alone would
    /// call a 16 GiB home an exact zero.
    pub complete: bool,
    /// Whether `live_total_bytes` is a measurement or a floor.
    #[serde(default)]
    pub meters_origin: MetersOrigin,
    /// Stable machine-readable cause when the meter projection is incomplete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
    pub budget_bytes: u64,
    pub live_total_bytes: u64,
    /// Bytes on app rows (`kind = app`).
    pub attributed_bytes: u64,
    /// Bytes on the reserved `system` row.
    pub system_bytes: u64,
    /// Bytes on the reserved `unattributed` row.
    pub unattributed_bytes: u64,
    /// Part of `unattributed_bytes` the write path could not trace to any
    /// schema: tips and bookkeeping written before the molecule was bound to
    /// its owner, plus any atom-total drift. Structural, not a resolver miss.
    ///
    /// Tip and bookkeeping bytes the write path *did* trace sit on their
    /// owner's row instead, so this number shrinks as a home writes.
    pub unattributable_plane_bytes: u64,
    /// Part of `unattributed_bytes` from metered schemas this node cannot
    /// resolve. A resolver miss, and the part a later slice can close.
    pub unresolved_schema_bytes: u64,
    /// Count/bytes still unresolved plus when the last reconciliation page
    /// ran. Distinct from `unattributable_plane_bytes`.
    pub reconciliation_lag: ReconciliationLag,
    /// App rows by `live_bytes` descending, then the reserved `system` and
    /// `unattributed` rows. Reserved rows stay last so the head of the list
    /// is always a real app ranking.
    pub owners: Vec<OwnerRow>,
}

#[derive(Default)]
struct Bucket {
    live_bytes: u64,
    atom_count: u64,
    schemas: Vec<String>,
}

impl Bucket {
    fn add(&mut self, schema: &str, live_bytes: u64, atom_count: u64) {
        self.live_bytes = self.live_bytes.saturating_add(live_bytes);
        self.atom_count = self.atom_count.saturating_add(atom_count);
        self.schemas.push(schema.to_string());
    }

    fn into_row(mut self, owner: String, kind: OwnerKind) -> OwnerRow {
        self.schemas.sort_unstable();
        self.schemas.dedup();
        OwnerRow {
            owner,
            kind,
            live_bytes: self.live_bytes,
            atom_count: self.atom_count,
            schema_count: self.schemas.len() as u64,
            schemas: self.schemas,
        }
    }
}

/// Resolve `owner_app_id` from a catalog schema row.
///
/// A present schema with an empty owner is a core plane (`system`). Absence
/// of a row is a miss (`Unknown`) and must land on `unattributed`.
#[must_use]
pub fn owner_from_catalog(owner_app_id: Option<&str>) -> OwnerResolution {
    owner_app_id
        .map(str::trim)
        .filter(|owner| !owner.is_empty())
        .map_or(OwnerResolution::System, |owner| {
            OwnerResolution::App(owner.to_string())
        })
}

/// Live cache wins; overlay fills a cache miss recovered by the job.
#[must_use]
pub fn resolve_with_overlay(
    schema: &str,
    live: OwnerResolution,
    overlay: &BTreeMap<String, StoredOwner>,
) -> OwnerResolution {
    match live {
        OwnerResolution::Unknown => overlay
            .get(schema)
            .map_or(OwnerResolution::Unknown, StoredOwner::to_resolution),
        other => other,
    }
}

/// Sorted unique union of declared catalog names and metered schema names.
///
/// This is the page key for the job: declared key layouts, plus any meter
/// names the catalog does not currently list. It is built from already-held
/// maps — never a store scan.
#[must_use]
pub fn declared_layout_names(
    declared: impl IntoIterator<Item = String>,
    metered: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    names.extend(declared);
    names.extend(metered);
    names.into_iter().collect()
}

/// Slice of `layouts` after `resume_after`, at most `page_size` names.
#[must_use]
pub fn next_page<'a>(
    layouts: &'a [String],
    resume_after: Option<&str>,
    page_size: usize,
) -> &'a [String] {
    let size = page_size.clamp(1, RECONCILE_PAGE_SIZE_MAX);
    let start = match resume_after {
        None => 0,
        Some(after) => layouts
            .iter()
            .position(|name| name.as_str() > after)
            .unwrap_or(layouts.len()),
    };
    let end = (start + size).min(layouts.len());
    &layouts[start..end]
}

/// Apply one page of lookup results to the checkpoint and advance the cursor.
///
/// `lookup` is a point-get of one declared name (cache, then durable catalog).
/// The job never asks for "every schema in the store".
pub fn apply_page(
    mut checkpoint: ReconciliationCheckpoint,
    page: &[String],
    layouts: &[String],
    lookup: &dyn Fn(&str) -> OwnerResolution,
    now: DateTime<Utc>,
) -> ReconciliationCheckpoint {
    for name in page {
        if let Some(stored) = StoredOwner::from_resolution(&lookup(name)) {
            checkpoint.overlay.insert(name.clone(), stored);
        }
    }
    checkpoint.last_pass_at = Some(now);
    checkpoint.resume_after = match page.last() {
        Some(last) if layouts.last() == Some(last) => None,
        Some(last) => Some(last.clone()),
        None => None,
    };
    checkpoint
}

/// Group the keep-small projection by owner.
///
/// `resolve_owner` is called once per metered schema name. Callers pass an
/// in-memory schema-metadata lookup; this function never touches the store.
///
/// An owner row carries its schemas' atom bytes **plus** the tip and
/// bookkeeping bytes the write path traced to those schemas' molecules.
/// Structural bytes that belong to no schema stay on the `unattributed` row.
pub fn build_report(
    snapshot: &KeepSmallSnapshot,
    resolve_owner: &dyn Fn(&str) -> OwnerResolution,
) -> AppStorageReport {
    build_report_with_lag(
        snapshot,
        resolve_owner,
        &ReconciliationCheckpoint::default(),
    )
}

/// Same as [`build_report`], with lag from a durable checkpoint.
pub fn build_report_with_lag(
    snapshot: &KeepSmallSnapshot,
    resolve_owner: &dyn Fn(&str) -> OwnerResolution,
    checkpoint: &ReconciliationCheckpoint,
) -> AppStorageReport {
    build_report_full(snapshot, resolve_owner, checkpoint, MetersOrigin::Measured)
}

/// Same as [`build_report_with_lag`], and told whether the meters were
/// hydrated. This is the form the live route uses; the two above default to
/// [`MetersOrigin::Measured`] for callers that build a snapshot themselves.
pub fn build_report_full(
    snapshot: &KeepSmallSnapshot,
    resolve_owner: &dyn Fn(&str) -> OwnerResolution,
    checkpoint: &ReconciliationCheckpoint,
    meters_origin: MetersOrigin,
) -> AppStorageReport {
    let mut apps: BTreeMap<String, Bucket> = BTreeMap::new();
    let mut system = Bucket::default();
    let mut unresolved = Bucket::default();

    let live_total_bytes = snapshot.totals.live_total_bytes();
    let atom_schema_bytes = snapshot
        .schemas
        .values()
        .fold(0u64, |acc, meter| acc.saturating_add(meter.live_bytes));
    // Everything the per-schema atom meters do not cover: tips, order log,
    // headers, secondary-index rows, and any atom-total drift.
    let plane_bytes = live_total_bytes.saturating_sub(atom_schema_bytes);
    // The part of that remainder the write path could trace to an owning
    // schema's molecules.
    let claimed_plane_bytes = snapshot
        .schemas
        .values()
        .fold(0u64, |acc, meter| acc.saturating_add(meter.plane_bytes()));
    // A claim larger than the remainder means the per-schema counters and the
    // global totals disagree — a home that hydrated per-schema plane bytes and
    // then lost global ones, for instance. Attribute nothing rather than hand
    // an app bytes the node cannot show it holds; the report stays exact and
    // reads as less complete, which is the honest direction to fail.
    let attribute_plane = claimed_plane_bytes <= plane_bytes;

    for (schema_name, meter) in &snapshot.schemas {
        let bytes = if attribute_plane {
            meter.live_bytes.saturating_add(meter.plane_bytes())
        } else {
            meter.live_bytes
        };
        match resolve_owner(schema_name) {
            OwnerResolution::App(owner) => {
                apps.entry(owner)
                    .or_default()
                    .add(schema_name, bytes, meter.atom_count);
            }
            OwnerResolution::System => {
                system.add(schema_name, bytes, meter.atom_count);
            }
            OwnerResolution::Unknown => {
                unresolved.add(schema_name, bytes, meter.atom_count);
            }
        }
    }

    // Tips and bookkeeping the write path could not trace to a schema stay
    // structural. Report the remainder instead of letting the row sum quietly
    // fall short of the live total.
    let unattributable_plane_bytes = if attribute_plane {
        plane_bytes.saturating_sub(claimed_plane_bytes)
    } else {
        plane_bytes
    };
    let unresolved_schema_bytes = unresolved.live_bytes;
    let unresolved_schema_count = unresolved.schemas.len() as u64;
    let unattributed_bytes = unresolved_schema_bytes.saturating_add(unattributable_plane_bytes);

    let mut owners: Vec<OwnerRow> = apps
        .into_iter()
        .map(|(owner, bucket)| bucket.into_row(owner, OwnerKind::App))
        .collect();
    owners.sort_by(|a, b| {
        b.live_bytes
            .cmp(&a.live_bytes)
            .then_with(|| a.owner.cmp(&b.owner))
    });
    let attributed_bytes = owners.iter().map(|row| row.live_bytes).sum();

    let system_bytes = system.live_bytes;
    if system_bytes > 0 || !system.schemas.is_empty() {
        owners.push(system.into_row(SYSTEM_OWNER.to_string(), OwnerKind::System));
    }
    if unattributed_bytes > 0 || !unresolved.schemas.is_empty() {
        let mut row = unresolved.into_row(UNATTRIBUTED_OWNER.to_string(), OwnerKind::Unattributed);
        // Plane bytes have no schema and no atom of their own; they only move
        // the row's byte total.
        row.live_bytes = unattributed_bytes;
        owners.push(row);
    }

    AppStorageReport {
        measured_at: Utc::now(),
        heavy: false,
        complete: unattributed_bytes == 0 && meters_origin.is_measured(),
        meters_origin,
        incomplete_reason: None,
        budget_bytes: LIVE_BUDGET_BYTES,
        live_total_bytes,
        attributed_bytes,
        system_bytes,
        unattributed_bytes,
        unattributable_plane_bytes,
        unresolved_schema_bytes,
        reconciliation_lag: ReconciliationLag {
            unresolved_schema_count,
            unresolved_schema_bytes,
            last_pass_at: checkpoint.last_pass_at,
            resume_after: checkpoint.resume_after.clone(),
        },
        owners,
    }
}
