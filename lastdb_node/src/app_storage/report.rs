//! Owner-ranking report built from one keep-small snapshot.

use super::{
    AppStorageReport, MetersOrigin, OwnerKind, OwnerResolution, OwnerRow, ReconciliationCheckpoint,
    ReconciliationLag, SYSTEM_OWNER, UNATTRIBUTED_OWNER,
};
use chrono::Utc;
use fold_db::db_operations::{KeepSmallSnapshot, LIVE_BUDGET_BYTES};
use std::collections::BTreeMap;

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

/// How the bytes outside the per-schema atom meters (tips, order log,
/// headers, secondary-index rows, atom-total drift) split between owners and
/// the structural remainder.
struct PlaneAttribution {
    plane_bytes: u64,
    claimed_plane_bytes: u64,
    attribute_plane: bool,
}

impl PlaneAttribution {
    fn of(snapshot: &KeepSmallSnapshot, live_total_bytes: u64) -> Self {
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
        Self {
            plane_bytes,
            claimed_plane_bytes,
            attribute_plane: claimed_plane_bytes <= plane_bytes,
        }
    }

    /// Tips and bookkeeping the write path could not trace to a schema stay
    /// structural. Report the remainder instead of letting the row sum quietly
    /// fall short of the live total.
    fn unattributable_bytes(&self) -> u64 {
        if self.attribute_plane {
            self.plane_bytes.saturating_sub(self.claimed_plane_bytes)
        } else {
            self.plane_bytes
        }
    }
}

/// Metered schemas grouped by the owner the resolver reports.
#[derive(Default)]
struct OwnerBuckets {
    apps: BTreeMap<String, Bucket>,
    system: Bucket,
    unresolved: Bucket,
}

fn bucket_schemas(
    snapshot: &KeepSmallSnapshot,
    resolve_owner: &dyn Fn(&str) -> OwnerResolution,
    attribute_plane: bool,
) -> OwnerBuckets {
    let mut buckets = OwnerBuckets::default();
    for (schema_name, meter) in &snapshot.schemas {
        let bytes = if attribute_plane {
            meter.live_bytes.saturating_add(meter.plane_bytes())
        } else {
            meter.live_bytes
        };
        match resolve_owner(schema_name) {
            OwnerResolution::App(owner) => {
                buckets
                    .apps
                    .entry(owner)
                    .or_default()
                    .add(schema_name, bytes, meter.atom_count);
            }
            OwnerResolution::System => {
                buckets.system.add(schema_name, bytes, meter.atom_count);
            }
            OwnerResolution::Unknown => {
                buckets.unresolved.add(schema_name, bytes, meter.atom_count);
            }
        }
    }
    buckets
}

/// App rows by `live_bytes` descending, then the reserved rows. Returns the
/// rows and the bytes on app rows.
fn rank_owner_rows(buckets: OwnerBuckets, unattributed_bytes: u64) -> (Vec<OwnerRow>, u64) {
    let OwnerBuckets {
        apps,
        system,
        unresolved,
    } = buckets;
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

    if system.live_bytes > 0 || !system.schemas.is_empty() {
        owners.push(system.into_row(SYSTEM_OWNER.to_string(), OwnerKind::System));
    }
    if unattributed_bytes > 0 || !unresolved.schemas.is_empty() {
        let mut row = unresolved.into_row(UNATTRIBUTED_OWNER.to_string(), OwnerKind::Unattributed);
        // Plane bytes have no schema and no atom of their own; they only move
        // the row's byte total.
        row.live_bytes = unattributed_bytes;
        owners.push(row);
    }
    (owners, attributed_bytes)
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
    let live_total_bytes = snapshot.totals.live_total_bytes();
    let plane = PlaneAttribution::of(snapshot, live_total_bytes);
    let buckets = bucket_schemas(snapshot, resolve_owner, plane.attribute_plane);

    let unattributable_plane_bytes = plane.unattributable_bytes();
    let unresolved_schema_bytes = buckets.unresolved.live_bytes;
    let unresolved_schema_count = buckets.unresolved.schemas.len() as u64;
    let unattributed_bytes = unresolved_schema_bytes.saturating_add(unattributable_plane_bytes);
    let system_bytes = buckets.system.live_bytes;
    let (owners, attributed_bytes) = rank_owner_rows(buckets, unattributed_bytes);

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
