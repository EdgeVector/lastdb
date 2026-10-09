//! Part of `lastdb_local_maintain`.
// lint:file-size-ok moved verbatim from lastdb_local_maintain.rs; cohesive unit, split further in a later pass

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use super::home::{open_home, HomeStore};
use fold_db::hex::sha256_hex;
use fold_db::storage::traits::{KvStore, NamespacedStore, PhysicalScanCursor};
use lastdb_node::atom_gc_reap::{
    classify_group, molecule_uuid_of_partition, AtomCopy, AtomGroup, AtomKeyShape,
    HomeAtomKeyEncoding, ReapPolicy, ReapVerdict,
};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub(crate) struct AtomGcAuditReport {
    ok: bool,
    mode: &'static str,
    home: String,
    store_root: String,
    seam: &'static str,
    primary_guard: &'static str,
    atom_key_encoding: &'static str,
    referenced_atom_uuids: u64,
    atom_body_keys_scanned: u64,
    atom_body_keys_referenced: u64,
    atom_body_keys_unreferenced: u64,
    flat_atom_body_keys: u64,
    prefixed_atom_body_keys: u64,
    opaque_atom_body_keys: u64,
    locator_rows: u64,
    duplicate_uuid_groups: u64,
    content_hash_comparable_groups: u64,
    content_hash_equal_groups: u64,
    content_hash_conflict_groups: u64,
    schema_coverage: Vec<AtomGcSchemaCoverage>,
    namespaces_scanned: Vec<String>,
    duplicate_details: Vec<AtomGcDuplicateGroup>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AtomGcSchemaCoverage {
    schema: &'static str,
    visible_in_schema_catalog: bool,
    status: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct AtomGcDuplicateGroup {
    atom_uuid: String,
    keys: Vec<String>,
    referenced: bool,
    /// Whether every copy in the group yielded a content hash. `false` means at
    /// least one body was opaque, so equality was never evaluated.
    content_comparable: bool,
    content_sha256_equal: bool,
    content_sha256: Vec<String>,
}

/// What a reap pass did, or would do.
#[derive(Debug, Serialize)]
pub(crate) struct AtomGcReapReport {
    ok: bool,
    mode: &'static str,
    home: String,
    store_root: String,
    seam: &'static str,
    atom_key_encoding: &'static str,
    reap_unreferenced_orphans: bool,
    /// Body keys removed (or, in a plan, that would be).
    deleted_keys: u64,
    /// Body keys deliberately left in place.
    kept_keys: u64,
    /// Groups the rules refused to judge. These delete nothing.
    ambiguous_groups: u64,
    groups_scanned: u64,
    delete_groups: u64,
    keep_groups: u64,
    /// Reason → group count, for both deletes and refusals.
    reason_counts: BTreeMap<String, u64>,
    /// Molecule uuids whose partitions lost a duplicate.
    affected_molecules: Vec<String>,
    /// Schema names resolved from those molecules, where the catalog allowed it.
    affected_schemas: Vec<String>,
    schema_coverage: Vec<AtomGcSchemaCoverage>,
    namespaces_scanned: Vec<String>,
    decisions: Vec<AtomGcReapDecision>,
    decisions_truncated: bool,
    notes: Vec<String>,
}

/// One group's decision, with the reason that authorized it.
#[derive(Debug, Serialize)]
pub(crate) struct AtomGcReapDecision {
    atom_uuid: String,
    verdict: &'static str,
    reason: String,
    referenced: bool,
    survivor_key: String,
    deleted_keys: Vec<String>,
}

pub(crate) fn atom_gc_audit(
    home: &Path,
    detail_limit: usize,
    i_know_this_is_primary: bool,
    json: bool,
) -> Result<(), String> {
    let opened = open_home(home, i_know_this_is_primary)?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let report = runtime.block_on(atom_gc_audit_report(
        &opened,
        home,
        detail_limit,
        i_know_this_is_primary,
    ))?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("json report: {e}"))?
        );
    } else {
        println!("atom-gc audit: {} ({})", report.mode, report.primary_guard);
        println!("  store_root: {}", report.store_root);
        println!("  seam: {}", report.seam);
        println!("  atom_key_encoding: {}", report.atom_key_encoding);
        println!("  referenced_atom_uuids: {}", report.referenced_atom_uuids);
        println!("  locator_rows: {}", report.locator_rows);
        println!(
            "  atom_body_keys: scanned={} referenced={} unreferenced={} flat={} prefixed={} opaque={}",
            report.atom_body_keys_scanned,
            report.atom_body_keys_referenced,
            report.atom_body_keys_unreferenced,
            report.flat_atom_body_keys,
            report.prefixed_atom_body_keys,
            report.opaque_atom_body_keys
        );
        println!(
            "  duplicate_uuid_groups: {} comparable={} equal={} conflicts={}",
            report.duplicate_uuid_groups,
            report.content_hash_comparable_groups,
            report.content_hash_equal_groups,
            report.content_hash_conflict_groups
        );
        for coverage in &report.schema_coverage {
            println!("  schema {}: {}", coverage.schema, coverage.status);
        }
    }
    Ok(())
}

/// One pass over the home, shared by the audit and the reaper.
///
/// Built once and handed to both so a destructive pass can never disagree with
/// the audit that authorized it: same reference keep-set, same grouping, same
/// content hashes.
pub(crate) struct AtomGcScan {
    /// One entry per atom uuid, in key order.
    groups: Vec<AtomGroup>,
    namespaces_scanned: BTreeSet<String>,
    schema_names: BTreeSet<String>,
    referenced_atom_uuids: u64,
    flat_atom_body_keys: u64,
    prefixed_atom_body_keys: u64,
    /// Body rows whose value did not parse as content — the shape a home opened
    /// without its at-rest key produces.
    opaque_atom_body_keys: u64,
    locator_rows: u64,
    /// The encoding the home's durable marker names, or `None` when it carries
    /// no marker.
    encoding_marker: Option<HomeAtomKeyEncoding>,
}

impl AtomGcScan {
    /// The encoding to judge by. A home with no marker has never been migrated,
    /// which is exactly [`HomeAtomKeyEncoding::Flat`] — but see
    /// [`atom_gc_reap`], which refuses to *execute* on an unmarked home that
    /// nonetheless holds prefixed keys.
    fn encoding(&self) -> HomeAtomKeyEncoding {
        self.encoding_marker.unwrap_or(HomeAtomKeyEncoding::Flat)
    }

    fn atom_body_keys_scanned(&self) -> u64 {
        self.groups.iter().map(|g| g.copies.len() as u64).sum()
    }
}

// lint:fn-size-ok moved verbatim from lastdb_local_maintain.rs; splitting this function is separate work
pub(crate) async fn scan_atom_gc(store: &dyn NamespacedStore) -> Result<AtomGcScan, String> {
    let namespaces = store
        .list_namespaces()
        .await
        .map_err(|e| format!("list namespaces: {e}"))?;
    let namespace_set: BTreeSet<String> = namespaces.into_iter().collect();
    let mut namespaces_scanned = BTreeSet::new();

    let mut refs = HashSet::new();
    for (namespace, prefix) in [
        ("main", "mk:"),
        ("tips", "mk:"),
        ("field_tips", "mk:"),
        ("main", "tv:"),
        ("tips", "tv:"),
        ("field_tip_versions", "tv:"),
        ("main", "history:"),
        ("tips", "history:"),
        ("main", "conflict:"),
        ("sync_conflicts", "conflict:"),
        ("main", "ref:"),
    ] {
        collect_atom_refs_from_prefix(
            store,
            &namespace_set,
            &mut namespaces_scanned,
            namespace,
            prefix,
            &mut refs,
        )
        .await?;
    }

    let mut copies_by_uuid: BTreeMap<String, Vec<AtomCopy>> = BTreeMap::new();
    let mut flat_atom_body_keys = 0u64;
    let mut prefixed_atom_body_keys = 0u64;
    let mut opaque_atom_body_keys = 0u64;
    collect_atom_body_rows(
        store,
        &namespace_set,
        &mut namespaces_scanned,
        "main",
        Some("atom:"),
        &mut copies_by_uuid,
        &mut flat_atom_body_keys,
        &mut prefixed_atom_body_keys,
        &mut opaque_atom_body_keys,
    )
    .await?;
    collect_atom_body_rows(
        store,
        &namespace_set,
        &mut namespaces_scanned,
        "atoms",
        Some("atom:"),
        &mut copies_by_uuid,
        &mut flat_atom_body_keys,
        &mut prefixed_atom_body_keys,
        &mut opaque_atom_body_keys,
    )
    .await?;
    if namespace_set.contains("atoms") && !namespaces_scanned.contains("atoms") {
        collect_atom_body_rows(
            store,
            &namespace_set,
            &mut namespaces_scanned,
            "atoms",
            None,
            &mut copies_by_uuid,
            &mut flat_atom_body_keys,
            &mut prefixed_atom_body_keys,
            &mut opaque_atom_body_keys,
        )
        .await?;
    }

    let locators = collect_atom_locators(store, &namespace_set, &mut namespaces_scanned).await?;
    let schema_names = collect_schema_names(store, &namespace_set, &mut namespaces_scanned).await?;
    let encoding_marker = read_encoding_marker(store).await?;

    let groups = copies_by_uuid
        .into_iter()
        .map(|(atom_uuid, copies)| AtomGroup {
            referenced: refs.contains(&atom_uuid),
            locator_partition: locators.get(&atom_uuid).cloned(),
            atom_uuid,
            copies,
        })
        .collect();

    Ok(AtomGcScan {
        groups,
        namespaces_scanned,
        schema_names,
        referenced_atom_uuids: refs.len() as u64,
        flat_atom_body_keys,
        prefixed_atom_body_keys,
        opaque_atom_body_keys,
        locator_rows: locators.len() as u64,
        encoding_marker,
    })
}

pub(crate) async fn atom_gc_audit_report(
    opened: &HomeStore,
    home: &Path,
    detail_limit: usize,
    i_know_this_is_primary: bool,
) -> Result<AtomGcAuditReport, String> {
    let scan = scan_atom_gc(opened.store.as_ref()).await?;

    let mut atom_body_keys_referenced = 0u64;
    let mut atom_body_keys_unreferenced = 0u64;
    let mut duplicate_uuid_groups = 0u64;
    let mut content_hash_comparable_groups = 0u64;
    let mut content_hash_equal_groups = 0u64;
    let mut content_hash_conflict_groups = 0u64;
    let mut duplicate_details = Vec::new();

    for group in &scan.groups {
        let n = group.copies.len() as u64;
        if group.referenced {
            atom_body_keys_referenced += n;
        } else {
            atom_body_keys_unreferenced += n;
        }
        if group.copies.len() <= 1 {
            continue;
        }
        duplicate_uuid_groups += 1;
        // A group is comparable only when every copy produced a content hash.
        // An opaque row makes the whole group incomparable rather than silently
        // comparing the copies that happened to read.
        let hashes: Option<BTreeSet<String>> = group
            .copies
            .iter()
            .map(|c| c.content_sha256.clone())
            .collect();
        let (comparable, equal, hashes) = match hashes {
            Some(hashes) => {
                content_hash_comparable_groups += 1;
                let equal = hashes.len() == 1;
                if equal {
                    content_hash_equal_groups += 1;
                } else {
                    content_hash_conflict_groups += 1;
                }
                (true, equal, hashes.into_iter().collect())
            }
            None => (false, false, Vec::new()),
        };
        if duplicate_details.len() < detail_limit {
            duplicate_details.push(AtomGcDuplicateGroup {
                atom_uuid: group.atom_uuid.clone(),
                keys: group.copies.iter().map(|c| c.key.clone()).collect(),
                referenced: group.referenced,
                content_comparable: comparable,
                content_sha256_equal: equal,
                content_sha256: hashes,
            });
        }
    }

    let schema_coverage = schema_coverage_of(&scan.schema_names);

    Ok(AtomGcAuditReport {
        ok: true,
        mode: "dry-run-audit",
        home: home.display().to_string(),
        store_root: opened.store_root.display().to_string(),
        seam: opened.seam,
        primary_guard: if i_know_this_is_primary {
            "overridden"
        } else {
            "cow-copy-required"
        },
        atom_key_encoding: encoding_label(scan.encoding_marker),
        referenced_atom_uuids: scan.referenced_atom_uuids,
        atom_body_keys_scanned: scan.atom_body_keys_scanned(),
        atom_body_keys_referenced,
        atom_body_keys_unreferenced,
        flat_atom_body_keys: scan.flat_atom_body_keys,
        prefixed_atom_body_keys: scan.prefixed_atom_body_keys,
        opaque_atom_body_keys: scan.opaque_atom_body_keys,
        locator_rows: scan.locator_rows,
        duplicate_uuid_groups,
        content_hash_comparable_groups,
        content_hash_equal_groups,
        content_hash_conflict_groups,
        schema_coverage,
        namespaces_scanned: scan.namespaces_scanned.into_iter().collect(),
        duplicate_details,
        notes: audit_notes(opened.seam),
    })
}

pub(crate) fn schema_coverage_of(schema_names: &BTreeSet<String>) -> Vec<AtomGcSchemaCoverage> {
    ["Card", "BoardCards", "MilestoneCards"]
        .into_iter()
        .map(|schema| {
            let visible = schema_names.contains(schema);
            AtomGcSchemaCoverage {
                schema,
                visible_in_schema_catalog: visible,
                status: if visible {
                    "visible"
                } else {
                    "absent in copied fixture"
                },
            }
        })
        .collect()
}

pub(crate) fn encoding_label(marker: Option<HomeAtomKeyEncoding>) -> &'static str {
    match marker {
        Some(HomeAtomKeyEncoding::PartitionPrefix) => "partition_prefix",
        Some(HomeAtomKeyEncoding::Flat) => "flat",
        None => "unmarked (treated as flat)",
    }
}

pub(crate) fn audit_notes(seam: &'static str) -> Vec<String> {
    let mut notes = vec![
        "audit-only: no keys were deleted, copied, or rewritten".into(),
        "run against a CoW clone first; destructive atom GC is `atom-gc-reap`".into(),
    ];
    if seam != "at-rest-seam" {
        notes.push(
            "opened WITHOUT the at-rest seam (no readable identity.key): tip values do not \
             parse, so referenced counts read as zero, and body values are sealed ciphertext, \
             so content hashes cannot be compared. Counts here are structural only."
                .into(),
        );
    }
    notes
}

#[path = "atom_gc/collect.rs"]
mod collect;
#[path = "atom_gc/reap.rs"]
mod reap;

pub(crate) use collect::*;
pub(crate) use reap::*;
