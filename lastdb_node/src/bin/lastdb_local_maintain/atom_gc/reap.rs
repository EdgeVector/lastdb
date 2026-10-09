//! Atom GC reaper: plan/execute redundant-copy deletion. Moved verbatim from `atom_gc.rs`.

use super::*;

pub(crate) struct AtomGcReapArgs<'a> {
    pub(crate) home: &'a Path,
    pub(crate) execute: bool,
    pub(crate) policy: ReapPolicy,
    pub(crate) detail_limit: usize,
    pub(crate) i_know_this_is_primary: bool,
    pub(crate) json: bool,
}

/// Reclaim the redundant atom-body copies the audit's rules prove safe.
///
/// Three gates stand in front of the delete, and each exists because the
/// failure it prevents is silent:
///
/// 1. **Primary is never executed against.** `--i-know-this-is-primary` buys a
///    plan, not a delete. The first destructive proof surface for atom GC is a
///    CoW clone; promoting it to the primary is a separate, supervised decision
///    (`lastdb-safe-upgrade`'s gate), not a flag on this command.
/// 2. **No at-rest seam, no delete.** Without the home's identity key every tip
///    reads as unparseable, so the reference keep-set comes back empty and
///    every live body looks like an orphan.
/// 3. **No encoding marker over a prefixed home, no delete.** Which copy is
///    canonical depends entirely on the encoding; guessing it inverts the rules.
pub(crate) fn atom_gc_reap(args: &AtomGcReapArgs<'_>) -> Result<(), String> {
    if args.execute && args.i_know_this_is_primary {
        return Err(
            "refusing --execute with --i-know-this-is-primary: this command has no \
                    primary destructive mode. Reap a CoW copy, prove reads, and promote the \
                    result under the supervised upgrade gate."
                .into(),
        );
    }

    let opened = open_home(args.home, args.i_know_this_is_primary)?;
    if args.execute && opened.seam != "at-rest-seam" {
        return Err(format!(
            "refusing --execute: home opened as {} (no readable identity.key). Reference and \
             content checks are both blind at rest, so every delete would be unproven.",
            opened.seam
        ));
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio: {e}"))?;
    let report = runtime.block_on(atom_gc_reap_run(&opened, args.home, args))?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("json report: {e}"))?
        );
    } else {
        println!("atom-gc reap: {} ({})", report.mode, report.seam);
        println!("  store_root: {}", report.store_root);
        println!("  atom_key_encoding: {}", report.atom_key_encoding);
        println!(
            "  groups: scanned={} delete={} keep={} ambiguous={}",
            report.groups_scanned,
            report.delete_groups,
            report.keep_groups,
            report.ambiguous_groups
        );
        println!(
            "  keys: deleted={} kept={}",
            report.deleted_keys, report.kept_keys
        );
        for (reason, count) in &report.reason_counts {
            println!("  reason {reason}: {count}");
        }
        if !report.affected_schemas.is_empty() {
            println!("  affected_schemas: {}", report.affected_schemas.join(", "));
        }
        for coverage in &report.schema_coverage {
            println!("  schema {}: {}", coverage.schema, coverage.status);
        }
        for note in &report.notes {
            println!("  note: {note}");
        }
    }
    Ok(())
}

// lint:fn-size-ok moved verbatim from lastdb_local_maintain.rs; splitting this function is separate work
pub(crate) async fn atom_gc_reap_run(
    opened: &HomeStore,
    home: &Path,
    args: &AtomGcReapArgs<'_>,
) -> Result<AtomGcReapReport, String> {
    let scan = scan_atom_gc(opened.store.as_ref()).await?;
    let encoding = scan.encoding();

    if args.execute && scan.encoding_marker.is_none() && scan.prefixed_atom_body_keys > 0 {
        return Err(format!(
            "refusing --execute: home carries {} partition-prefixed body keys but no \
             amigr:atom_key_encoding_v1 marker. Which copy is canonical is undecidable here.",
            scan.prefixed_atom_body_keys
        ));
    }

    let mut report = AtomGcReapReport {
        ok: true,
        mode: if args.execute { "execute" } else { "dry-run" },
        home: home.display().to_string(),
        store_root: opened.store_root.display().to_string(),
        seam: opened.seam,
        atom_key_encoding: encoding_label(scan.encoding_marker),
        reap_unreferenced_orphans: args.policy.reap_unreferenced_orphans,
        deleted_keys: 0,
        kept_keys: 0,
        ambiguous_groups: 0,
        groups_scanned: scan.groups.len() as u64,
        delete_groups: 0,
        keep_groups: 0,
        reason_counts: BTreeMap::new(),
        affected_molecules: Vec::new(),
        affected_schemas: Vec::new(),
        schema_coverage: schema_coverage_of(&scan.schema_names),
        namespaces_scanned: scan.namespaces_scanned.iter().cloned().collect(),
        decisions: Vec::new(),
        decisions_truncated: false,
        notes: Vec::new(),
    };
    let mut affected_molecules: BTreeSet<String> = BTreeSet::new();
    // Namespace handles are opened once and reused: a delete pass over a large
    // home must not reopen a namespace per key.
    let mut namespace_handles = BTreeMap::new();

    for group in &scan.groups {
        let verdict = classify_group(group, encoding, args.policy);
        let (label, reason) = match &verdict {
            ReapVerdict::Keep { reason } => ("keep", *reason),
            ReapVerdict::Delete { reason, .. } => ("delete", *reason),
            ReapVerdict::Ambiguous { reason } => ("ambiguous", *reason),
        };
        *report
            .reason_counts
            .entry(format!("{label}:{reason}"))
            .or_insert(0) += 1;

        let delete_keys = verdict.delete_keys();
        match &verdict {
            ReapVerdict::Keep { .. } => {
                report.keep_groups += 1;
                report.kept_keys += group.copies.len() as u64;
            }
            ReapVerdict::Ambiguous { .. } => {
                report.ambiguous_groups += 1;
                report.kept_keys += group.copies.len() as u64;
            }
            ReapVerdict::Delete { survivor_key, .. } => {
                report.delete_groups += 1;
                report.deleted_keys += delete_keys.len() as u64;
                report.kept_keys += group.copies.len() as u64 - delete_keys.len() as u64;
                // Attributed from every partition in the group, survivor
                // included: the copy being deleted is usually the flat one,
                // which names no partition, so attributing from deletions alone
                // would report an empty blast radius for the common case.
                for copy in &group.copies {
                    if let Some(molecule) =
                        copy.shape.partition().and_then(molecule_uuid_of_partition)
                    {
                        affected_molecules.insert(molecule.to_string());
                    }
                }
                if args.execute {
                    execute_group_delete(
                        opened.store.as_ref(),
                        &mut namespace_handles,
                        group,
                        survivor_key,
                        delete_keys,
                    )
                    .await?;
                }
            }
        }

        if report.decisions.len() < args.detail_limit {
            let survivor_key = match &verdict {
                ReapVerdict::Delete { survivor_key, .. } => survivor_key.clone(),
                _ => String::new(),
            };
            report.decisions.push(AtomGcReapDecision {
                atom_uuid: group.atom_uuid.clone(),
                verdict: label,
                reason: reason.to_string(),
                referenced: group.referenced,
                survivor_key,
                deleted_keys: delete_keys.to_vec(),
            });
        } else {
            report.decisions_truncated = true;
        }
    }

    report.affected_schemas = resolve_schema_names(&affected_molecules, &scan.schema_names);
    report.affected_molecules = affected_molecules.into_iter().collect();
    report.notes = reap_notes(args, &scan);
    Ok(report)
}

/// Delete one group's redundant keys, then prove the survivor is still there.
///
/// The verify is per group and *after* the delete rather than a final sweep:
/// the point of the check is to stop a pass that has started removing
/// reachability, and a sweep at the end would learn that only once every group
/// had already been processed.
pub(crate) async fn execute_group_delete(
    store: &dyn NamespacedStore,
    namespace_handles: &mut BTreeMap<String, Arc<dyn fold_db::storage::traits::KvStore>>,
    group: &AtomGroup,
    survivor_key: &str,
    delete_keys: &[String],
) -> Result<(), String> {
    for copy in group.copies.iter().filter(|c| delete_keys.contains(&c.key)) {
        let kv = namespace_kv(store, namespace_handles, &copy.namespace).await?;
        kv.delete(copy.stored_key.as_bytes())
            .await
            .map_err(|e| format!("delete {}: {e}", copy.key))?;
    }

    // An orphan reap has no survivor by construction — nothing to verify.
    if survivor_key.is_empty() {
        return Ok(());
    }
    let Some(survivor) = group.copies.iter().find(|c| c.key == survivor_key) else {
        return Err(format!(
            "atom {}: survivor {survivor_key} vanished from the group",
            group.atom_uuid
        ));
    };
    let kv = namespace_kv(store, namespace_handles, &survivor.namespace).await?;
    let present = kv
        .exists(survivor.stored_key.as_bytes())
        .await
        .map_err(|e| format!("verify {survivor_key}: {e}"))?;
    if !present {
        return Err(format!(
            "atom {}: survivor {survivor_key} is missing after the delete — pass aborted",
            group.atom_uuid
        ));
    }
    Ok(())
}

pub(crate) async fn namespace_kv(
    store: &dyn NamespacedStore,
    handles: &mut BTreeMap<String, Arc<dyn fold_db::storage::traits::KvStore>>,
    namespace: &str,
) -> Result<Arc<dyn fold_db::storage::traits::KvStore>, String> {
    if let Some(kv) = handles.get(namespace) {
        return Ok(Arc::clone(kv));
    }
    let kv = store
        .open_namespace(namespace)
        .await
        .map_err(|e| format!("open namespace {namespace}: {e}"))?;
    handles.insert(namespace.to_string(), Arc::clone(&kv));
    Ok(kv)
}

/// Map affected molecule uuids back to schema names, where the catalog allows.
///
/// A molecule uuid is `sha256(schema:field)`, so this is a forward computation
/// over catalog names rather than an inversion — and it is best-effort by
/// design: a name the catalog does not carry simply does not appear, which is
/// why the report also lists the raw molecule uuids.
pub(crate) fn resolve_schema_names(
    molecules: &BTreeSet<String>,
    schema_names: &BTreeSet<String>,
) -> Vec<String> {
    if molecules.is_empty() || schema_names.is_empty() {
        return Vec::new();
    }
    let mut out = BTreeSet::new();
    for schema in schema_names {
        for field in COMMON_FIELD_NAMES {
            let uuid = fold_db::atom::deterministic_molecule_uuid(schema, field);
            if molecules.contains(&uuid) {
                out.insert(schema.clone());
            }
        }
    }
    out.into_iter().collect()
}

/// Field names probed when attributing a molecule uuid to a schema.
///
/// The catalog names schemas, not their fields, so attribution probes the field
/// names this workspace's board schemas actually use. Attribution is a reporting
/// nicety — nothing is deleted or kept because of it — so an unlisted field
/// costs a blank in `affected_schemas`, never a wrong decision.
pub(crate) const COMMON_FIELD_NAMES: &[&str] = &[
    "slug",
    "title",
    "body",
    "board",
    "column",
    "milestone",
    "status",
    "content",
    "name",
    "position",
    "tags",
    "repo",
    "kind",
];

pub(crate) fn reap_notes(args: &AtomGcReapArgs<'_>, scan: &AtomGcScan) -> Vec<String> {
    let mut notes = Vec::new();
    if args.execute {
        notes.push(
            "executed: redundant copies were deleted and each surviving body was re-probed".into(),
        );
    } else {
        notes.push("plan only: nothing was deleted; re-run with --execute".into());
    }
    if !args.policy.reap_unreferenced_orphans {
        notes.push(
            "unreferenced orphans retained; --reap-unreferenced-orphans opts into deleting \
             bodies the reference scan found no row for"
                .into(),
        );
    }
    if scan.opaque_atom_body_keys > 0 {
        notes.push(format!(
            "{} body rows did not parse as content and blocked their groups",
            scan.opaque_atom_body_keys
        ));
    }
    notes
}
