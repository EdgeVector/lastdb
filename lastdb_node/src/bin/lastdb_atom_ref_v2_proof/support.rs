//! Copy inspection, crash-resume and v1 drain helpers for the proof run.

use super::*;

pub(super) fn collect_molecules_by_prefix(
    host: &lastdb_node::Host,
) -> Result<BTreeMap<Option<String>, BTreeSet<String>>, String> {
    let mut molecules_by_prefix: BTreeMap<Option<String>, BTreeSet<String>> =
        BTreeMap::from([(None, BTreeSet::new())]);
    for schema in host
        .db
        .schema_manager()
        .get_schemas()
        .map_err(|error| error.to_string())?
        .into_values()
    {
        for field in schema.runtime_fields.values() {
            let Some(molecule_uuid) = field.common().molecule_uuid() else {
                continue;
            };
            molecules_by_prefix
                .entry(field.common().storage_prefix().map(str::to_string))
                .or_default()
                .insert(molecule_uuid.clone());
        }
    }
    Ok(molecules_by_prefix)
}

pub(super) async fn interrupt_and_resume_first_page(
    host: lastdb_node::Host,
    home: &Path,
    storage_prefix: Option<&str>,
    page: usize,
    life: &mut Lifetime,
) -> Result<(lastdb_node::Host, AtomRefCrashResumeProof), RunError> {
    let before = await_with_progress(
        life,
        host.db
            .db_ops()
            .atoms()
            .atom_ref_v2_backfill_status(storage_prefix),
    )
    .await?;
    if !matches!(
        before.phase,
        AtomRefBackfillPhase::Complete | AtomRefBackfillPhase::Blocked
    ) {
        await_with_progress(
            life,
            host.db
                .db_ops()
                .atoms()
                .reindex_atom_ref_v2_edges(storage_prefix, Some(page)),
        )
        .await?;
    }
    let interrupted = await_with_progress(
        life,
        host.db
            .db_ops()
            .atoms()
            .atom_ref_v2_backfill_status(storage_prefix),
    )
    .await?;
    let interrupted_phase = interrupted.phase;
    let interrupted_slots_walked = interrupted.slots_walked;
    let interrupted_edges_written = interrupted.edges_written;
    let interrupted_cursor = interrupted.after_tip_key.clone();
    drop(host);
    life.set_phase("crash_reboot");
    let host = await_with_progress(life, lastdb_node::Host::boot(home)).await?;
    let resumed = await_with_progress(
        life,
        host.db
            .db_ops()
            .atoms()
            .atom_ref_v2_backfill_status(storage_prefix),
    )
    .await?;
    let cursor_survived = resumed.phase == interrupted_phase
        && resumed.slots_walked == interrupted_slots_walked
        && resumed.edges_written == interrupted_edges_written
        && resumed.after_tip_key == interrupted_cursor;
    if !cursor_survived {
        return Err(RunError::Fail(format!(
            "compact rebuild cursor did not survive the crash interrupt: before={interrupted:?} after={resumed:?}"
        )));
    }
    Ok((
        host,
        AtomRefCrashResumeProof {
            interrupted_phase,
            interrupted_slots_walked,
            interrupted_edges_written,
            cursor_survived,
            resumed_to_complete: false,
        },
    ))
}

pub(super) async fn drain_and_compact(
    host: &lastdb_node::Host,
    proofs: &[PrefixProof],
    page: usize,
    max_pages: usize,
    v1_allocated_before: u64,
    v2_allocated_before: u64,
    life: &mut Lifetime,
) -> Result<ReclaimProof, RunError> {
    let readiness_prefixes = proofs
        .iter()
        .map(|proof| proof.storage_prefix.clone())
        .collect::<Vec<_>>();
    let mut total_pages = 0usize;
    loop {
        if life.timed_out() {
            return Err(RunError::Timeout);
        }
        let status =
            await_with_progress(life, host.db.db_ops().atoms().atom_ref_v1_drain_status()).await?;
        if status.completed {
            break;
        }
        if total_pages >= max_pages {
            return Err(RunError::Fail(format!(
                "legacy drain exceeded {max_pages} pages"
            )));
        }
        await_with_progress(
            life,
            host.db
                .db_ops()
                .atoms()
                .drain_atom_ref_v1_page(page, &readiness_prefixes),
        )
        .await?;
        total_pages = total_pages.saturating_add(1);
        life.pages = life.pages.saturating_add(1);
        life.progress(false);
    }
    let drain_status =
        await_with_progress(life, host.db.db_ops().atoms().atom_ref_v1_drain_status()).await?;
    let v1_keys_remain =
        await_with_progress(life, host.db.db_ops().atoms().atom_ref_v1_keys_remain()).await?;
    if v1_keys_remain {
        return Err(RunError::Fail(
            "legacy reverse-edge keys remain after the physical drain".to_string(),
        ));
    }

    life.set_phase("compact");
    let v1_compaction =
        await_with_progress(life, host.db.compact_collection(V1_COLLECTION, false)).await?;
    if !v1_compaction.executed {
        return Err(RunError::Fail(format!(
            "legacy reverse-edge compaction did not execute: {v1_compaction:?}"
        )));
    }
    let v2_compaction =
        await_with_progress(life, host.db.compact_collection(V2_COLLECTION, false)).await?;
    if !v2_compaction.executed {
        return Err(RunError::Fail(format!(
            "compact reverse-edge compaction did not execute: {v2_compaction:?}"
        )));
    }

    let v1_allocated_after = collection_allocated_bytes(host, V1_COLLECTION);
    let v2_allocated_after = collection_allocated_bytes(host, V2_COLLECTION);
    let atoms_allocated_after = collection_allocated_bytes(host, ATOMS_COLLECTION);
    let before = v1_allocated_before.saturating_add(v2_allocated_before);
    let after = v1_allocated_after.saturating_add(v2_allocated_after);
    let allocated_bytes_reclaimed = before.saturating_sub(after);
    let active_edges = proofs
        .iter()
        .filter_map(|proof| proof.audit.as_ref())
        .map(|audit| audit.expected_active_edges)
        .sum::<u64>();
    let v2_bytes_per_edge = v2_allocated_after.checked_div(active_edges).unwrap_or(0);
    let v2_to_atoms_basis_points = v2_allocated_after
        .saturating_mul(10_000)
        .checked_div(atoms_allocated_after)
        .unwrap_or(u64::MAX);

    if v1_compaction.live_keys != 0 || v1_allocated_after > MAX_DRAINED_V1_BYTES {
        return Err(RunError::Fail(format!(
            "legacy reverse-edge plane retained {} live keys and {v1_allocated_after} allocated bytes",
            v1_compaction.live_keys
        )));
    }
    if v2_allocated_after > MAX_DRAINED_V2_BYTES {
        return Err(RunError::Fail(format!(
            "compact reverse-edge plane retained {v2_allocated_after} bytes, above {MAX_DRAINED_V2_BYTES}"
        )));
    }
    if v2_bytes_per_edge > MAX_V2_BYTES_PER_EDGE {
        return Err(RunError::Fail(format!(
            "compact reverse-edge plane uses {v2_bytes_per_edge} bytes per edge, above {MAX_V2_BYTES_PER_EDGE}"
        )));
    }
    if v2_to_atoms_basis_points > MAX_V2_TO_ATOMS_BASIS_POINTS {
        return Err(RunError::Fail(format!(
            "compact index-to-atoms ratio is {v2_to_atoms_basis_points} basis points, above {MAX_V2_TO_ATOMS_BASIS_POINTS}"
        )));
    }
    let reclaim_floor_skipped = !reclaim_floor_applies(total_pages, v1_allocated_before);
    if allocated_bytes_reclaimed < MIN_RECLAIM_BYTES && !reclaim_floor_skipped {
        return Err(RunError::Fail(format!(
            "reverse-edge drain reclaimed {allocated_bytes_reclaimed} bytes, below {MIN_RECLAIM_BYTES}"
        )));
    }

    Ok(ReclaimProof {
        v1_allocated_before,
        v2_allocated_before,
        v1_allocated_after,
        v2_allocated_after,
        atoms_allocated_after,
        allocated_bytes_reclaimed,
        active_edges,
        v2_bytes_per_edge,
        v2_to_atoms_basis_points,
        drain_pages: total_pages,
        drain_status,
        v1_keys_remain,
        v1_compaction,
        v2_compaction,
        reclaim_floor_skipped,
    })
}

/// The 10 GiB reclaim floor applies when this run still had v1 to drain, or
/// when v1 was still larger than the leftover cap. An already-drained copy
/// with v1 at leftover size cannot reclaim 10 GiB and must not fail that
/// clause.
pub(super) fn reclaim_floor_applies(drain_pages: usize, v1_allocated_before: u64) -> bool {
    drain_pages > 0 || v1_allocated_before > MAX_DRAINED_V1_BYTES
}

pub(super) fn canonical_dir(path: &Path) -> Result<PathBuf, String> {
    path.canonicalize()
        .map_err(|error| format!("canonicalize {}: {error}", path.display()))
}

pub(super) fn env_falsey(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref().map(str::trim),
        Some("0" | "false" | "no" | "off")
    )
}

pub(super) fn v2_bytes(host: &lastdb_node::Host) -> u64 {
    host.db
        .db_ops()
        .namespaced_store()
        .collection_disk_bytes(V2_COLLECTION)
        .unwrap_or(0)
}

pub(super) fn collection_allocated_bytes(host: &lastdb_node::Host, collection: &str) -> u64 {
    host.db
        .db_ops()
        .namespaced_store()
        .collection_disk_usage(collection)
        .map_or(0, |usage| usage.allocated_bytes)
}

pub(super) fn require_growth_budget(
    host: &lastdb_node::Host,
    max_bytes: u64,
    active_edges: u64,
) -> Result<(), String> {
    let bytes = v2_bytes(host);
    let projected =
        lastdb_node::atom_ref_backfill::projected_atom_ref_v2_bytes(bytes, active_edges);
    if bytes > max_bytes || projected > max_bytes {
        Err(format!(
            "compact plane reached {bytes} bytes and projects to {projected}, above the {max_bytes}-byte proof limit"
        ))
    } else {
        Ok(())
    }
}
