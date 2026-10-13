//! Lap audit, reap options and the backup-cut holder of the automatic gc-atoms trigger.

use super::*;

/// Bind one completed reachability generation to exact zero-count epochs.
///
/// A candidate created after the probe began stays unaudited. The trace did
/// not observe that zero epoch, so absence from its marker set proves nothing.
/// The audit carries the id of this lap, and the reaper accepts only that id.
/// One failing audit keeps its candidate unaudited and does not stop the rest.
pub(super) async fn audit_zero_ref_candidates(
    atoms: &AtomStore,
    lap: &AutomaticGcAtomsProbeResult,
    max_candidates: usize,
) -> Result<(), crate::schema::SchemaError> {
    let started_at = chrono::DateTime::parse_from_rfc3339(&lap.started_at)
        .map_err(|error| {
            crate::schema::SchemaError::InvalidData(format!(
                "invalid automatic gc-atoms candidate audit start time: {error}"
            ))
        })?
        .timestamp_nanos_opt()
        .and_then(|nanos| u64::try_from(nanos).ok())
        .ok_or_else(|| {
            crate::schema::SchemaError::InvalidData(
                "automatic gc-atoms candidate audit start time is out of range".to_string(),
            )
        })?;
    let candidates = atoms
        .list_atom_gc_candidates(None, max_candidates)
        .await?
        .candidates
        .into_iter()
        .filter(|candidate| candidate.zero_since_unix_nanos <= started_at)
        .collect::<Vec<_>>();
    let uuids = candidates
        .iter()
        .map(|candidate| candidate.atom_uuid.clone())
        .collect::<Vec<_>>();
    let referenced = atoms
        .automatic_gc_atoms_referenced_candidates(None, lap.generation, &uuids)
        .await?;
    let audit_id = automatic_gc_audit_id(lap.generation);
    for candidate in candidates {
        if let Err(error) = atoms
            .record_atom_gc_audit(
                &candidate.atom_uuid,
                candidate.zero_since_unix_nanos,
                &audit_id,
                referenced.contains(&candidate.atom_uuid),
                None,
            )
            .await
        {
            warn!(
                target: "fold_db::gc_atoms",
                atom = %candidate.atom_uuid,
                %error,
                "automatic gc-atoms audit failed; candidate kept unaudited"
            );
        }
    }
    Ok(())
}

/// Options for the grace-window reaper of one finished lap. Only an audit that
/// carries this lap's id may clear a delete.
pub(super) fn lap_reap_options(
    lap: &AutomaticGcAtomsProbeResult,
    max_candidates: usize,
    now_unix_nanos: u64,
    compact_after_delete: bool,
) -> AtomGcReapOptions {
    AtomGcReapOptions {
        now_unix_nanos,
        grace_window: DEFAULT_ATOM_GC_GRACE_WINDOW,
        max_candidates,
        dry_run: false,
        compact_after_delete,
        required_audit_generation: Some(lap.generation),
    }
}

/// True when a backup cut holds the packing target. The lock closure runs at
/// most once. Its guard stays in `slot`, so a later call reuses it and the
/// guard lives until the caller drops `slot`.
pub(super) async fn cut_is_held<F, Fut, G>(
    slot: &mut Option<Option<G>>,
    lock: &mut Option<F>,
) -> bool
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Option<G>>,
{
    if slot.is_none() {
        let lock = lock.take().expect("the cut lock is taken once");
        *slot = Some(lock().await);
    }
    slot.as_ref().is_some_and(Option::is_none)
}
