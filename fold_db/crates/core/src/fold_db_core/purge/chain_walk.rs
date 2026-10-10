//! Tip-version chain walks used by purge: retained-chain collection,
//! per-target chain tracing and the backref reindex that feeds them.

use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::{self, StreamExt};

use crate::atom::AtomEntry;
use crate::db_operations::DbOperations;
use crate::schema::types::field::build_storage_key;
use crate::schema::types::Schema;
use crate::schema::SchemaError;

/// Cap on one slot's tip-version chain walk. Mirrors the bound in
/// [`crate::db_operations::AtomStore::tip_entry_at_as_of`], deliberately
/// including its failure mode: exceeding the cap is an **error**, not a
/// truncated walk.
///
/// A purge that silently stopped halfway would return `Ok` and a report while
/// leaving superseded values stored — the precise shape of the defect this
/// walk exists to close, re-introduced at the tail. Failing loudly leaves the
/// record intact and the operator informed; the alternative leaves neither.
pub(super) const MAX_TIP_CHAIN_WALK: usize = 1_000_000;

/// Walk one slot's `prev_tip_id` → `tv:{id}` chain starting from `head`.
///
/// Appends every chain node's full storage key to `tv_keys` (when supplied)
/// and every atom uuid the chain names to `atom_uuids`. `head`'s own atom is
/// NOT collected — that is the live head, which the caller already accounts
/// for separately.
///
/// A missing node ends the walk without error: legacy depth-1 heads carry an
/// atom id rather than a version id, and GC may have pruned a chain already.
/// Neither is a failure, and both mean there is nothing further back to erase.
/// A key is only ever pushed after its row was found, so the delete batch
/// never names a row that was not there.
pub(crate) async fn walk_tip_chain(
    db_ops: &Arc<DbOperations>,
    head: &AtomEntry,
    storage_prefix: Option<&str>,
    mut tv_keys: Option<&mut Vec<String>>,
    atom_uuids: &mut HashSet<String>,
) -> Result<(), SchemaError> {
    let mut version_id = head.prev_tip_id.clone();
    let mut walked = 0usize;
    while !version_id.is_empty() {
        if walked >= MAX_TIP_CHAIN_WALK {
            return Err(SchemaError::InvalidData(format!(
                "purge: tip-version chain exceeded {MAX_TIP_CHAIN_WALK} nodes — refusing to \
                 partially erase a record (compliance verb)"
            )));
        }
        walked += 1;
        let Some(entry) = db_ops
            .atoms()
            .get_tip_version(&version_id, storage_prefix)
            .await?
        else {
            break;
        };
        if let Some(keys) = tv_keys.as_deref_mut() {
            keys.push(build_storage_key(
                storage_prefix,
                &crate::atom::molecule_key_codec::tip_version_key(&version_id),
            ));
        }
        atom_uuids.insert(entry.atom_uuid.clone());
        version_id = entry.prev_tip_id;
    }
    Ok(())
}

/// Candidate atoms that an archived tip version of a retained slot still
/// references.
///
/// The reverse-reference index turns the old complement walk (every retained
/// slot in every field) into keyed reads for the atoms purge may delete. Each
/// storage prefix has its own completeness marker, so absence is authoritative
/// only after every prefix represented by this schema reports complete.
///
/// A partial index is **never** treated as proof that an atom is unreferenced —
/// that path is still fail-closed. Production callers that need to decide under
/// an unfinished reindex must use
/// [`collect_retained_chain_atoms_for_storage_slots`], which heals/falls back
/// rather than hard-refusing soft-delete.
pub(crate) async fn collect_retained_candidate_atoms_from_backrefs(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    candidate_atoms: &HashSet<String>,
    target_slots: &HashSet<(String, String, String)>,
) -> Result<HashSet<String>, SchemaError> {
    if candidate_atoms.is_empty() {
        return Ok(HashSet::new());
    }

    let storage_prefixes = schema_storage_prefixes(schema);
    let mut retained = HashSet::new();

    for atom_uuid in candidate_atoms {
        for storage_prefix in &storage_prefixes {
            let lookup = db_ops
                .atoms()
                .tip_version_backrefs_for_atom(atom_uuid, storage_prefix.as_deref())
                .await?;
            if !lookup.is_complete() {
                let scope = storage_prefix.as_deref().unwrap_or("<default>");
                return Err(SchemaError::InvalidData(format!(
                    "purge: tip-version reverse-reference index is incomplete for storage prefix \
                     {scope:?}; refusing to decide whether candidate atom {atom_uuid:?} is retained"
                )));
            }
            if lookup.references().iter().any(|reference| {
                !target_slots.contains(&(
                    reference.molecule_uuid.clone(),
                    reference.hash.clone(),
                    reference.range.clone(),
                ))
            }) {
                retained.insert(atom_uuid.clone());
                break;
            }
        }
    }
    Ok(retained)
}

/// Storage prefixes that participate in this schema's molecule plane.
pub(crate) fn schema_storage_prefixes(schema: &Schema) -> HashSet<Option<String>> {
    schema
        .runtime_fields
        .values()
        .filter(|field| field.common().molecule_uuid().is_some())
        .map(|field| field.common().storage_prefix().map(str::to_string))
        .collect()
}

/// Whether a purge error is the fail-closed incomplete reverse-ref guard.
pub(crate) fn is_incomplete_tip_version_backref_error(err: &SchemaError) -> bool {
    err.to_string()
        .contains("reverse-reference index is incomplete")
}

/// Whether a heal-page error is a recoverable reverse-ref rebuild fault.
///
/// Missing `tv:` mid-chain used to hard-fail reindex and bubble out of the
/// soft-delete path as HTTP 400. Reindex now tolerates that case; this helper
/// keeps the purge heal path fail-open to the complement walk if an older
/// binary (or a residual hard error) still surfaces it.
pub(crate) fn is_recoverable_tip_version_backref_heal_error(err: &SchemaError) -> bool {
    let msg = err.to_string();
    msg.contains("reverse-reference index is incomplete")
        || msg.contains("missing tv:")
        || msg.contains("while rebuilding reverse references")
}

/// Bound on synchronous heal pages attempted before falling back to the
/// complement walk. Each page walks up to 256 `mk:` slots; 32 pages ≈ 8k slots
/// — enough to finish empty/small prefixes and nudge mid-reindex primaries
/// without turning every soft-delete into a full historical rebuild.
const SYNC_BACKREF_REINDEX_PAGE_BUDGET: usize = 32;

/// Reachability guard for purge: prefer the reverse-ref index when complete;
/// when a storage prefix is still incomplete, advance the durable reindex a
/// bounded number of pages and retry; if still incomplete, fall back to the
/// pre-backref complement walk over already-loaded molecules.
///
/// Soft-delete / `fkanban rm` must not hard-fail with HTTP 400 while the
/// background reindex for storage prefix `"<default>"` is unfinished. Treating
/// a partial index as complete would be unsafe; falling back to the complement
/// walk is the historical correct decide path.
pub(crate) async fn collect_retained_chain_atoms_for_storage_slots(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    candidate_atoms: &HashSet<String>,
    target_slots: &HashSet<(String, String, String)>,
) -> Result<HashSet<String>, SchemaError> {
    if candidate_atoms.is_empty() {
        return Ok(HashSet::new());
    }

    match collect_retained_candidate_atoms_from_backrefs(
        db_ops,
        schema,
        candidate_atoms,
        target_slots,
    )
    .await
    {
        Ok(retained) => return Ok(retained),
        Err(err) if is_incomplete_tip_version_backref_error(&err) => {
            // Heal: advance incomplete prefixes, then retry the keyed path.
            // Recoverable reindex faults (missing tv mid-chain, still-incomplete)
            // fall through to the complement walk instead of hard-failing rm.
            match advance_incomplete_tip_version_backref_reindex(db_ops, schema).await {
                Ok(()) => {}
                Err(heal_err) if is_recoverable_tip_version_backref_heal_error(&heal_err) => {
                    // Fall through to complement below.
                    return collect_retained_chain_atoms_with_concurrency(
                        db_ops,
                        schema,
                        target_slots,
                        retained_chain_walk_concurrency(),
                    )
                    .await;
                }
                Err(heal_err) => return Err(heal_err),
            }
            match collect_retained_candidate_atoms_from_backrefs(
                db_ops,
                schema,
                candidate_atoms,
                target_slots,
            )
            .await
            {
                Ok(retained) => return Ok(retained),
                Err(err) if is_incomplete_tip_version_backref_error(&err) => {
                    // Still incomplete after bounded heal — decide via complement.
                }
                Err(err) => return Err(err),
            }
        }
        Err(err) => return Err(err),
    }

    collect_retained_chain_atoms_with_concurrency(
        db_ops,
        schema,
        target_slots,
        retained_chain_walk_concurrency(),
    )
    .await
}

/// Advance the tip-version reverse-ref reindex for each schema storage prefix
/// that is not yet complete, up to [`SYNC_BACKREF_REINDEX_PAGE_BUDGET`] pages
/// total across all prefixes. Best-effort heal for the soft-delete path.
async fn advance_incomplete_tip_version_backref_reindex(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
) -> Result<(), SchemaError> {
    let mut pages_left = SYNC_BACKREF_REINDEX_PAGE_BUDGET;
    for storage_prefix in schema_storage_prefixes(schema) {
        if pages_left == 0 {
            break;
        }
        let prefix = storage_prefix.as_deref();
        // Probe completeness without scanning candidate atoms: empty atom uuid
        // prefix still consults the durable complete marker.
        let status = db_ops
            .atoms()
            .tip_version_backref_reindex_status(prefix)
            .await?;
        if status.completed {
            continue;
        }
        while pages_left > 0 {
            let report = db_ops
                .atoms()
                .reindex_tip_version_backrefs(prefix, Some(256))
                .await?;
            pages_left = pages_left.saturating_sub(1);
            if report.completed {
                break;
            }
            // Zero slots walked with not-completed usually means the page
            // advanced past residual noise; still count the budget and stop
            // spinning if the cursor is stuck.
            if report.slots_walked == 0 && !report.completed {
                break;
            }
        }
    }
    Ok(())
}

/// How many retained-record chain walks the complement fallback keeps in flight.
///
/// Default 64 matches the pre-backref production path. Override with
/// `LASTDB_PURGE_CHAIN_WALK_CONCURRENCY` (values that do not parse, or 0, fall
/// back to the default rather than deadlocking on a zero-width stream).
fn retained_chain_walk_concurrency() -> usize {
    parse_chain_walk_concurrency(
        std::env::var("LASTDB_PURGE_CHAIN_WALK_CONCURRENCY")
            .ok()
            .as_deref(),
    )
}

/// The complement walk with the fan-out passed in directly.
///
/// Production uses this as the fallback while reverse-ref reindex is unfinished.
/// Tests also drive `concurrency = 1` vs the default through the same code
/// without mutating process env — which is global, racy under the test harness,
/// and `unsafe` in edition 2024.
pub(crate) async fn collect_retained_chain_atoms_with_concurrency(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
    target_slots: &HashSet<(String, String, String)>,
    concurrency: usize,
) -> Result<HashSet<String>, SchemaError> {
    let mut out: HashSet<String> = HashSet::new();
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        let prefix = field.common().storage_prefix().map(str::to_string);
        let Some(molecule) = field.molecule_data() else {
            continue;
        };

        // Split the field's records into the heads (free, in memory) and the
        // chain walks (one `tv:` read per node, the whole cost of this guard).
        // Heads go straight into `out`; only the walks are scheduled.
        let mut retained_heads = Vec::new();
        for (hash, range, entry, _meta) in molecule.per_key_records() {
            if target_slots.contains(&(molecule_uuid.clone(), hash.clone(), range.clone())) {
                continue;
            }
            retained_heads.push(entry);
        }

        // A retained key's live head is already covered by
        // `collect_live_atom_uuids`, but only while the molecule is loaded;
        // adding it here costs nothing and makes this set self-contained.
        // Taken before the walks so each entry can be MOVED into its future.
        out.extend(retained_heads.iter().map(|entry| entry.atom_uuid.clone()));

        // One walk per retained record, run with bounded concurrency. Chains
        // are sequential WITHIN a record (each node names its predecessor) but
        // records are independent of each other, so the walks overlap without
        // changing what any one of them reads. Collected per record and merged
        // — a shared `&mut HashSet` is what forced this to be sequential.
        //
        // Every future owns its inputs (entry moved, `Arc` and prefix cloned)
        // rather than borrowing from this frame. That is not style: a future
        // that borrows `entry` from the iterator is higher-ranked over that
        // lifetime, and `Send` for a higher-ranked future is NOT implied by
        // `Send` at each concrete lifetime. Borrowing here compiles fine in
        // this module and then fails as "`Send` is not general enough" at
        // distant `tokio::spawn` sites that merely await the write path —
        // `purge_concurrency_test` and `per_key_molecule_wire_test` both broke
        // that way. The clones are one `String` and one `Arc` bump per record.
        {
            let walks = stream::iter(retained_heads.into_iter().map(|entry| {
                let db_ops = Arc::clone(db_ops);
                let prefix = prefix.clone();
                async move {
                    let mut chain: HashSet<String> = HashSet::new();
                    walk_tip_chain(&db_ops, &entry, prefix.as_deref(), None, &mut chain).await?;
                    Ok::<_, SchemaError>(chain)
                }
            }))
            .buffer_unordered(concurrency);
            futures::pin_mut!(walks);
            while let Some(chain) = walks.next().await {
                out.extend(chain?);
            }
        }
    }
    Ok(out)
}

/// Parse the complement walk's concurrency setting.
///
/// A `0` must NOT be honoured: `buffer_unordered(0)` yields nothing and would
/// silently return an EMPTY retained set — a guard that spares nothing, which
/// is the one failure direction that deletes a live atom. Treating it as
/// "unset" fails safe.
pub(crate) fn parse_chain_walk_concurrency(raw: Option<&str>) -> usize {
    const DEFAULT: usize = 64;
    raw.and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT)
}
