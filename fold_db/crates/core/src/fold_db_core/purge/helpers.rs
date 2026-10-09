//! Pure helpers for purge planning and key matching.

use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::{self, StreamExt};

use crate::atom::{AtomEntry, FieldKey, MutationEvent};
use crate::db_operations::DbOperations;
use crate::schema::types::field::{build_storage_key, FieldVariant};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::schema::DeclarativeSchemaType;
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
const MAX_TIP_CHAIN_WALK: usize = 1_000_000;

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
    crate::test_helpers::wait_purge_walk_stall().await;
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

/// The purge target's own tip-version chain for one field: the `tv:` rows to
/// delete and the superseded atoms they name.
///
/// Returns `(tv_keys, atom_uuids)`. Empty for a field with no molecule, no
/// slot matching the key's shape, or a depth-1 head — the common case.
///
/// # This is the per-record half of the batch, so it must be O(1) in `R`
///
/// [`super::purge_records_bulk`] calls this once per `(field, key)`. Everything
/// else it does per key is a hashmap lookup ([`current_atom_for_key`],
/// [`remove_key_from_field`]), so this function alone decides whether a batch of
/// `N` costs `O(F·N)` or `O(F·N·R)`.
///
/// It used to call `per_key_records()` and linearly filter the result for one
/// slot. That helper materialises a `Vec` of the WHOLE molecule, cloning every
/// hash, range and [`AtomEntry`] — so finding one slot cost `R` clones, and the
/// batch that had just been de-quadratified in `N` was still quadratic through
/// `R`. Measured on the merged batching change: 200/800/1600 records took
/// 119/755/2841 ms — 8× the records for 23.9× the time, the residual `N^1.5`
/// that made a naive extrapolation to the 74,105-row telemetry plane ~15 min
/// for a single drain.
///
/// `atom_uuids` is `HashMap<hash, BTreeMap<range, AtomEntry>>`, so a
/// `(hash, range)` slot is unique by construction: the old loop could match at
/// most one record and [`get_atom_entry`](crate::atom::MoleculeHashRange::get_atom_entry)
/// returns exactly that one. Same answer, `O(1)` instead of `O(R)`.
///
/// The entry is cloned rather than borrowed only so no borrow of the molecule
/// is held across the `walk_tip_chain` await — one clone, where the scan did
/// `R` of them.
///
/// Pinned by `collect_target_chain_does_not_scan_the_molecule`, which asserts
/// the scan helper is not called at all rather than merely checking the answer
/// is right — the scan returned the right answer too.
pub(crate) struct TargetChainTrace {
    pub tip_version_keys: Vec<String>,
    pub atom_uuids: HashSet<String>,
    pub atom_ref_edge_keys: Vec<String>,
    pub atom_ref_v2_edge_keys: Vec<String>,
}

pub(crate) async fn collect_target_chain(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
) -> Result<TargetChainTrace, SchemaError> {
    collect_target_chain_with_mode(db_ops, field, key, ChainTraceMode::AtomErasure).await
}

/// Live Delete needs the slot's source keys and exact reverse-edge keys.
/// It does not plan atom candidates because the later reclaim janitor owns
/// body deletion. The converge batch removes each source before its edges.
pub(crate) async fn collect_target_tip_chain(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
) -> Result<TargetChainTrace, SchemaError> {
    collect_target_chain_with_mode(db_ops, field, key, ChainTraceMode::TipVersionsOnly).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChainTraceMode {
    AtomErasure,
    TipVersionsOnly,
}

async fn collect_target_chain_with_mode(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    key: &KeyValue,
    mode: ChainTraceMode,
) -> Result<TargetChainTrace, SchemaError> {
    let mut tv_keys: Vec<String> = Vec::new();
    let mut atom_uuids: HashSet<String> = HashSet::new();
    let mut atom_ref_edge_keys = Vec::new();
    let mut atom_ref_v2_edge_keys = Vec::new();
    let Some((target_hash, target_range)) = field.disk_slot_for_key(key) else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    let prefix = field.common().storage_prefix().map(str::to_string);
    let Some(molecule_uuid) = field.common().molecule_uuid() else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    let Some(molecule) = field.molecule_data() else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    let Some(entry) = molecule
        .get_atom_entry(&target_hash, &target_range)
        .cloned()
    else {
        return Ok(TargetChainTrace {
            tip_version_keys: tv_keys,
            atom_uuids,
            atom_ref_edge_keys,
            atom_ref_v2_edge_keys,
        });
    };
    atom_ref_edge_keys.push(db_ops.atoms().atom_ref_tip_edge_key(
        molecule_uuid,
        &target_hash,
        &target_range,
        &entry,
        prefix.as_deref(),
    ));
    if let Ok(v2_key) = db_ops.atoms().atom_ref_tip_edge_v2_key(
        molecule_uuid,
        &target_hash,
        &target_range,
        &entry,
        prefix.as_deref(),
    ) {
        atom_ref_v2_edge_keys.push(v2_key);
    }

    let mut version_id = entry.prev_tip_id.clone();
    let mut walked = 0usize;
    while !version_id.is_empty() {
        if walked >= MAX_TIP_CHAIN_WALK {
            return Err(SchemaError::InvalidData(format!(
                "purge: tip-version chain exceeded {MAX_TIP_CHAIN_WALK} nodes — refusing to \
                 partially erase a record (compliance verb)"
            )));
        }
        walked += 1;
        let Some(archived) = db_ops
            .atoms()
            .get_tip_version(&version_id, prefix.as_deref())
            .await?
        else {
            break;
        };
        tv_keys.push(build_storage_key(
            prefix.as_deref(),
            &crate::atom::molecule_key_codec::tip_version_key(&version_id),
        ));
        if mode == ChainTraceMode::AtomErasure {
            atom_uuids.insert(archived.atom_uuid.clone());
        }
        atom_ref_edge_keys.push(db_ops.atoms().atom_ref_tip_version_edge_key(
            molecule_uuid,
            &target_hash,
            &target_range,
            &version_id,
            &archived,
            prefix.as_deref(),
        ));
        if let Ok(v2_key) = db_ops.atoms().atom_ref_tip_version_edge_v2_key(
            molecule_uuid,
            &target_hash,
            &target_range,
            &version_id,
            &archived,
            prefix.as_deref(),
        ) {
            atom_ref_v2_edge_keys.push(v2_key);
        }
        version_id = archived.prev_tip_id;
    }
    Ok(TargetChainTrace {
        tip_version_keys: tv_keys,
        atom_uuids,
        atom_ref_edge_keys,
        atom_ref_v2_edge_keys,
    })
}

/// Translate an API-form (plaintext) [`KeyValue`] into the **storage form** the
/// loaded molecule is actually keyed by.
///
/// Purge hydrates its molecules through the full-load path
/// (`refresh_runtime_field_molecules` → `refresh_from_db` →
/// `load_all_mk_records`), which reassembles each slot by decoding the
/// `mk:{M}:{esc(hash)}\0{range}` key it was stored under. That key segment is
/// storage form: HMAC-blinded under `HashKeyEncoding::BlindV1` and OPE-encoded
/// under `RangeKeyEncoding::OpeV1`, which are the product defaults for every
/// node booted with a recovery phrase. The `keys` a purge is given arrive in
/// API form, because that is what the query path hands back.
///
/// So every molecule lookup in this module has to cross that boundary, and
/// none of them did — `hash='lastdbd-self'` was compared against a stored
/// `hash='3CrWWLdb5nvISTA1_2Vf2Q'`, nothing ever matched, and the compliance
/// verb refused every batch on a real node. Unencrypted homes are the only
/// place the two forms coincide, and that is the only configuration the rest
/// of the purge suite builds.
///
/// Both encodings are **molecule-uuid-domain-separated**, so this is per
/// field, not per schema: one API hash maps to a different storage segment in
/// every field of the same record. Pinned next door by
/// `one_api_hash_blinds_differently_per_field`.
///
/// A field with no molecule uuid has never been written and has no slot to
/// address; its key passes through unchanged rather than erroring, matching how
/// every other helper here treats that field (skip, don't fail).
pub(crate) fn storage_form_key(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    kv: &KeyValue,
) -> Result<KeyValue, SchemaError> {
    let Some(mol_uuid) = field.common().molecule_uuid() else {
        return Ok(kv.clone());
    };
    let hash = match kv.hash.as_deref() {
        Some(h) => Some(db_ops.atoms().storage_hash(mol_uuid, h)?),
        None => None,
    };
    let range = match kv.range.as_deref() {
        Some(r) => Some(db_ops.atoms().storage_range(mol_uuid, r)?),
        None => None,
    };
    Ok(KeyValue::new(hash, range))
}

/// [`storage_form_key`] for a whole batch, positionally aligned with `keys` so
/// a caller can keep using the batch index it already has for the API-form key
/// (error text, the delete-ledger fingerprint) while addressing storage with
/// the translated one.
pub(crate) fn storage_form_keys(
    db_ops: &Arc<DbOperations>,
    field: &FieldVariant,
    keys: &[KeyValue],
) -> Result<Vec<KeyValue>, SchemaError> {
    keys.iter()
        .map(|k| storage_form_key(db_ops, field, k))
        .collect()
}

/// Same shape-check the mutation pipeline applies to Delete in
/// `mutation_manager::prepare_atoms_and_key_values`. Failing here means
/// the caller submitted a `Hash` schema purge with no hash, etc.
pub(crate) fn validate_purge_key_shape(
    schema: &Schema,
    schema_name: &str,
    key: &KeyValue,
) -> Result<(), SchemaError> {
    match &schema.schema_type {
        DeclarativeSchemaType::Hash if key.hash.is_none() => Err(SchemaError::InvalidData(
            format!("Hash schema '{schema_name}' purge has no hash key"),
        )),
        DeclarativeSchemaType::Range if key.range.is_none() => Err(SchemaError::InvalidData(
            format!("Range schema '{schema_name}' purge has no range key"),
        )),
        DeclarativeSchemaType::HashRange if key.hash.is_none() || key.range.is_none() => {
            Err(SchemaError::InvalidData(format!(
                "HashRange schema '{}' purge requires both hash and range keys, got hash={:?} range={:?}",
                schema_name, key.hash, key.range,
            )))
        }
        _ => Ok(()),
    }
}

/// Pull every field's molecule into memory if it isn't there yet. Reuses
/// the same `refresh_from_db` plumbing that the normal mutation path
/// runs in `restore_missing_molecules`.
///
/// Field loads are independent (distinct molecule UUIDs / storage prefixes).
/// Issue them concurrently: under the exclusive purge barrier a 24-field
/// schema used to pay 24 sequential full-molecule cold loads before any
/// slot was removed. The end state is unchanged — every empty field with a
/// molecule UUID is hydrated, or left empty when the store has no rows.
pub(crate) async fn refresh_runtime_field_molecules(
    db_ops: &Arc<DbOperations>,
    schema: &mut Schema,
) -> Result<(), SchemaError> {
    let plans: Vec<(String, String, Option<String>)> = schema
        .runtime_fields
        .iter()
        .filter(|(_, f)| f.common().molecule_uuid().is_some() && !f.has_molecule())
        .filter_map(|(n, f)| {
            let uuid = f.common().molecule_uuid()?.clone();
            let prefix = f.common().storage_prefix().map(str::to_string);
            Some((n.clone(), uuid, prefix))
        })
        .collect();
    if plans.is_empty() {
        return Ok(());
    }

    let loads = futures::future::try_join_all(plans.iter().map(|(name, uuid, prefix)| {
        let db_ops = Arc::clone(db_ops);
        let name = name.clone();
        let uuid = uuid.clone();
        let prefix = prefix.clone();
        async move {
            let data = db_ops
                .atoms()
                .load_molecule_per_key(&uuid, prefix.as_deref())
                .await?;
            Ok::<_, SchemaError>((name, data))
        }
    }))
    .await?;

    for (name, data) in loads {
        let Some(data) = data else {
            continue;
        };
        if let Some(field) = schema.runtime_fields.get_mut(&name) {
            // Same normalization/kind checks as `refresh_from_db`.
            let data = data.retyped_to_slot(field.kind.retype_slot());
            field.set_molecule_data(data)?;
        }
    }
    Ok(())
}

/// Load only the exact durable slots addressed by one purge batch.
///
/// The reverse-edge path does not need the complement of the target set. It
/// must therefore not materialize every `mk:` row in each field molecule.
pub(crate) async fn refresh_runtime_field_molecules_for_purge_keys(
    db_ops: &Arc<DbOperations>,
    schema: &mut Schema,
    keys: &[KeyValue],
) -> Result<(), SchemaError> {
    let mut plans = Vec::new();
    for (field_name, field) in &schema.runtime_fields {
        let Some(molecule_uuid) = field.common().molecule_uuid().cloned() else {
            continue;
        };
        let storage_keys = storage_form_keys(db_ops, field, keys)?;
        let slots: Vec<_> = storage_keys
            .iter()
            .filter_map(|key| field.disk_slot_for_key(key))
            .collect();
        plans.push((
            field_name.clone(),
            molecule_uuid,
            field.common().storage_prefix().map(str::to_string),
            slots,
        ));
    }

    let loads = futures::future::try_join_all(plans.iter().map(
        |(field_name, molecule_uuid, storage_prefix, slots)| {
            let db_ops = Arc::clone(db_ops);
            let field_name = field_name.clone();
            let molecule_uuid = molecule_uuid.clone();
            let storage_prefix = storage_prefix.clone();
            let slots = slots.clone();
            async move {
                let data = db_ops
                    .atoms()
                    .load_molecule_for_storage_slot_purge(
                        &molecule_uuid,
                        storage_prefix.as_deref(),
                        &slots,
                    )
                    .await?;
                Ok::<_, SchemaError>((field_name, data))
            }
        },
    ))
    .await?;

    for (field_name, data) in loads {
        let Some(field) = schema.runtime_fields.get_mut(&field_name) else {
            continue;
        };
        field.clear_molecule();
        if let Some(data) = data {
            field.set_molecule_data(data.retyped_to_slot(field.kind.retype_slot()))?;
        }
    }
    Ok(())
}

/// True when this schema can use the exact reverse-edge purge path.
///
/// Every molecule that can reference a same-schema content-addressed atom must
/// have a replay-complete manifest that includes mutation-history edges.
/// A v1 tip-only manifest advances one bounded, molecule-keyed history page.
/// Incomplete work keeps the guarded path for this purge and resumes later.
pub(crate) async fn atom_ref_cutover_ready(
    db_ops: &Arc<DbOperations>,
    schema: &Schema,
) -> Result<bool, SchemaError> {
    if !barrierless_purge_enabled_from(std::env::var("LASTDB_BARRIERLESS_PURGE").ok().as_deref()) {
        return Ok(false);
    }

    let mut saw_molecule = false;
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        saw_molecule = true;
        let prefix = field.common().storage_prefix();
        let Some(manifest) = db_ops
            .atoms()
            .atom_ref_molecule_manifest(molecule_uuid, prefix)
            .await?
        else {
            return Ok(false);
        };
        match (manifest.version, manifest.replay_complete) {
            (crate::db_operations::atom_store::ATOM_REF_MANIFEST_VERSION_HISTORY, true) => {}
            (crate::db_operations::atom_store::ATOM_REF_MANIFEST_VERSION_TIPS, true) => {
                let upgrade = db_ops
                    .atoms()
                    .upgrade_atom_ref_molecule_history_page(molecule_uuid, prefix, 256)
                    .await?;
                if !upgrade.complete {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(saw_molecule)
}

fn barrierless_purge_enabled_from(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("0" | "false" | "off" | "no")
    )
}

/// Insert every atom UUID a history event names (the new head and, when
/// present, the prior head it superseded) into `sink`. Centralised so the
/// purge target's candidate-atom gather and the sibling-key history-row
/// preservation guard agree on what counts as a referenced atom — drift
/// between the two would silently re-open the same content-addressed
/// purge bug they collectively close.
pub(crate) fn collect_event_atom_uuids(event: &MutationEvent, sink: &mut HashSet<String>) {
    sink.insert(event.new_atom_uuid.clone());
    if let Some(old) = &event.old_atom_uuid {
        sink.insert(old.clone());
    }
}

/// True when a stored history event refers to the same per-key slot the
/// caller is purging. `FieldKey::single()` always matches because a Single
/// field has exactly one slot per schema.
pub(crate) fn field_key_matches_value(field_key: &FieldKey, kv: &KeyValue) -> bool {
    // Single always matches (one slot per schema). Keyed slots compare
    // against the KeyValue shape of the purge target.
    field_key.is_single() || field_key.matches_key_value(kv)
}

/// The atom UUID currently at the molecule head for the field's slot
/// identified by `kv`. `None` when no entry exists or the variant
/// doesn't carry a key shape compatible with `kv`.
pub(crate) fn current_atom_for_key(field: &FieldVariant, kv: &KeyValue) -> Option<String> {
    field.current_atom_uuid(kv)
}

/// Remove the entry for `kv` from the field's molecule. Returns `true`
/// when an entry was actually dropped (so the caller knows to persist
/// the molecule). For `Single` variants, the entire molecule is taken
/// — a Single schema has exactly one "record" per schema, so purging
/// it means nuking the field's value entirely.
pub(crate) fn remove_key_from_field(field: &mut FieldVariant, kv: &KeyValue) -> bool {
    field.remove_key(kv)
}

/// Walk every still-loaded molecule across every field of the schema
/// and collect the atom UUIDs they reference. Used to decide which of
/// the purge target's atoms are safe to hard-delete: an atom still in
/// this set is referenced by another key in the same schema (atoms are
/// content-addressed by `(schema, content)`, so this can happen for
/// byte-identical writes) and must NOT be deleted, or the still-live
/// record would dangle.
pub(crate) fn collect_live_atom_uuids(schema: &Schema) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    for field in schema.runtime_fields.values() {
        field.collect_atom_uuids(&mut out);
    }
    out
}

/// True when AtomRefEdges must keep `uuid` because T0 already acked it.
/// Disk reverse edges do not see that write until its persist envelope runs.
pub(crate) fn atom_ref_edges_keep_resident_live(
    uuid: &str,
    resident_live: &HashSet<String>,
) -> bool {
    resident_live.contains(uuid)
}

/// Load an atom for hard-erase accounting. A storage error must fail the
/// verb: skipping it still leaves `uuid` in `atoms_to_delete`.
pub(crate) fn atom_load_for_hard_erase(
    loaded: Result<Option<crate::atom::Atom>, SchemaError>,
    uuid: &str,
) -> Result<Option<crate::atom::Atom>, SchemaError> {
    match loaded {
        Ok(atom) => Ok(atom),
        Err(error) => Err(SchemaError::InvalidData(format!(
            "Failed to load atom {uuid} before hard-delete: {error}"
        ))),
    }
}

/// Resident tips for this schema's molecules. A concurrent write can ack
/// a new live head before its persist envelope runs; disk molecules then
/// lag T0. Union this set into the preserve list so a purge commit that
/// walked before that write does not delete the new atom.
pub(crate) fn collect_resident_live_atom_uuids(
    db_ops: &DbOperations,
    schema: &Schema,
) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    let graph = db_ops.resident();
    for field in schema.runtime_fields.values() {
        let Some(molecule_uuid) = field.common().molecule_uuid() else {
            continue;
        };
        for tip in graph.tips_for_molecule(molecule_uuid) {
            if !tip.atom_uuid.is_empty() {
                out.insert(tip.atom_uuid);
            }
        }
    }
    out
}

/// Human-readable rendering of a KeyValue for log/error messages.
pub(crate) fn describe_key(kv: &KeyValue) -> String {
    match (&kv.hash, &kv.range) {
        (Some(h), Some(r)) => format!("hash='{h}' range='{r}'"),
        (Some(h), None) => format!("hash='{h}'"),
        (None, Some(r)) => format!("range='{r}'"),
        (None, None) => "<single>".to_string(),
    }
}

// `FieldPurgePlan` lived here: a per-field materialised plan that only made
// sense while planning was scoped to one record. The batched path accumulates
// candidates, `history:` keys and `tv:` keys across the whole batch instead, so
// the per-field struct had no remaining reader. Removed rather than left
// `#[allow(dead_code)]` — an unused plan type in the store's most destructive
// module reads as a surface someone still maintains.
