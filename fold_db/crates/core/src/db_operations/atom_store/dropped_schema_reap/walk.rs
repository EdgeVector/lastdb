//! Bounded walk helpers for the dropped-schema reaper: physical key pages,
//! cursor validation, and the per-phase steps of one pass.

use super::{
    DroppedSchemaReapPhase, DroppedSchemaReapReport, DroppedSchemaTipProof, ReapCursorState,
    MAX_REAPER_CANDIDATES, MAX_REAPER_CURSOR_BYTES,
};
use crate::atom::molecule_key_codec;
use crate::db_operations::atom_store::helpers::schema_index_codec;
use crate::db_operations::atom_store::AtomStore;
use crate::db_operations::key_fingerprint;
use crate::kind_partition::exact_prefix_bounds;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::traits::{KvStore, PhysicalScanCursor};
use crate::storage::TypedKvStore;
use std::collections::BTreeSet;

/// One page from a physically bounded LastStore walk. The production backend
/// visits at most one shard/group per request. The in-memory default is a
/// correctness-first range page and does not prove physical work.
struct KeyPage {
    keys: Vec<String>,
    next_cursor: Option<PhysicalScanCursor>,
    handles_visited: u64,
    cold_shard_loads: u64,
}

fn validate_position(
    position: &PhysicalScanCursor,
    prefix: &str,
    expected_collection: &str,
) -> Result<(), SchemaError> {
    if position
        .collection
        .as_deref()
        .is_some_and(|collection| collection != expected_collection)
        || position
            .after_key
            .as_deref()
            .is_some_and(|key| !key.starts_with(prefix.as_bytes()))
    {
        return Err(SchemaError::InvalidCursor(
            "reaper physical cursor is outside its collection or key prefix".to_string(),
        ));
    }
    Ok(())
}

async fn key_page(
    store: &TypedKvStore<dyn KvStore>,
    prefix: &str,
    position: Option<&PhysicalScanCursor>,
    expected_collection: &str,
    limit: usize,
) -> Result<KeyPage, SchemaError> {
    if let Some(position) = position {
        validate_position(position, prefix, expected_collection)?;
    }
    let (start, end) = exact_prefix_bounds(prefix);
    let page = store
        .inner()
        .scan_range_physical_paged(start.as_bytes(), end.as_bytes(), position, limit, 1)
        .await
        .map_err(|error| SchemaError::InvalidData(format!("read reaper physical page: {error}")))?;
    if page.handles_visited > 1 || page.rows.len() > limit {
        return Err(SchemaError::InvalidData(
            "reaper backend exceeded its physical page budget".to_string(),
        ));
    }
    if let Some(next) = page.next_cursor.as_ref() {
        validate_position(next, prefix, expected_collection)?;
        if Some(next) == position || page.handles_visited == 0 {
            return Err(SchemaError::InvalidData(
                "reaper physical page did not advance".to_string(),
            ));
        }
    }
    let keys = page
        .rows
        .into_iter()
        .map(|(key, _)| {
            if !key.starts_with(prefix.as_bytes()) {
                return Err(SchemaError::InvalidData(
                    "reaper physical page returned a key outside its prefix".to_string(),
                ));
            }
            String::from_utf8(key)
                .map_err(|error| SchemaError::InvalidData(format!("non-UTF8 reaper key: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(KeyPage {
        keys,
        next_cursor: page.next_cursor,
        handles_visited: page.handles_visited,
        cold_shard_loads: page.cold_shard_loads,
    })
}

pub(super) fn index_prefixes(schema_name: &str) -> [String; 1] {
    [build_storage_key(
        None,
        &schema_index_codec::schema_prefix(schema_name),
    )]
}

/// Build the candidate molecule set and its fingerprint, enforcing the input
/// and budget limits of one pass.
pub(super) fn reap_candidates(
    schema_name: &str,
    field_molecule_uuids: &[String],
    field_names: &[String],
    drop_receipt_proves_removed: bool,
    max_ops: usize,
) -> Result<(BTreeSet<String>, String), SchemaError> {
    if field_molecule_uuids.len() > MAX_REAPER_CANDIDATES
        || field_names.len() > MAX_REAPER_CANDIDATES
    {
        return Err(SchemaError::InvalidField(
            "reaper input has more than 4096 candidate fields or molecules".to_string(),
        ));
    }
    let mut known_molecules: BTreeSet<String> = field_molecule_uuids
        .iter()
        .filter(|molecule| !molecule.is_empty())
        .cloned()
        .collect();
    for field in field_names.iter().filter(|field| !field.is_empty()) {
        let write = crate::atom::deterministic_molecule_uuid(schema_name, field);
        known_molecules.insert(write);
    }
    let has_known_molecules = !known_molecules.is_empty();
    if known_molecules.len() > MAX_REAPER_CANDIDATES {
        return Err(SchemaError::InvalidField(
            "reaper candidate molecule set exceeds 4096".to_string(),
        ));
    }
    let candidate_fingerprint = key_fingerprint(
        schema_name,
        &format!(
            "receipt={}\0{}",
            drop_receipt_proves_removed,
            known_molecules
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join("\0")
        ),
    );
    if has_known_molecules && max_ops < 2 {
        return Err(SchemaError::InvalidField(
            "max_ops must be at least 2 for a tip proof (one molecule check and one tip key)"
                .to_string(),
        ));
    }
    Ok((known_molecules, candidate_fingerprint))
}

/// Reject a cursor that does not match this schema, candidate set, or the
/// physical prefixes it claims to resume.
pub(super) fn validate_cursor_state(
    state: &ReapCursorState,
    schema_name: &str,
    candidate_fingerprint: &str,
    known_molecules: &BTreeSet<String>,
) -> Result<(), SchemaError> {
    let has_known_molecules = !known_molecules.is_empty();
    let cursor_bytes = serde_json::to_vec(state)
        .map_err(|error| SchemaError::InvalidCursor(format!("serialize cursor: {error}")))?;
    if cursor_bytes.len() > MAX_REAPER_CURSOR_BYTES
        || state.schema != schema_name
        || state.candidate_fingerprint != candidate_fingerprint
        || state.index_form > 1
        || (!has_known_molecules && state.phase == DroppedSchemaReapPhase::Tips)
        || (state.phase == DroppedSchemaReapPhase::Tips
            && (state.index_form != 0 || state.index_position.is_some()))
        || (state.phase == DroppedSchemaReapPhase::Index
            && (state.tip_molecule.is_some()
                || state.tip_position.is_some()
                || state.molecule_after.is_some()))
        || (state.index_form == 1 && state.index_position.is_some())
        || state
            .tip_molecule
            .as_deref()
            .is_some_and(|molecule| !known_molecules.contains(molecule))
        || state
            .molecule_after
            .as_deref()
            .is_some_and(|molecule| !known_molecules.contains(molecule))
        || state.tip_position.is_some() && state.tip_molecule.is_none()
        || state.tip_position.as_ref().is_some_and(|position| {
            state.tip_molecule.as_deref().is_none_or(|molecule| {
                validate_position(
                    position,
                    &molecule_key_codec::molecule_record_prefix(molecule),
                    "tips",
                )
                .is_err()
            })
        })
        || state.index_position.as_ref().is_some_and(|position| {
            state.index_form >= 1
                || validate_position(
                    position,
                    &index_prefixes(schema_name)[state.index_form as usize],
                    "schema_index",
                )
                .is_err()
        })
    {
        return Err(SchemaError::InvalidCursor(
            "reaper cursor is too large or does not match this schema and candidate set"
                .to_string(),
        ));
    }
    Ok(())
}

/// Resume after `molecule` with no active tip page.
fn move_past_molecule(state: &mut ReapCursorState, molecule: &str) {
    state.molecule_after = Some(molecule.to_string());
    state.tip_molecule = None;
    state.tip_position = None;
}

impl AtomStore {
    /// Run the Tips phase until the budget ends or every molecule is done.
    /// Returns true when the phase completed and the pass moved to Index.
    pub(super) async fn reap_tips_phase(
        &self,
        schema_name: &str,
        known_molecules: &BTreeSet<String>,
        drop_receipt_proves_removed: bool,
        state: &mut ReapCursorState,
        budget: &mut usize,
        report: &mut DroppedSchemaReapReport,
    ) -> Result<bool, SchemaError> {
        for molecule in known_molecules {
            if let Some(active) = state.tip_molecule.as_deref() {
                if molecule.as_str() < active {
                    continue;
                }
            } else if state
                .molecule_after
                .as_deref()
                .is_some_and(|after| molecule.as_str() <= after)
            {
                continue;
            }
            if *budget == 0 {
                return Ok(false);
            }
            *budget -= 1;
            report.molecules += 1;
            if self
                .reap_tip_molecule(
                    schema_name,
                    molecule,
                    drop_receipt_proves_removed,
                    state,
                    *budget,
                    report,
                )
                .await?
            {
                return Ok(false);
            }
        }
        if *budget == 0 {
            return Ok(false);
        }
        state.phase = DroppedSchemaReapPhase::Index;
        state.molecule_after = None;
        state.tip_molecule = None;
        state.tip_position = None;
        Ok(true)
    }

    /// Check one molecule and page its tip keys. Returns true when the pass
    /// must stop here (a page was taken or the budget ended); false when the
    /// molecule was retained and the walk moves on.
    async fn reap_tip_molecule(
        &self,
        schema_name: &str,
        molecule: &str,
        drop_receipt_proves_removed: bool,
        state: &mut ReapCursorState,
        budget: usize,
        report: &mut DroppedSchemaReapReport,
    ) -> Result<bool, SchemaError> {
        let edges_complete = self.molecule_ref_edges_complete(None).await?;
        if !edges_complete && !drop_receipt_proves_removed {
            report.molecules_retained_incomplete += 1;
            move_past_molecule(state, molecule);
            return Ok(false);
        }
        let has_active_refs = if edges_complete {
            self.has_active_molecule_refs(molecule, None).await?
        } else {
            self.has_any_molecule_ref_edges(molecule, None).await?
        };
        if has_active_refs {
            report.molecules_retained_active_edges += 1;
            move_past_molecule(state, molecule);
            return Ok(false);
        }
        if self.protein_of_molecule(molecule).await?.is_some() {
            report.molecules_retained_protein += 1;
            move_past_molecule(state, molecule);
            return Ok(false);
        }
        if !edges_complete {
            report.molecules_allowed_by_drop_receipt += 1;
        }
        if budget == 0 {
            state.tip_molecule = Some(molecule.to_string());
            state.tip_position = None;
            return Ok(true);
        }
        let prefix = molecule_key_codec::molecule_record_prefix(molecule);
        let page = key_page(
            &self.main_store,
            &prefix,
            if state.tip_molecule.as_deref() == Some(molecule) {
                state.tip_position.as_ref()
            } else {
                None
            },
            "tips",
            budget,
        )
        .await?;
        report.tips += page.keys.len() as u64;
        report.physical_handles_visited += page.handles_visited;
        report.cold_shard_loads += page.cold_shard_loads;
        for key in &page.keys {
            report.tip_proofs.push(DroppedSchemaTipProof {
                molecule_uuid: molecule.to_string(),
                key_fingerprint: key_fingerprint(schema_name, key),
            });
        }
        if let Some(next) = page.next_cursor {
            state.tip_molecule = Some(molecule.to_string());
            state.tip_position = Some(next);
        } else {
            move_past_molecule(state, molecule);
        }
        Ok(true)
    }

    /// Take one Index page. Returns true when every index form is done and
    /// the report is final (no resume cursor).
    pub(super) async fn reap_index_phase(
        &self,
        prefixes: &[String; 1],
        state: &mut ReapCursorState,
        budget: usize,
        report: &mut DroppedSchemaReapReport,
    ) -> Result<bool, SchemaError> {
        if state.index_form >= 1 {
            return Ok(true);
        }
        let prefix = &prefixes[state.index_form as usize];
        let page = key_page(
            &self.schema_index_store,
            prefix,
            state.index_position.as_ref(),
            "schema_index",
            budget,
        )
        .await?;
        report.index_keys += page.keys.len() as u64;
        report.physical_handles_visited += page.handles_visited;
        report.cold_shard_loads += page.cold_shard_loads;
        if let Some(next) = page.next_cursor {
            state.index_position = Some(next);
        } else {
            state.index_form += 1;
            state.index_position = None;
        }
        Ok(state.index_form >= 1)
    }
}
