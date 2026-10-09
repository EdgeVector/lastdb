//! Bounded inspection of tips for a dropped schema identity.
//!
//! A drop receipt or explicit field names supply the molecule set. Without
//! either, the inspector can page `schemaidx:` markers but cannot assert which
//! molecule tips belong to them. It never scans `atom:`.

use super::helpers::schema_index_codec;
use super::AtomStore;
use crate::atom::molecule_key_codec;
use crate::db_operations::key_fingerprint;
use crate::kind_partition::{exact_prefix_bounds, form_twin};
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use crate::storage::traits::{KvStore, PhysicalScanCursor};
use crate::storage::TypedKvStore;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::OnceLock;

pub const MAX_DROPPED_SCHEMA_REAP_OPS: usize = 4096;
const MAX_REAPER_CURSOR_BYTES: usize = 16 * 1024;
const MAX_REAPER_CANDIDATES: usize = 4096;
const REAPER_CURSOR_AAD: &[u8] = b"lastdb-dropped-schema-reap-v1";
static REAPER_CURSOR_KEY: OnceLock<[u8; 32]> = OnceLock::new();

fn cursor_key() -> &'static [u8; 32] {
    REAPER_CURSOR_KEY.get_or_init(rand::random)
}

fn seal_cursor_with_key(
    state: &ReapCursorState,
    key: &[u8; 32],
) -> Result<DroppedSchemaReapCursor, SchemaError> {
    let payload = serde_json::to_vec(state)
        .map_err(|error| SchemaError::InvalidCursor(format!("serialize cursor: {error}")))?;
    if payload.len() > MAX_REAPER_CURSOR_BYTES {
        return Err(SchemaError::InvalidCursor(
            "reaper cursor state exceeds the 16 KiB limit".to_string(),
        ));
    }
    let nonce: [u8; 12] = rand::random();
    let cipher = Aes256Gcm::new_from_slice(key).expect("AES-256 accepts 32 bytes");
    let encrypted = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &payload,
                aad: REAPER_CURSOR_AAD,
            },
        )
        .map_err(|_| SchemaError::InvalidCursor("encrypt reaper cursor failed".to_string()))?;
    let mut bytes = Vec::with_capacity(1 + nonce.len() + encrypted.len());
    bytes.push(1);
    bytes.extend_from_slice(&nonce);
    bytes.extend_from_slice(&encrypted);
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    if token.len() > MAX_REAPER_CURSOR_BYTES {
        return Err(SchemaError::InvalidCursor(
            "reaper cursor token exceeds the 16 KiB limit".to_string(),
        ));
    }
    Ok(DroppedSchemaReapCursor(token))
}

fn unseal_cursor_with_key(
    cursor: &DroppedSchemaReapCursor,
    key: &[u8; 32],
) -> Result<ReapCursorState, SchemaError> {
    if cursor.0.len() > MAX_REAPER_CURSOR_BYTES {
        return Err(SchemaError::InvalidCursor(
            "reaper cursor token exceeds the 16 KiB limit".to_string(),
        ));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&cursor.0)
        .map_err(|_| SchemaError::InvalidCursor("invalid reaper cursor token".to_string()))?;
    if bytes.len() < 1 + 12 + 16 || bytes[0] != 1 {
        return Err(SchemaError::InvalidCursor(
            "invalid reaper cursor version or length".to_string(),
        ));
    }
    let cipher = Aes256Gcm::new_from_slice(key).expect("AES-256 accepts 32 bytes");
    let payload = cipher
        .decrypt(
            Nonce::from_slice(&bytes[1..13]),
            Payload {
                msg: &bytes[13..],
                aad: REAPER_CURSOR_AAD,
            },
        )
        .map_err(|_| SchemaError::InvalidCursor("invalid reaper cursor proof".to_string()))?;
    serde_json::from_slice(&payload)
        .map_err(|error| SchemaError::InvalidCursor(format!("decode cursor state: {error}")))
}

fn seal_cursor(state: &ReapCursorState) -> Result<DroppedSchemaReapCursor, SchemaError> {
    seal_cursor_with_key(state, cursor_key())
}

fn unseal_cursor(cursor: &DroppedSchemaReapCursor) -> Result<ReapCursorState, SchemaError> {
    unseal_cursor_with_key(cursor, cursor_key())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DroppedSchemaReapPhase {
    Tips,
    Index,
}

/// Opaque resume token for one bounded pass. A daemon restart changes its
/// process key and invalidates the token. The caller then starts at page one.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct DroppedSchemaReapCursor(String);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReapCursorState {
    schema: String,
    candidate_fingerprint: String,
    phase: DroppedSchemaReapPhase,
    index_form: u8,
    #[serde(default)]
    index_position: Option<PhysicalScanCursor>,
    #[serde(default)]
    molecule_after: Option<String>,
    #[serde(default)]
    tip_molecule: Option<String>,
    #[serde(default)]
    tip_position: Option<PhysicalScanCursor>,
}

/// A content-free proof that an exact `mk:` key was present in this page.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DroppedSchemaTipProof {
    pub molecule_uuid: String,
    pub key_fingerprint: String,
}

/// One exact-key owner probe. It does not return the key, atom UUID, or body.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DroppedSchemaTipProbe {
    pub schema: String,
    pub molecule_uuid: String,
    pub key_fingerprint: String,
    pub candidate_fingerprints: Vec<String>,
    pub tip_present: bool,
    /// `None` when the tip is absent and no atom UUID can be read.
    pub atom_body_present: Option<bool>,
    /// `None` when the tip is absent or the atom body is absent.
    pub atom_source_schema_matches: Option<bool>,
    /// `None` when the tip is absent and no atom UUID can be read.
    /// Checks both anchored and legacy schema-index marker forms.
    pub schema_index_present: Option<bool>,
    pub protein_bound: bool,
    pub ref_edges_present_conservative: bool,
    pub molecule_edges_complete: bool,
}

/// Report for one bounded dropped-schema inspection pass.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DroppedSchemaReapReport {
    pub schema: String,
    pub dry_run: bool,
    pub refused_scan: bool,
    /// Candidate molecules checked in this pass, not a lifetime total.
    pub molecules: u64,
    pub molecules_allowed_by_drop_receipt: u64,
    pub molecules_retained_incomplete: u64,
    pub molecules_retained_active_edges: u64,
    pub molecules_retained_protein: u64,
    /// Tip keys inspected in this pass, not a lifetime total.
    pub tips: u64,
    pub tips_deleted: u64,
    /// Index keys inspected in this pass, not a lifetime total.
    pub index_keys: u64,
    pub index_keys_deleted: u64,
    pub physical_handles_visited: u64,
    pub cold_shard_loads: u64,
    /// False until the replayable erase journal also supports marker cleanup.
    pub index_only_cleanup: bool,
    /// False until the reaper execute route is safe.
    pub index_cleanup_available: bool,
    /// False when neither a receipt nor fields name any candidate molecules.
    pub tip_inspection_available: bool,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tip_proofs: Vec<DroppedSchemaTipProof>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DroppedSchemaReapCursor>,
}

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

fn index_prefixes(schema_name: &str) -> [String; 2] {
    let anchored = build_storage_key(None, &schema_index_codec::schema_prefix(schema_name));
    let legacy = form_twin(&anchored).unwrap_or_else(|| anchored.clone());
    [anchored, legacy]
}

impl AtomStore {
    /// Point-read one receipt-named `mk:` tip for a clone pilot proof.
    /// The caller must confirm that the schema is dropped and the receipt
    /// contains this molecule before it calls this method.
    pub async fn probe_dropped_schema_tip(
        &self,
        schema_name: &str,
        molecule_uuid: &str,
        key_hash: &str,
        key_range: &str,
        expected_key_fingerprint: Option<&str>,
    ) -> Result<DroppedSchemaTipProbe, SchemaError> {
        let keys = self
            .key_codec_for_molecule(molecule_uuid)
            .api_hash_range_record_keys_for_read(molecule_uuid, key_hash, key_range)
            .map_err(|error| SchemaError::InvalidField(format!("encode probe key: {error}")))?;
        if keys.is_empty() || keys.len() > 16 {
            return Err(SchemaError::InvalidData(
                "exact probe resolved no key or more than 16 key forms".to_string(),
            ));
        }
        let fingerprints: Vec<String> = keys
            .iter()
            .map(|key| key_fingerprint(schema_name, key))
            .collect();
        if let Some(expected) = expected_key_fingerprint {
            if !fingerprints
                .iter()
                .any(|fingerprint| fingerprint == expected)
            {
                return Err(SchemaError::InvalidField(
                    "expected key fingerprint does not match this exact probe key".to_string(),
                ));
            }
        }
        let mut found = None;
        for (index, key) in keys.iter().enumerate() {
            let record = self
                .main_store
                .get_item::<super::PerKeyRecord>(key)
                .await
                .map_err(|error| {
                    SchemaError::InvalidData(format!("point-read dropped tip: {error}"))
                })?;
            if let Some(record) = record {
                if found.is_some() {
                    return Err(SchemaError::InvalidData(
                        "multiple storage forms hold this exact tip".to_string(),
                    ));
                }
                found = Some((index, record));
            }
        }
        if let (Some(expected), Some((index, _))) = (expected_key_fingerprint, &found) {
            if fingerprints[*index] != expected {
                return Err(SchemaError::InvalidField(
                    "a different storage form holds this exact tip".to_string(),
                ));
            }
        }
        let selected_index = found
            .as_ref()
            .map(|(index, _)| *index)
            .or_else(|| {
                expected_key_fingerprint.and_then(|expected| {
                    fingerprints
                        .iter()
                        .position(|fingerprint| fingerprint == expected)
                })
            })
            .unwrap_or(0);
        let mut result = DroppedSchemaTipProbe {
            schema: schema_name.to_string(),
            molecule_uuid: molecule_uuid.to_string(),
            key_fingerprint: fingerprints[selected_index].clone(),
            candidate_fingerprints: fingerprints,
            tip_present: found.is_some(),
            atom_body_present: None,
            atom_source_schema_matches: None,
            schema_index_present: None,
            protein_bound: self.protein_of_molecule(molecule_uuid).await?.is_some(),
            ref_edges_present_conservative: self
                .has_any_molecule_ref_edges(molecule_uuid, None)
                .await?,
            molecule_edges_complete: self.molecule_ref_edges_complete(None).await?,
        };
        if let Some((_, record)) = found {
            let atom_uuid = &record.entry.atom_uuid;
            let atom = self.get_atom_by_uuid(atom_uuid, None).await?;
            result.atom_body_present = Some(atom.is_some());
            result.atom_source_schema_matches = atom
                .as_ref()
                .map(|atom| atom.source_schema_name() == schema_name);
            let index_key = build_storage_key(
                None,
                &schema_index_codec::record_key(schema_name, atom_uuid),
            );
            let mut index_forms = vec![index_key.clone()];
            if let Some(twin) = form_twin(&index_key) {
                if twin != index_key {
                    index_forms.push(twin);
                }
            }
            let mut index_present = false;
            for form in index_forms {
                index_present |= self
                    .schema_index_store
                    .inner()
                    .exists(form.as_bytes())
                    .await
                    .map_err(|error| {
                        SchemaError::InvalidData(format!(
                            "point-read dropped schema index: {error}"
                        ))
                    })?;
            }
            result.schema_index_present = Some(index_present);
        }
        Ok(result)
    }

    /// Compatibility entry point for a first page. Use the returned cursor
    /// with `reap_dropped_schema_tips_bounded` to finish a large inspection.
    pub async fn reap_dropped_schema_tips(
        &self,
        schema_name: &str,
        field_molecule_uuids: &[String],
        field_names: &[String],
        drop_receipt_proves_removed: bool,
        dry_run: bool,
        max_ops: usize,
    ) -> Result<DroppedSchemaReapReport, SchemaError> {
        self.reap_dropped_schema_tips_bounded(
            schema_name,
            field_molecule_uuids,
            field_names,
            drop_receipt_proves_removed,
            dry_run,
            max_ops,
            None,
        )
        .await
    }

    /// Inspect at most `max_ops` candidate molecules, index keys, and tip keys.
    /// `max_ops` must be 2..=4096 for a tip proof; one unit checks liveness.
    /// Execute remains closed until tip erasure and the keep-small debit share
    /// a durable, replayable operation ID.
    #[allow(clippy::too_many_arguments)] // The explicit cursor extends the compatibility entry point.
    pub async fn reap_dropped_schema_tips_bounded(
        &self,
        schema_name: &str,
        field_molecule_uuids: &[String],
        field_names: &[String],
        drop_receipt_proves_removed: bool,
        dry_run: bool,
        max_ops: usize,
        cursor: Option<DroppedSchemaReapCursor>,
    ) -> Result<DroppedSchemaReapReport, SchemaError> {
        if max_ops == 0 || max_ops > MAX_DROPPED_SCHEMA_REAP_OPS {
            return Err(SchemaError::InvalidField(format!(
                "max_ops must be 1..={MAX_DROPPED_SCHEMA_REAP_OPS}"
            )));
        }
        if !dry_run {
            return Err(SchemaError::Blocked(
                "dropped-schema reap execute needs a replayable exact-once tip delete and keep-small debit journal".to_string(),
            ));
        }
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
            known_molecules.extend(crate::atom::molecule_uuid_read_candidates(&write));
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
        let mut report = DroppedSchemaReapReport {
            schema: schema_name.to_string(),
            dry_run,
            tip_inspection_available: has_known_molecules,
            ..Default::default()
        };
        let state = cursor.map(|token| unseal_cursor(&token)).transpose()?;
        let mut state = state.unwrap_or_else(|| ReapCursorState {
            schema: schema_name.to_string(),
            candidate_fingerprint: candidate_fingerprint.clone(),
            phase: if has_known_molecules {
                DroppedSchemaReapPhase::Tips
            } else {
                DroppedSchemaReapPhase::Index
            },
            index_form: 0,
            index_position: None,
            molecule_after: None,
            tip_molecule: None,
            tip_position: None,
        });
        let cursor_bytes = serde_json::to_vec(&state)
            .map_err(|error| SchemaError::InvalidCursor(format!("serialize cursor: {error}")))?;
        if cursor_bytes.len() > MAX_REAPER_CURSOR_BYTES
            || state.schema != schema_name
            || state.candidate_fingerprint != candidate_fingerprint
            || state.index_form > 2
            || (!has_known_molecules && state.phase == DroppedSchemaReapPhase::Tips)
            || (state.phase == DroppedSchemaReapPhase::Tips
                && (state.index_form != 0 || state.index_position.is_some()))
            || (state.phase == DroppedSchemaReapPhase::Index
                && (state.tip_molecule.is_some()
                    || state.tip_position.is_some()
                    || state.molecule_after.is_some()))
            || (state.index_form == 2 && state.index_position.is_some())
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
                state.index_form >= 2
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
        let prefixes = index_prefixes(schema_name);
        let mut budget = max_ops;
        while budget > 0 {
            match state.phase {
                DroppedSchemaReapPhase::Tips => {
                    let mut complete = true;
                    for molecule in &known_molecules {
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
                        if budget == 0 {
                            complete = false;
                            break;
                        }
                        budget -= 1;
                        report.molecules += 1;
                        let edges_complete = self.molecule_ref_edges_complete(None).await?;
                        if !edges_complete && !drop_receipt_proves_removed {
                            report.molecules_retained_incomplete += 1;
                            state.molecule_after = Some(molecule.clone());
                            state.tip_molecule = None;
                            state.tip_position = None;
                            continue;
                        }
                        let has_active_refs = if edges_complete {
                            self.has_active_molecule_refs(molecule, None).await?
                        } else {
                            self.has_any_molecule_ref_edges(molecule, None).await?
                        };
                        if has_active_refs {
                            report.molecules_retained_active_edges += 1;
                            state.molecule_after = Some(molecule.clone());
                            state.tip_molecule = None;
                            state.tip_position = None;
                            continue;
                        }
                        if self.protein_of_molecule(molecule).await?.is_some() {
                            report.molecules_retained_protein += 1;
                            state.molecule_after = Some(molecule.clone());
                            state.tip_molecule = None;
                            state.tip_position = None;
                            continue;
                        }
                        if !edges_complete {
                            report.molecules_allowed_by_drop_receipt += 1;
                        }
                        if budget == 0 {
                            state.tip_molecule = Some(molecule.clone());
                            state.tip_position = None;
                            complete = false;
                            break;
                        }
                        let prefix = molecule_key_codec::molecule_record_prefix(molecule);
                        let page = key_page(
                            &self.main_store,
                            &prefix,
                            if state.tip_molecule.as_deref() == Some(molecule.as_str()) {
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
                                molecule_uuid: molecule.clone(),
                                key_fingerprint: key_fingerprint(schema_name, key),
                            });
                        }
                        if let Some(next) = page.next_cursor {
                            state.tip_molecule = Some(molecule.clone());
                            state.tip_position = Some(next);
                        } else {
                            state.molecule_after = Some(molecule.clone());
                            state.tip_molecule = None;
                            state.tip_position = None;
                        }
                        complete = false;
                        break;
                    }
                    if !complete || budget == 0 {
                        break;
                    }
                    state.phase = DroppedSchemaReapPhase::Index;
                    state.molecule_after = None;
                    state.tip_molecule = None;
                    state.tip_position = None;
                }
                DroppedSchemaReapPhase::Index => {
                    if state.index_form >= 2 {
                        return Ok(report);
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
                    if state.index_form >= 2 {
                        return Ok(report);
                    }
                    break;
                }
            }
        }
        report.truncated = true;
        report.next_cursor = Some(seal_cursor(&state)?);
        Ok(report)
    }
}
