//! Bounded inspection of tips for a dropped schema identity.
//!
//! A drop receipt or explicit field names supply the molecule set. Without
//! either, the inspector can page `schemaidx:` markers but cannot assert which
//! molecule tips belong to them. It never scans `atom:`.

use super::AtomStore;
use crate::schema::SchemaError;
use crate::storage::traits::PhysicalScanCursor;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use walk::{index_prefixes, reap_candidates, validate_cursor_state};

mod probe;
mod walk;

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
    /// Checks the exact current schema-index marker.
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

impl AtomStore {
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
        let (known_molecules, candidate_fingerprint) = reap_candidates(
            schema_name,
            field_molecule_uuids,
            field_names,
            drop_receipt_proves_removed,
            max_ops,
        )?;
        let has_known_molecules = !known_molecules.is_empty();
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
        validate_cursor_state(
            &state,
            schema_name,
            &candidate_fingerprint,
            &known_molecules,
        )?;
        let prefixes = index_prefixes(schema_name);
        let mut budget = max_ops;
        while budget > 0 {
            match state.phase {
                DroppedSchemaReapPhase::Tips => {
                    let moved_to_index = self
                        .reap_tips_phase(
                            schema_name,
                            &known_molecules,
                            drop_receipt_proves_removed,
                            &mut state,
                            &mut budget,
                            &mut report,
                        )
                        .await?;
                    if !moved_to_index {
                        break;
                    }
                }
                DroppedSchemaReapPhase::Index => {
                    if self
                        .reap_index_phase(&prefixes, &mut state, budget, &mut report)
                        .await?
                    {
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
