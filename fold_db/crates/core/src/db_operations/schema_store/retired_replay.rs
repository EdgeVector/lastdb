//! Exact retirement authority for exclusive, one-shot restore replay.

use super::catalog_keys::{decode_catalog_json, name_claim_key, schema_drop_receipt_key};
use super::{SchemaDropReceipt, SchemaStore};
use crate::schema::{Schema, SchemaError};
use crate::storage::traits::KvStore;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_AUTHORITY_BYTES: usize = 8 * 1024 * 1024;

/// The journal retains the exact stored proof and its native interpretation.
#[derive(Clone, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct RetiredReplayProof {
    pub(crate) schema_name: String,
    pub(crate) receipt: SchemaDropReceipt,
    pub(crate) receipt_raw: Vec<u8>,
}

/// Do not silently discard unsupported fields from a retirement decision.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictDropReceipt {
    identity: String,
    #[serde(deserialize_with = "required_owner")]
    owner_app: Option<String>,
    field_molecule_uuids: Vec<String>,
    dropped_at_unix_ms: u64,
}

fn required_owner<'de, D: Deserializer<'de>>(decoder: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(decoder)
}

struct StatePositions {
    residual: Vec<usize>,
    receipt: usize,
}

struct AuthorityPlan {
    names: Vec<String>,
    state_keys: Vec<Vec<u8>>,
    states: Vec<StatePositions>,
}

impl AuthorityPlan {
    fn new(schema_names: &[String]) -> Result<Self, SchemaError> {
        let names: Vec<_> = schema_names
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if names
            .iter()
            .any(|name| name.is_empty() || name.contains('\0') || name.trim() != name)
        {
            return Err(refused("invalid exact replay schema identity"));
        }
        let mut state_keys = Vec::new();
        let mut positions = BTreeMap::new();
        let mut states = Vec::with_capacity(names.len());
        for name in &names {
            let mut residual = Vec::new();
            for key in [name.clone(), name_claim_key(name)] {
                residual.push(key_position(
                    key.into_bytes(),
                    &mut state_keys,
                    &mut positions,
                ));
            }
            let receipt = key_position(
                schema_drop_receipt_key(name).into_bytes(),
                &mut state_keys,
                &mut positions,
            );
            states.push(StatePositions { residual, receipt });
        }
        Ok(Self {
            names,
            state_keys,
            states,
        })
    }
}

fn key_position(
    key: Vec<u8>,
    keys: &mut Vec<Vec<u8>>,
    positions: &mut BTreeMap<Vec<u8>, usize>,
) -> usize {
    if let Some(position) = positions.get(&key) {
        return *position;
    }
    let position = keys.len();
    keys.push(key.clone());
    positions.insert(key, position);
    position
}

impl SchemaStore {
    /// Verify historical proof bytes without granting current retirement authority.
    pub(crate) fn validate_retired_replay_proof(
        &self,
        proof: &RetiredReplayProof,
    ) -> Result<(), SchemaError> {
        if proof.schema_name.is_empty()
            || proof.schema_name.contains('\0')
            || proof.schema_name.trim() != proof.schema_name
        {
            return Err(refused("invalid retained replay schema identity"));
        }
        if self.decode_retired_receipt(&proof.receipt_raw, &proof.schema_name)? != proof.receipt {
            return Err(refused(
                "retired replay proof bytes differ from native receipt",
            ));
        }
        Ok(())
    }

    /// Return proofs only for fully absent authority with an exact durable receipt.
    /// A valid active body has no entry. Every unknown retirement state refuses.
    /// The exclusive restore caller supplies the no-concurrent-writer boundary.
    pub(crate) async fn retired_replay_proofs(
        &self,
        schema_names: &[String],
    ) -> Result<BTreeMap<String, RetiredReplayProof>, SchemaError> {
        let plan = AuthorityPlan::new(schema_names)?;
        if plan.names.is_empty() {
            return Ok(BTreeMap::new());
        }
        let keys: Vec<_> = plan
            .names
            .iter()
            .map(|name| name.as_bytes().to_vec())
            .collect();
        let (bodies, successors, states) = tokio::try_join!(
            strict_raw_values(self.schemas_store.inner().as_ref(), keys.clone()),
            strict_raw_values(self.superseded_by_store.inner().as_ref(), keys),
            strict_raw_values(
                self.schema_states_store.inner().as_ref(),
                plan.state_keys.clone()
            ),
        )?;
        if bodies.len() != plan.names.len()
            || successors.len() != plan.names.len()
            || states.len() != plan.state_keys.len()
        {
            return Err(refused("incomplete exact replay authority batch"));
        }
        let mut proofs = BTreeMap::new();
        for (index, name) in plan.names.iter().enumerate() {
            let positions = &plan.states[index];
            if let Some(raw) = bodies[index].as_deref() {
                self.validate_active_replay_body(raw, name)?;
                continue;
            }
            if successors[index].is_some()
                || positions
                    .residual
                    .iter()
                    .any(|position| states[*position].is_some())
            {
                return Err(refused("residual authority for absent replay schema"));
            }
            let receipt_raw = states[positions.receipt].as_deref();
            let receipt = receipt_raw
                .map(|raw| self.decode_retired_receipt(raw, name))
                .transpose()?;
            let receipt =
                receipt.ok_or_else(|| refused("absent replay schema has no retirement proof"))?;
            let receipt_raw = receipt_raw
                .ok_or_else(|| refused("decoded retirement receipt has no stored bytes"))?;
            proofs.insert(
                name.clone(),
                RetiredReplayProof {
                    schema_name: name.clone(),
                    receipt,
                    receipt_raw: receipt_raw.to_vec(),
                },
            );
        }
        Ok(proofs)
    }

    fn validate_active_replay_body(&self, raw: &[u8], name: &str) -> Result<(), SchemaError> {
        bounded(raw)?;
        let mut schema: Schema = decode_catalog_json(raw, self.catalog_unwrap_key.as_ref())
            .map_err(|_| refused("unreadable exact replay schema body"))?;
        if schema.name != name {
            return Err(refused("exact replay schema body identity mismatch"));
        }
        schema
            .populate_runtime_fields()
            .map_err(|_| refused("invalid exact replay schema fields"))
    }

    fn decode_retired_receipt(
        &self,
        raw: &[u8],
        name: &str,
    ) -> Result<SchemaDropReceipt, SchemaError> {
        bounded(raw)?;
        let receipt: StrictDropReceipt = decode_catalog_json(raw, self.catalog_unwrap_key.as_ref())
            .map_err(|_| refused("unreadable exact replay retirement receipt"))?;
        if receipt.identity != name {
            return Err(refused("exact replay retirement receipt identity mismatch"));
        }
        Ok(SchemaDropReceipt {
            identity: receipt.identity,
            owner_app: receipt.owner_app,
            field_molecule_uuids: receipt.field_molecule_uuids,
            dropped_at_unix_ms: receipt.dropped_at_unix_ms,
        })
    }
}

async fn strict_raw_values(
    store: &dyn KvStore,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
    let (presence, values) = tokio::try_join!(
        store.exists_many(keys.clone()),
        store.get_many(keys.clone()),
    )?;
    if presence.len() != keys.len()
        || values.len() != keys.len()
        || presence
            .iter()
            .zip(&values)
            .any(|(exists, value)| *exists != value.is_some())
    {
        return Err(refused("raw replay authority presence or decode differs"));
    }
    Ok(values)
}

fn bounded(raw: &[u8]) -> Result<(), SchemaError> {
    if raw.len() > MAX_AUTHORITY_BYTES {
        return Err(refused("exact replay authority exceeds byte cap"));
    }
    Ok(())
}

fn refused(reason: &'static str) -> SchemaError {
    SchemaError::InvalidData(reason.into())
}
