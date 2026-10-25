//! Strict decoding of retained retirement proof bytes. No replay admission.

use super::catalog_keys::decode_catalog_json;
use super::{SchemaDropReceipt, SchemaStore};
use crate::schema::SchemaError;
use serde::{Deserialize, Deserializer, Serialize};

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

fn bounded(raw: &[u8]) -> Result<(), SchemaError> {
    if raw.len() > MAX_AUTHORITY_BYTES {
        return Err(refused("exact replay authority exceeds byte cap"));
    }
    Ok(())
}

fn refused(reason: &'static str) -> SchemaError {
    SchemaError::InvalidData(reason.into())
}
