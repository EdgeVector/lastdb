//! Exact-key diagnostics; never construct schema caches or heal authority.

use super::catalog_keys::{decode_catalog_json, name_claim_key, schema_drop_receipt_key};
use super::{SchemaDropReceipt, SchemaStore};
use crate::crypto::at_rest::{is_sealed_at_rest, open_at_rest};
use crate::schema::{SchemaNameClaim, SchemaState};
use crate::storage::traits::{KvStore, NamespacedStore};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

const MAX_AUTHORITY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum DecodeStatus {
    Absent,
    ReadError,
    EnvelopeOpenError,
    TooLarge,
    JsonDecodeError,
    Decoded,
}

/// Fixed evidence for one exact key. An absent value differs from a failed read.
#[derive(Serialize)]
pub struct AuthorityKeyProbe {
    raw_present: Option<bool>,
    envelope_present: Option<bool>,
    status: DecodeStatus,
}

/// No names, keys, values, identity digests, or decoder messages leave this report.
#[derive(Serialize)]
pub struct ReplayAuthorityProbe {
    pub body: AuthorityKeyProbe,
    pub body_name_matches: Option<bool>,
    pub body_runtime_fields_valid: Option<bool>,
    pub state_forms: Vec<AuthorityKeyProbe>,
    pub states: Vec<Option<SchemaState>>,
    pub supersession: AuthorityKeyProbe,
    pub supersession_nonempty: Option<bool>,
    pub name_claim: AuthorityKeyProbe,
    pub name_claim_retired: Option<bool>,
    pub drop_receipt: AuthorityKeyProbe,
    pub drop_receipt_identity_matches: Option<bool>,
}

impl AuthorityKeyProbe {
    fn failed() -> Self {
        Self {
            raw_present: None,
            envelope_present: None,
            status: DecodeStatus::ReadError,
        }
    }
}

fn decode<T: DeserializeOwned>(
    raw: Option<Vec<u8>>,
    key: &[u8; 32],
) -> (AuthorityKeyProbe, Option<T>) {
    let Some(raw) = raw else {
        return (
            AuthorityKeyProbe {
                raw_present: Some(false),
                envelope_present: None,
                status: DecodeStatus::Absent,
            },
            None,
        );
    };
    let sealed = is_sealed_at_rest(&raw);
    let mut fact = AuthorityKeyProbe {
        raw_present: Some(true),
        envelope_present: Some(sealed),
        status: DecodeStatus::TooLarge,
    };
    if raw.len() > MAX_AUTHORITY_BYTES {
        return (fact, None);
    }
    let plain = if sealed {
        if let Ok(plain) = open_at_rest(key, &raw) {
            plain
        } else {
            fact.status = DecodeStatus::EnvelopeOpenError;
            return (fact, None);
        }
    } else {
        raw
    };
    if plain.len() > MAX_AUTHORITY_BYTES {
        return (fact, None);
    }
    if let Ok(value) = decode_catalog_json(&plain, None) {
        fact.status = DecodeStatus::Decoded;
        (fact, Some(value))
    } else {
        fact.status = DecodeStatus::JsonDecodeError;
        (fact, None)
    }
}

async fn read_keys(store: &dyn KvStore, keys: Vec<Vec<u8>>) -> Option<Vec<Option<Vec<u8>>>> {
    let expected = keys.len();
    store
        .get_many(keys)
        .await
        .ok()
        .filter(|values| values.len() == expected)
}

impl SchemaStore {
    /// Read only the requested body's exact authority keys with native decoders.
    /// Namespace reads are batched and independent; no normal healing getter runs.
    pub async fn probe_replay_authority(
        store: Arc<dyn NamespacedStore>,
        name: &str,
        key: &[u8; 32],
    ) -> Result<ReplayAuthorityProbe, &'static str> {
        let (body_store, state_store, supersession_store) = tokio::try_join!(
            store.open_namespace("schemas"),
            store.open_namespace("schema_states"),
            store.open_namespace("schema_superseded_by"),
        )
        .map_err(|_| "NAMESPACE_OPEN_FAILED")?;
        let forms = crate::kind_partition::read_forms(name);
        let mut state_keys: Vec<_> = forms.iter().map(|form| form.as_bytes().to_vec()).collect();
        state_keys.push(name_claim_key(name).into_bytes());
        state_keys.push(schema_drop_receipt_key(name).into_bytes());
        let (body_raw, state_raw, successor_raw) = tokio::join!(
            read_keys(body_store.as_ref(), vec![name.as_bytes().to_vec()]),
            read_keys(state_store.as_ref(), state_keys),
            read_keys(supersession_store.as_ref(), vec![name.as_bytes().to_vec()]),
        );
        let (body, schema) = match body_raw {
            Some(mut values) => decode::<crate::schema::Schema>(values.remove(0), key),
            None => (AuthorityKeyProbe::failed(), None),
        };
        // Same schema type and runtime validation as get_all_schemas_strict, without heal.
        let (body_name_matches, body_runtime_fields_valid) = match schema {
            Some(mut schema) => (
                Some(schema.name == name),
                Some(schema.populate_runtime_fields().is_ok()),
            ),
            None => (None, None),
        };
        let mut state_forms = Vec::with_capacity(forms.len());
        let mut states = Vec::with_capacity(forms.len());
        let (name_claim, claim, drop_receipt, receipt) = if let Some(mut values) = state_raw {
            let drop_raw = values.pop().expect("one drop receipt key was requested");
            let claim_raw = values.pop().expect("one claim key was requested");
            for raw in values {
                let (fact, state) = decode::<SchemaState>(raw, key);
                state_forms.push(fact);
                states.push(state);
            }
            let (claim_fact, claim) = decode::<SchemaNameClaim>(claim_raw, key);
            let (drop_fact, receipt) = decode::<SchemaDropReceipt>(drop_raw, key);
            (claim_fact, claim, drop_fact, receipt)
        } else {
            for _ in &forms {
                state_forms.push(AuthorityKeyProbe::failed());
                states.push(None);
            }
            (
                AuthorityKeyProbe::failed(),
                None,
                AuthorityKeyProbe::failed(),
                None,
            )
        };
        let (supersession, successor) = match successor_raw {
            Some(mut values) => decode::<String>(values.remove(0), key),
            None => (AuthorityKeyProbe::failed(), None),
        };
        Ok(ReplayAuthorityProbe {
            body,
            body_name_matches,
            body_runtime_fields_valid,
            state_forms,
            states,
            supersession,
            supersession_nonempty: successor.map(|value| !value.is_empty()),
            name_claim,
            name_claim_retired: claim.map(|value| value.retired),
            drop_receipt,
            drop_receipt_identity_matches: receipt.map(|value| value.identity == name),
        })
    }
}
